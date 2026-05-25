%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Generic replication driver for a gossiped OR-Map.
%%%
%%% This is the low-level public substrate behind `mycelium_map' and the
%%% built-in registry / leader / sharded-placement / durable-reminder
%%% features. Use `mycelium_map' for an ordinary replicated key-value map;
%%% implement this behaviour directly only when you need custom merge or
%%% snapshot semantics (e.g. leader election layers fencing tokens on top
%%% via `broadcast_custom/2'). Stability: beta.
%%%
%%% An instance is started with `start_link(#{name := atom(), callback :=
%%% module()})'. `name' is BOTH the registered process name and the
%%% Plumtree tag that scopes this instance's broadcasts, so it must be a
%%% unique atom. Instances share the Plumtree bus; each ignores payloads
%%% carrying another instance's tag. The driver handles:
%%%
%%%   - broadcast add/remove deltas as OR-Map entries (`broadcast_update/2'),
%%%   - route incoming deltas to the owner's merge callback,
%%%   - seed peers from the active view and full-sync on start / `peer_up',
%%%   - drop a node's entries on `peer_down'.
%%%
%%% The OWNER process holds the actual OR-Map (so it can run its side
%%% effects synchronously) and implements the callbacks below. The driver
%%% calls them from its own process, passing the instance `name' first so
%%% one callback module can back many instances. Start the owner BEFORE
%%% its replica instance (the callbacks cast into the owner).
%%%
%%% Wire safety: callbacks receive entries straight off gossip. Passing
%%% them to `mycelium_ormap:absorb_clock/merge' unvalidated can crash the
%%% merge or the shared `mycelium_hlc' server (malformed dot/HLC, empty dot
%%% map, non-map payload). An implementer that merges deltas from sources
%%% it does not fully control SHOULD validate via `mycelium_crdt_wire'
%%% (the recommended helper; not enforced). Leaf-payload validation is the
%%% app's own concern. (Of the built-ins, only the reminder validates;
%%% registry/leader/shard have internal writers.)
%%%
%%% Transport: gossip rides `mycelium_plumtree' + `mycelium_hyparview_events'
%%% over mycelium's dist carrier, so a consumer must run on mycelium's
%%% distribution. A pluggable transport (for apps with their own membership)
%%% is future work.
-module(mycelium_replica).
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([broadcast_update/2, broadcast_custom/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SYNC_TAG, '$mycelium_replica').

%% Every callback receives the instance `Name' as its first argument, so
%% one callback module can back several independently-named instances
%% (e.g. many `mycelium_map' instances share the `mycelium_map' module).

%% Merge an incoming delta (one or more {Key, entry}) into the owner's
%% map and run its side effects.
-callback replica_merge_delta(Name :: atom(), Delta :: mycelium_ormap:ormap()) -> ok.

%% Apply a full snapshot received from a peer on connect.
-callback replica_apply_full_sync(Name :: atom(), Snapshot :: term()) -> ok.

%% Produce a snapshot to send to a newly connected peer, or `empty'
%% when there is nothing to send.
-callback replica_full_sync_snapshot(Name :: atom()) ->
    {sync, Snapshot :: term()} | empty.

%% Drop all entries owned by a node that left or failed.
-callback replica_remove_node(Name :: atom(), node()) -> ok.

%% Merge a feature-specific custom broadcast (optional).
-callback replica_merge_custom(Name :: atom(), Payload :: term()) -> ok.

%% Whether this callback's instances run periodic anti-entropy (optional;
%% absent or `false' = off). A module returns `true' here to make periodic
%% full-sync convergence intrinsic to its instances, with no per-instance or
%% operator opt-out; the only knob is the `replica_anti_entropy_ms' interval.
%% Implement it only for value-carrying stores with a full-state snapshot and
%% tombstone removal (the built-in reminder and `mycelium_map' do); the
%% registry/leader/shard do not, so they stay off structurally.
-callback replica_anti_entropy() -> boolean().

-optional_callbacks([replica_merge_custom/2, replica_anti_entropy/0]).

-record(state, {
    name :: atom(),
    cb :: module(),
    peers = [] :: [node()],
    watch = #{} :: mycelium_source_monitor:watch(),
    %% Periodic anti-entropy interval (ms); 0 = disabled (timer never armed).
    ae_ms = 0 :: non_neg_integer()
}).

%% The instance `name' is both the registered process name and the
%% Plumtree tag that scopes this instance's broadcasts. Periodic
%% anti-entropy (a full-sync PULL from a random peer, so the instance
%% reconverges after a heal even without a fresh `peer_up') is governed by
%% the callback's `replica_anti_entropy/0', not by config: a value-carrying
%% store declares it in code and there is no per-instance toggle.
-type config() :: #{name := atom(), callback := module()}.
-export_type([config/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link(config()) -> {ok, pid()} | {error, term()}.
start_link(#{name := Name} = Config) ->
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% Broadcast an OR-Map add/remove on this instance.
-spec broadcast_update(atom(), {add, term(), term()} | {remove, term()}) -> ok.
broadcast_update(Name, Update) ->
    gen_server:cast(Name, {broadcast, Update}).

%% Broadcast a feature-specific payload on this instance's tag,
%% delivered to the owner's `replica_merge_custom/1'.
-spec broadcast_custom(atom(), term()) -> ok.
broadcast_custom(Name, Payload) ->
    gen_server:cast(Name, {broadcast_custom, Payload}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#{name := Name, callback := Cb}) ->
    %% Subscribe to both sources and keep the subscriptions alive across a
    %% source restart (a plumtree/hyparview-events crash does not restart
    %% us, so a one-shot subscribe would be silently dropped).
    Watch = mycelium_source_monitor:start(
        [mycelium_plumtree, mycelium_hyparview_events]
    ),
    %% Seed from the current active view and pull existing state. peer_up
    %% only fires for FUTURE joins, so an instance started after the cluster
    %% has already formed (e.g. a mycelium_map created at runtime) would
    %% otherwise sit at peers=[] and never sync. Deferred via a self-message
    %% so init/1 stays non-blocking.
    self() ! seed_initial_sync,
    AeMs =
        case anti_entropy_enabled(Cb) of
            true -> application:get_env(mycelium, replica_anti_entropy_ms, 30000);
            false -> 0
        end,
    arm_anti_entropy(AeMs),
    {ok, #state{name = Name, cb = Cb, watch = Watch, ae_ms = AeMs}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({broadcast, {add, Key, Val}}, #state{name = Name} = State) ->
    Dot = {node(), mycelium_hlc:now()},
    Delta = #{Key => {value, Val, #{Dot => true}}},
    mycelium_plumtree:broadcast(Name, {delta, node(), Delta}),
    {noreply, State};
handle_cast({broadcast, {remove, Key}}, #state{name = Name} = State) ->
    %% Tombstone-as-delta: the receiver's OR-Map merge resolves against
    %% any in-flight value by HLC, so a delayed add cannot resurrect it.
    Delta = #{Key => {tombstone, mycelium_hlc:now()}},
    mycelium_plumtree:broadcast(Name, {delta, node(), Delta}),
    {noreply, State};
handle_cast({broadcast_custom, Payload}, #state{name = Name} = State) ->
    mycelium_plumtree:broadcast(Name, {custom, node(), Payload}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Plumtree delivery scoped to this instance (tag =:= our name).
handle_info({plumtree_broadcast, {MsgTag, Payload}}, #state{name = Name} = State) when
    MsgTag =:= Name
->
    handle_payload(Payload, Name, State);
%% Plumtree delivery for another instance.
handle_info({plumtree_broadcast, _Other}, State) ->
    {noreply, State};
handle_info({mycelium_event, {peer_up, Node}}, #state{peers = Peers} = State) ->
    case Node =:= node() orelse lists:member(Node, Peers) of
        true ->
            {noreply, State};
        false ->
            self() ! {do_full_sync, Node},
            {noreply, State#state{peers = [Node | Peers]}}
    end;
handle_info(
    {mycelium_event, {peer_down, Node, _Reason}},
    #state{name = Name, cb = Cb, peers = Peers} = State
) ->
    Cb:replica_remove_node(Name, Node),
    {noreply, State#state{peers = lists:delete(Node, Peers)}};
handle_info({mycelium_event, _Other}, State) ->
    {noreply, State};
%% Seed peers from the current active view and pull their state (see init/1).
handle_info(seed_initial_sync, #state{peers = Peers} = State) ->
    Seeded = lists:usort(Peers ++ active_view_peers()),
    {noreply, request_sync_from_peers(State#state{peers = Seeded})};
handle_info({do_full_sync, Node}, #state{name = Name, cb = Cb, peers = Peers} = State) ->
    case lists:member(Node, Peers) of
        true ->
            case Cb:replica_full_sync_snapshot(Name) of
                empty -> ok;
                {sync, Snap} -> send_to_peer(Name, Node, {full_sync, node(), Snap})
            end;
        false ->
            ok
    end,
    {noreply, State};
handle_info(
    {?SYNC_TAG, {full_sync, _FromNode, Snapshot}},
    #state{name = Name, cb = Cb} = State
) ->
    Cb:replica_apply_full_sync(Name, Snapshot),
    {noreply, State};
%% A peer that just re-subscribed asks us to push our state to it.
handle_info(
    {?SYNC_TAG, {request_sync, FromNode}},
    #state{name = Name, cb = Cb} = State
) ->
    case Cb:replica_full_sync_snapshot(Name) of
        empty -> ok;
        {sync, Snap} -> send_to_peer(Name, FromNode, {full_sync, node(), Snap})
    end,
    {noreply, State};
%% A watched source restarted: re-subscribe (with retry) and, once back,
%% pull a full sync from known peers to recover deltas missed during the
%% gap. Full sync rides direct dist messages, not plumtree, so it works
%% even while plumtree is bouncing.
handle_info({mycelium_source_monitor, retry, Source}, #state{watch = Watch} = State) ->
    Was = maps:is_key(Source, Watch),
    Watch1 = mycelium_source_monitor:retry(Source, Watch),
    State1 = State#state{watch = Watch1},
    case (not Was) andalso maps:is_key(Source, Watch1) of
        true -> {noreply, request_sync_from_peers(State1)};
        false -> {noreply, State1}
    end;
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{watch = Watch} = State) ->
    case mycelium_source_monitor:down(Ref, Watch) of
        {down, _Source, Watch1} -> {noreply, State#state{watch = Watch1}};
        ignore -> {noreply, State}
    end;
%% Periodic anti-entropy: pull a full sync from one random peer so a node
%% that missed updates (e.g. a surviving link after a partition heal, which
%% gets no fresh peer_up) reconverges. The merge is idempotent, so repeated
%% pulls are safe; over a few ticks state propagates transitively.
handle_info(anti_entropy, #state{name = Name, peers = Peers, ae_ms = AeMs} = State) ->
    case Peers of
        [] -> ok;
        _ -> send_to_peer(Name, random_peer(Peers), {request_sync, node()})
    end,
    arm_anti_entropy(AeMs),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

handle_payload({delta, _FromNode, Delta}, Name, #state{cb = Cb} = State) ->
    Cb:replica_merge_delta(Name, Delta),
    {noreply, State};
handle_payload({custom, _FromNode, Payload}, Name, #state{cb = Cb} = State) ->
    case erlang:function_exported(Cb, replica_merge_custom, 2) of
        true -> Cb:replica_merge_custom(Name, Payload);
        false -> ok
    end,
    {noreply, State};
handle_payload(_Other, _Name, State) ->
    {noreply, State}.

send_to_peer(Name, Node, Msg) ->
    erlang:send({Name, Node}, {?SYNC_TAG, Msg}, [noconnect]).

%% Current HyParView active view (other nodes), used to seed a freshly
%% started instance. Safe during early boot: returns [] if HyParView is
%% not answering yet.
active_view_peers() ->
    try mycelium:active_view() of
        Nodes -> [N || N <- Nodes, N =/= node()]
    catch
        _:_ ->
            []
    end.

%% Ask every known peer to push its full state to us. Used after a
%% re-subscribe to recover anything missed while a source was down.
request_sync_from_peers(#state{name = Name, peers = Peers} = State) ->
    [send_to_peer(Name, Node, {request_sync, node()}) || Node <- Peers],
    State.

%% A callback module opts its instances into periodic anti-entropy by
%% exporting `replica_anti_entropy/0' returning `true'. Absent (the
%% registry/leader/shard) it stays off.
anti_entropy_enabled(Cb) ->
    erlang:function_exported(Cb, replica_anti_entropy, 0) andalso Cb:replica_anti_entropy().

random_peer(Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers).

%% Arm the next anti-entropy tick. 0 = disabled (never armed). Integer-only
%% jitter (+-25%) so instances/nodes do not sync in lockstep; robust at small
%% intervals (never rand:uniform(0), never a float for send_after).
arm_anti_entropy(0) ->
    ok;
arm_anti_entropy(AeMs) when is_integer(AeMs), AeMs > 0 ->
    _ = erlang:send_after(jitter(AeMs), self(), anti_entropy),
    ok.

jitter(AeMs) ->
    case AeMs div 4 of
        0 -> AeMs;
        J -> AeMs - J + rand:uniform(2 * J)
    end.
