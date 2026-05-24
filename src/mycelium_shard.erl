%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Sharded service placement: given a key, agree cluster-wide on the
%%% node that should own it, and emit a churn-aware ownership event so
%%% owners can hand off / take over. This is the substrate for
%%% partitioning state across nodes.
%%%
%%% Membership is a replicated, LEASE-based live-node set (not the
%%% bounded HyParView active view, and not driven by `peer_down', which
%%% is active-view churn rather than cluster death). Each node gossips a
%%% periodic heartbeat carrying its wall-clock time; a node is "in the
%%% ring" while its lease is fresh (`Now - EmitWallMs =< member_ttl_ms').
%%% Heartbeats too far in the future are rejected so a fast clock cannot
%%% pin a dead node. This converges WITHOUT tombstones: a stale entry
%%% carried in a full-sync is already expired by its timestamp.
%%%
%%% Placement is rendezvous (HRW) hashing over the live set, bucketed
%%% into `ring_size' partitions (so ownership events are finite and a
%%% departing node moves only its own partitions). Ownership is
%%% computed deterministically as `max {phash2({Node, P}), Node}'.
%%%
%%% The live member list is published to a read-concurrency ETS table so
%%% `place/1' and friends are lock-free pure reads off the hot path.
%%% The replicated set rides a `mycelium_replica' instance named
%%% `mycelium_members_replica' (callback = this module).
-module(mycelium_shard).
-behaviour(gen_server).
-behaviour(mycelium_replica).

%% Public API (pure reads + subscription)
-export([
    place/1,
    owners/2,
    is_owner/1,
    partition/1,
    members/0,
    subscribe/1,
    unsubscribe/1
]).

%% Internal API
-export([start_link/0]).

%% mycelium_replica callbacks
-export([
    replica_merge_delta/2,
    replica_apply_full_sync/2,
    replica_full_sync_snapshot/1,
    replica_remove_node/2
]).

%% Internal (invoked by the replica callbacks; owner/2 also used by tests)
-export([merge_delta/1, apply_full_sync/1, snapshot/0, owner/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TAB, ?MODULE).
-define(REPLICA, mycelium_members_replica).
-define(DEFAULT_RING_SIZE, 64).
-define(DEFAULT_HEARTBEAT_MS, 2000).
-define(DEFAULT_TTL_MS, 6000).
-define(DEFAULT_SKEW_MS, 5000).

-record(state, {
    ring_size :: pos_integer(),
    heartbeat_ms :: pos_integer(),
    ttl_ms :: pos_integer(),
    skew_ms :: non_neg_integer(),
    leases = #{} :: #{node() => integer()},
    members = [] :: [node()],
    owned = #{} :: #{non_neg_integer() => true},
    subscribers = #{} :: #{pid() => reference()},
    watch = #{} :: mycelium_source_monitor:watch()
}).

%%====================================================================
%% Public API (pure, read from ETS, off the gen_server hot path)
%%====================================================================

%% @doc The node that should own `Key' cluster-wide.
-spec place(term()) -> node() | undefined.
place(Key) ->
    owner(partition(Key), members()).

%% @doc The top-`N' distinct owner nodes for `Key' (for replicated
%% placement), best owner first.
-spec owners(term(), pos_integer()) -> [node()].
owners(Key, N) ->
    P = partition(Key),
    Desc = lists:reverse(
        lists:sort(
            [{erlang:phash2({Nd, P}), Nd} || Nd <- members()]
        )
    ),
    [Nd || {_Score, Nd} <- lists:sublist(Desc, N)].

%% @doc Whether this node currently owns `Key'.
-spec is_owner(term()) -> boolean().
is_owner(Key) ->
    place(Key) =:= node().

%% @doc The ring partition `Key' falls in. Consumers (e.g. reminders)
%% use this to map keys to partitions without duplicating ring/hash
%% logic.
-spec partition(term()) -> non_neg_integer().
partition(Key) ->
    erlang:phash2(Key, ring_size()).

%% @doc The current live member set (sorted).
-spec members() -> [node()].
members() ->
    case ets:info(?TAB, name) of
        undefined ->
            [];
        _ ->
            case ets:lookup(?TAB, members) of
                [{members, M}] -> M;
                [] -> []
            end
    end.

-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
    gen_server:call(?SERVER, {subscribe, Pid}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}).

%%====================================================================
%% Internal API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%====================================================================
%% mycelium_replica callbacks (run in the replica process)
%%====================================================================

replica_merge_delta(_Name, Delta) ->
    merge_delta(Delta).

replica_apply_full_sync(_Name, Snapshot) ->
    apply_full_sync(Snapshot).

replica_full_sync_snapshot(_Name) ->
    snapshot().

%% Membership is lease-based; a peer leaving the active view is not
%% cluster death, so do nothing here (the lease expiry handles it).
replica_remove_node(_Name, _Node) ->
    ok.

-spec merge_delta(mycelium_ormap:ormap()) -> ok.
merge_delta(Delta) ->
    gen_server:cast(?SERVER, {merge_delta, Delta}).

-spec apply_full_sync(#{node() => integer()}) -> ok.
apply_full_sync(Snapshot) ->
    gen_server:cast(?SERVER, {apply_full_sync, Snapshot}).

-spec snapshot() -> {sync, #{node() => integer()}} | empty.
snapshot() ->
    gen_server:call(?SERVER, snapshot).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    RingSize = cfg(ring_size, ?DEFAULT_RING_SIZE),
    Hb = cfg(member_heartbeat_ms, ?DEFAULT_HEARTBEAT_MS),
    Ttl = cfg(member_ttl_ms, ?DEFAULT_TTL_MS),
    Skew = cfg(member_skew_ms, ?DEFAULT_SKEW_MS),
    ?TAB = ets:new(?TAB, [named_table, protected, set, {read_concurrency, true}]),
    ets:insert(?TAB, {ring_size, RingSize}),
    %% Self is always live; seed the lease and publish before anyone
    %% can call place/1.
    Now = now_ms(),
    Leases = #{node() => Now},
    Members = [node()],
    ets:insert(?TAB, {members, Members}),
    Owned = compute_owned(Members, RingSize),
    %% Convergence hints: heartbeats reach the whole cluster, and a new
    %% peer triggers a full-sync (in the replica) plus an immediate beat.
    %% Keep the subscription alive across a hyparview-events restart.
    Watch = mycelium_source_monitor:start([mycelium_hyparview_events]),
    %% Do NOT broadcast inline here: the replica process may not be
    %% registered yet, and broadcast_update/2 is a cast that would be
    %% dropped. The first heartbeat fires from the timer.
    arm_timer(Hb),
    {ok, #state{
        ring_size = RingSize,
        heartbeat_ms = Hb,
        ttl_ms = Ttl,
        skew_ms = Skew,
        leases = Leases,
        members = Members,
        owned = Owned,
        watch = Watch
    }}.

handle_call({subscribe, Pid}, _From, State) ->
    case maps:is_key(Pid, State#state.subscribers) of
        true ->
            {reply, ok, State};
        false ->
            Ref = monitor(process, Pid),
            Subs = maps:put(Pid, Ref, State#state.subscribers),
            {reply, ok, State#state{subscribers = Subs}}
    end;
handle_call({unsubscribe, Pid}, _From, State) ->
    case maps:take(Pid, State#state.subscribers) of
        {Ref, Subs} ->
            demonitor(Ref, [flush]),
            {reply, ok, State#state{subscribers = Subs}};
        error ->
            {reply, ok, State}
    end;
handle_call(snapshot, _From, State) ->
    Now = now_ms(),
    Ttl = State#state.ttl_ms,
    Live = maps:filter(
        fun(_N, Emit) -> Now - Emit =< Ttl end,
        State#state.leases
    ),
    Reply =
        case map_size(Live) of
            0 -> empty;
            _ -> {sync, Live}
        end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({merge_delta, Delta}, State) ->
    Now = now_ms(),
    Skew = State#state.skew_ms,
    %% Accept only plausible heartbeats. The guard is a FUTURE bound:
    %% reject `Emit > Now + skew' so a fast clock cannot pin a dead node.
    %% Past-staleness is handled by the TTL, not here.
    Accepted = maps:filter(
        fun
            (_Node, {value, {alive, Emit}, _Dots}) -> Emit =< Now + Skew;
            (_Node, _Other) -> false
        end,
        Delta
    ),
    %% Filter BEFORE absorbing: mycelium_hlc:update/1 accepts future
    %% timestamps, so absorbing a rejected far-future dot would still
    %% move our clock forward.
    ok = mycelium_ormap:absorb_clock(Accepted),
    Leases = maps:fold(
        fun
            (Node, {value, {alive, Emit}, _Dots}, Acc) ->
                lww(Node, Emit, Acc);
            (_Node, _Other, Acc) ->
                Acc
        end,
        State#state.leases,
        Accepted
    ),
    {noreply, recompute(State#state{leases = Leases})};
handle_cast({apply_full_sync, Snapshot}, State) ->
    Now = now_ms(),
    Skew = State#state.skew_ms,
    %% Snapshot carries plain wall-clock leases (no OR-Map dots), so
    %% there is nothing to absorb; just future-bound and LWW-merge.
    Leases = maps:fold(
        fun
            (Node, Emit, Acc) when Emit =< Now + Skew -> lww(Node, Emit, Acc);
            (_Node, _Emit, Acc) -> Acc
        end,
        State#state.leases,
        Snapshot
    ),
    {noreply, recompute(State#state{leases = Leases})};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(heartbeat, State) ->
    State1 = do_heartbeat(State),
    arm_timer(State1#state.heartbeat_ms),
    {noreply, State1};
%% A new peer: gossip our liveness immediately to speed convergence
%% (the replica also full-syncs the member set on peer_up).
handle_info({mycelium_event, {peer_up, _Node}}, State) ->
    {noreply, do_heartbeat(State)};
handle_info({mycelium_event, _Other}, State) ->
    {noreply, State};
%% Re-subscribe if a watched source (hyparview events) restarted.
handle_info({mycelium_source_monitor, retry, Source}, State) ->
    {noreply, State#state{
        watch = mycelium_source_monitor:retry(Source, State#state.watch)
    }};
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case mycelium_source_monitor:down(Ref, State#state.watch) of
        {down, _Source, Watch} ->
            {noreply, State#state{watch = Watch}};
        ignore ->
            case maps:get(Pid, State#state.subscribers, undefined) of
                Ref ->
                    Subs = maps:remove(Pid, State#state.subscribers),
                    {noreply, State#state{subscribers = Subs}};
                _ ->
                    {noreply, State}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Refresh our own lease, gossip it, then sweep + recompute.
do_heartbeat(State) ->
    Now = now_ms(),
    Leases = maps:put(node(), Now, State#state.leases),
    mycelium_replica:broadcast_update(?REPLICA, {add, node(), {alive, Now}}),
    recompute(State#state{leases = Leases}).

%% GC expired leases from the local map (bounding its size), derive the
%% live set, and on a change publish it + emit ownership transitions.
recompute(State) ->
    Now = now_ms(),
    Ttl = State#state.ttl_ms,
    Leases = maps:filter(
        fun(_N, Emit) -> Now - Emit =< Ttl end,
        State#state.leases
    ),
    Members = lists:sort(maps:keys(Leases)),
    case Members =:= State#state.members of
        true ->
            State#state{leases = Leases};
        false ->
            ets:insert(?TAB, {members, Members}),
            NewOwned = compute_owned(Members, State#state.ring_size),
            emit_changes(State#state.owned, NewOwned, State#state.subscribers),
            State#state{leases = Leases, members = Members, owned = NewOwned}
    end.

lww(Node, Emit, Acc) ->
    case Emit > maps:get(Node, Acc, 0) of
        true -> maps:put(Node, Emit, Acc);
        false -> Acc
    end.

compute_owned(Members, RingSize) ->
    Self = node(),
    maps:from_list(
        [
            {P, true}
         || P <- lists:seq(0, RingSize - 1),
            owner(P, Members) =:= Self
        ]
    ).

emit_changes(OldOwned, NewOwned, Subs) ->
    Acquired = maps:keys(maps:without(maps:keys(OldOwned), NewOwned)),
    Released = maps:keys(maps:without(maps:keys(NewOwned), OldOwned)),
    lists:foreach(fun(P) -> notify(Subs, {acquired, P}) end, Acquired),
    lists:foreach(fun(P) -> notify(Subs, {released, P}) end, Released).

notify(Subs, Event) ->
    maps:foreach(fun(Pid, _Ref) -> Pid ! {mycelium_shard, Event} end, Subs).

%% HRW: the node maximizing {phash2({Node, P}), Node}. The trailing
%% Node is a deterministic tie-breaker (phash2 can collide; ownership
%% must not depend on traversal order).
owner(_P, []) ->
    undefined;
owner(P, Members) ->
    {_Score, Node} = lists:max([{erlang:phash2({N, P}), N} || N <- Members]),
    Node.

ring_size() ->
    case ets:info(?TAB, name) of
        undefined ->
            ?DEFAULT_RING_SIZE;
        _ ->
            case ets:lookup(?TAB, ring_size) of
                [{ring_size, R}] -> R;
                [] -> ?DEFAULT_RING_SIZE
            end
    end.

arm_timer(Hb) ->
    erlang:send_after(Hb, self(), heartbeat).

now_ms() ->
    erlang:system_time(millisecond).

cfg(Key, Default) ->
    application:get_env(mycelium, Key, Default).
