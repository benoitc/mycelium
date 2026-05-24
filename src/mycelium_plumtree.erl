%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_plumtree).
-behaviour(gen_server).

%% Plumtree: Epidemic Broadcast Trees
%% Efficient reliable broadcast over HyParView overlay
%%
%% Key concepts:
%% - Eager peers: Push messages immediately (fast path)
%% - Lazy peers: Send IHAVEs, request via GRAFT (recovery path)
%% - Self-healing: GRAFT repairs missing messages
%% - O(n) messages vs O(n²) for flooding

-include("mycelium.hrl").
-include_lib("hlc/include/hlc.hrl").

%% API
-export([start_link/0]).
-export([broadcast/2, broadcast/3]).
-export([subscribe/1, unsubscribe/1]).

%% Internal API (used by sync)
-export([get_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(PLUMTREE_TAG, '$mycelium_plumtree').
%% Time to wait before requesting via GRAFT
-define(IHAVE_TIMEOUT, 1000).
%% Keep messages for 5 minutes
-define(MESSAGE_TTL, 300000).
%% Cleanup old messages every minute
-define(CLEANUP_INTERVAL, 60000).

-record(state, {
    %% Peer classification

    %% Push immediately
    eager_peers = [] :: [node()],
    %% Send IHAVEs only
    lazy_peers = [] :: [node()],

    %% Message tracking (uses HLC timestamps for clock-skew-tolerant TTL)

    %% MsgId -> {Payload, HLC}
    received = #{} :: #{binary() => {term(), mycelium_hlc:timestamp()}},
    %% MsgId -> {Sender, TimerRef}
    pending_ihaves = #{} :: #{binary() => {node(), reference()}},

    %% Subscribers for delivered messages
    subscribers = #{} :: #{pid() => reference()},

    %% Keeps our own hyparview-events subscription alive across a restart
    watch = #{} :: mycelium_source_monitor:watch(),

    %% Stats
    gossip_sent = 0 :: non_neg_integer(),
    gossip_received = 0 :: non_neg_integer(),
    ihave_sent = 0 :: non_neg_integer(),
    graft_sent = 0 :: non_neg_integer(),
    prune_sent = 0 :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Broadcast a message to all nodes
-spec broadcast(term(), term()) -> ok.
broadcast(Tag, Payload) ->
    MsgId = generate_msg_id(),
    broadcast(Tag, Payload, MsgId).

-spec broadcast(term(), term(), binary()) -> ok.
broadcast(Tag, Payload, MsgId) ->
    gen_server:cast(?SERVER, {broadcast, Tag, Payload, MsgId}).

%% Subscribe to receive broadcast messages
-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
    gen_server:call(?SERVER, {subscribe, Pid}).

%% Unsubscribe from broadcast messages
-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}).

%% Get broadcast statistics
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Subscribe to HyParView events, keeping the subscription alive if
    %% the events process restarts (we need peer_up/peer_down to keep the
    %% eager/lazy peer lists correct).
    Watch = mycelium_source_monitor:start([mycelium_hyparview_events]),

    %% Initialize eager peers from current active view
    EagerPeers = mycelium:active_view(),

    %% Schedule periodic cleanup
    erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup),

    {ok, #state{eager_peers = EagerPeers, watch = Watch}}.

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
handle_call(get_stats, _From, State) ->
    Stats = #{
        eager_peers => length(State#state.eager_peers),
        lazy_peers => length(State#state.lazy_peers),
        cached_messages => maps:size(State#state.received),
        pending_ihaves => maps:size(State#state.pending_ihaves),
        gossip_sent => State#state.gossip_sent,
        gossip_received => State#state.gossip_received,
        ihave_sent => State#state.ihave_sent,
        graft_sent => State#state.graft_sent,
        prune_sent => State#state.prune_sent
    },
    {reply, Stats, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({broadcast, Tag, Payload, MsgId}, State) ->
    %% Check for duplicate
    case maps:is_key(MsgId, State#state.received) of
        true ->
            %% Already broadcast - ignore
            {noreply, State};
        false ->
            %% Store message locally with HLC timestamp
            HLC = mycelium_hlc:now(),
            Received = maps:put(MsgId, {{Tag, Payload}, HLC}, State#state.received),

            %% Deliver to local subscribers
            deliver_to_subscribers({Tag, Payload}, State#state.subscribers),

            %% Send GOSSIP to eager peers, IHAVE to lazy peers
            State2 = send_gossip(MsgId, Tag, Payload, node(), State#state.eager_peers, State),
            State3 = send_ihaves(MsgId, State#state.lazy_peers, State2),

            {noreply, State3#state{received = Received}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Receive GOSSIP message
handle_info({?PLUMTREE_TAG, {gossip, MsgId, Tag, Payload, Sender}}, State) ->
    mycelium_metrics:gossip_received(Sender),
    State2 = State#state{gossip_received = State#state.gossip_received + 1},
    case maps:is_key(MsgId, State2#state.received) of
        true ->
            %% Duplicate - send PRUNE to convert sender to lazy
            send_prune(Sender, State2),
            {noreply, State2#state{prune_sent = State2#state.prune_sent + 1}};
        false ->
            %% New message - store, deliver, and forward
            HLC = mycelium_hlc:now(),
            Received = maps:put(MsgId, {{Tag, Payload}, HLC}, State2#state.received),

            %% Cancel any pending IHAVE timer for this message
            State3 = cancel_pending_ihave(MsgId, State2#state{received = Received}),

            %% Deliver to local subscribers
            deliver_to_subscribers({Tag, Payload}, State3#state.subscribers),

            %% Forward to peers (excluding sender)
            EagerPeers = State3#state.eager_peers -- [Sender],
            LazyPeers = State3#state.lazy_peers -- [Sender],
            State4 = send_gossip(MsgId, Tag, Payload, node(), EagerPeers, State3),
            State5 = send_ihaves(MsgId, LazyPeers, State4),

            %% Ensure sender is in eager peers
            State6 = ensure_eager(Sender, State5),

            {noreply, State6}
    end;
%% Receive IHAVE notification
handle_info({?PLUMTREE_TAG, {ihave, MsgId, Sender}}, State) ->
    case maps:is_key(MsgId, State#state.received) of
        true ->
            %% Already have it - ignore
            {noreply, State};
        false ->
            %% Don't have it - schedule GRAFT request
            case maps:is_key(MsgId, State#state.pending_ihaves) of
                true ->
                    %% Already waiting for this message
                    {noreply, State};
                false ->
                    %% Start timer to request via GRAFT
                    TimerRef = erlang:send_after(
                        ?IHAVE_TIMEOUT, self(), {graft_timeout, MsgId, Sender}
                    ),
                    Pending = maps:put(MsgId, {Sender, TimerRef}, State#state.pending_ihaves),
                    {noreply, State#state{pending_ihaves = Pending}}
            end
    end;
%% GRAFT timeout - request the message
handle_info({graft_timeout, MsgId, Sender}, State) ->
    case maps:is_key(MsgId, State#state.received) of
        true ->
            %% Already received via another path
            Pending = maps:remove(MsgId, State#state.pending_ihaves),
            {noreply, State#state{pending_ihaves = Pending}};
        false ->
            %% Still missing - send GRAFT request
            send_graft(MsgId, Sender),
            Pending = maps:remove(MsgId, State#state.pending_ihaves),
            {noreply, State#state{
                pending_ihaves = Pending,
                graft_sent = State#state.graft_sent + 1
            }}
    end;
%% Receive GRAFT request
handle_info({?PLUMTREE_TAG, {graft, MsgId, Sender}}, State) ->
    %% Move sender to eager peers and send the message
    State2 = ensure_eager(Sender, State),
    case maps:get(MsgId, State2#state.received, undefined) of
        {{Tag, Payload}, _Time} ->
            send_gossip(MsgId, Tag, Payload, node(), [Sender], State2),
            {noreply, State2};
        undefined ->
            %% Don't have it anymore
            {noreply, State2}
    end;
%% Receive PRUNE request
handle_info({?PLUMTREE_TAG, {prune, Sender}}, State) ->
    %% Move sender from eager to lazy
    State2 = move_to_lazy(Sender, State),
    {noreply, State2};
%% HyParView events
handle_info({mycelium_event, {peer_up, Node}}, State) ->
    %% New peer joins - add to eager peers
    State2 = ensure_eager(Node, State),
    {noreply, State2};
handle_info({mycelium_event, {peer_down, Node, _Reason}}, State) ->
    %% Peer left - remove from all lists. mycelium_hyparview emits the
    %% 3-tuple form unconditionally; matching only on the 2-tuple let
    %% the broadcast tree keep dead peers in eager/lazy.
    EagerPeers = State#state.eager_peers -- [Node],
    LazyPeers = State#state.lazy_peers -- [Node],
    {noreply, State#state{eager_peers = EagerPeers, lazy_peers = LazyPeers}};
%% Re-subscribe if a watched source (hyparview events) restarted.
handle_info({mycelium_source_monitor, retry, Source}, State) ->
    {noreply, State#state{
        watch = mycelium_source_monitor:retry(Source, State#state.watch)
    }};
%% Source restart, or a subscriber, going down.
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
%% Periodic cleanup using HLC wall time
handle_info(cleanup, State) ->
    NowWall = mycelium_hlc:wall_time(mycelium_hlc:now()),
    Cutoff = NowWall - ?MESSAGE_TTL,

    %% Remove old messages based on HLC wall time
    Received = maps:filter(
        fun(_MsgId, {_Payload, HLC}) ->
            mycelium_hlc:wall_time(HLC) > Cutoff
        end,
        State#state.received
    ),

    erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup),
    {noreply, State#state{received = Received}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

generate_msg_id() ->
    %% Unique message ID using HLC for causal ordering
    HLC = mycelium_hlc:now(),
    Wall = mycelium_hlc:wall_time(HLC),
    Logical = mycelium_hlc:logical(HLC),
    Rand = rand:uniform(16#FFFFFFFF),
    Term = {node(), Wall, Logical, Rand},
    crypto:hash(sha256, term_to_binary(Term)).

send_gossip(MsgId, Tag, Payload, Origin, Peers, State) ->
    Msg = {?PLUMTREE_TAG, {gossip, MsgId, Tag, Payload, Origin}},
    lists:foreach(
        fun(Peer) ->
            erlang:send({?SERVER, Peer}, Msg, [nosuspend])
        end,
        Peers
    ),
    mycelium_metrics:gossip_sent(length(Peers)),
    State#state{gossip_sent = State#state.gossip_sent + length(Peers)}.

send_ihaves(MsgId, Peers, State) ->
    Msg = {?PLUMTREE_TAG, {ihave, MsgId, node()}},
    lists:foreach(
        fun(Peer) ->
            erlang:send({?SERVER, Peer}, Msg, [nosuspend])
        end,
        Peers
    ),
    mycelium_metrics:ihave_sent(length(Peers)),
    State#state{ihave_sent = State#state.ihave_sent + length(Peers)}.

send_graft(MsgId, Peer) ->
    Msg = {?PLUMTREE_TAG, {graft, MsgId, node()}},
    erlang:send({?SERVER, Peer}, Msg, [nosuspend]),
    mycelium_metrics:graft_sent(Peer).

send_prune(Peer, _State) ->
    Msg = {?PLUMTREE_TAG, {prune, node()}},
    erlang:send({?SERVER, Peer}, Msg, [nosuspend]),
    mycelium_metrics:prune_sent(Peer).

ensure_eager(Node, State) when Node =:= node() ->
    State;
ensure_eager(Node, State) ->
    case lists:member(Node, State#state.eager_peers) of
        true ->
            State;
        false ->
            LazyPeers = State#state.lazy_peers -- [Node],
            EagerPeers = [Node | State#state.eager_peers],
            State#state{eager_peers = EagerPeers, lazy_peers = LazyPeers}
    end.

move_to_lazy(Node, State) when Node =:= node() ->
    State;
move_to_lazy(Node, State) ->
    case lists:member(Node, State#state.eager_peers) of
        true ->
            EagerPeers = State#state.eager_peers -- [Node],
            LazyPeers = [Node | State#state.lazy_peers],
            State#state{eager_peers = EagerPeers, lazy_peers = LazyPeers};
        false ->
            State
    end.

cancel_pending_ihave(MsgId, State) ->
    case maps:take(MsgId, State#state.pending_ihaves) of
        {{_Sender, TimerRef}, Pending} ->
            erlang:cancel_timer(TimerRef),
            State#state{pending_ihaves = Pending};
        error ->
            State
    end.

deliver_to_subscribers(Message, Subscribers) ->
    maps:foreach(
        fun(Pid, _Ref) ->
            Pid ! {plumtree_broadcast, Message}
        end,
        Subscribers
    ).
