%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_hyparview).
-behaviour(gen_server).
-include("mycelium.hrl").

%% API
-export([start_link/1, join/1, leave/0]).
-export([active_view/0, passive_view/0]).
-export([peer_connected/2, peer_disconnected/2, peer_failed/2]).
-export([initiate_shuffle/2]).

%% Churn handling API
-export([get_churn_stats/0, cleanup_passive_view/0]).

%% Test-support: hold a partition by refusing overlay links to given nodes.
-export([block_peers/1, unblock_peers/0]).

%% Protocol handlers (called by mycelium_protocol)
-export([handle_msg/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports - internal functions exposed for unit testing
-ifdef(TEST).
-export([
    record_churn_event/2,
    maybe_reset_churn_window/2,
    record_peer_failure/2,
    do_cleanup_passive_view/1,
    find_eligible_passive_peer/2,
    is_backoff_expired/2,
    last_seen_cmp/2,
    make_peer/1,
    add_to_passive/3
]).
-endif.

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%% @doc Test-support: refuse overlay links to `Nodes' - drop them from the
%% active/passive views, stop gossiping to them, and never (re)connect them -
%% until `unblock_peers/0'. Holds a partition that `disconnect_node' alone
%% cannot (HyParView would re-dial via passive promotion). Inert in normal
%% operation: nothing calls it and the blocked set defaults empty.
-spec block_peers([node()]) -> ok.
block_peers(Nodes) ->
    gen_server:call(?SERVER, {block_peers, Nodes}).

%% @doc Test-support: clear the blocked set (heal the partition).
-spec unblock_peers() -> ok.
unblock_peers() ->
    gen_server:call(?SERVER, unblock_peers).

-spec join(node()) -> ok | {error, term()}.
join(ContactNode) ->
    gen_server:call(?SERVER, {join, ContactNode}).

-spec leave() -> ok.
leave() ->
    gen_server:call(?SERVER, leave).

-spec active_view() -> [node()].
active_view() ->
    gen_server:call(?SERVER, active_view).

-spec passive_view() -> [node()].
passive_view() ->
    gen_server:call(?SERVER, passive_view).

%% Called by bridge when Erlang connection established/lost
-spec peer_connected(node(), term()) -> ok.
peer_connected(Node, DHandle) ->
    gen_server:cast(?SERVER, {peer_connected, Node, DHandle}).

-spec peer_disconnected(node(), term()) -> ok.
peer_disconnected(Node, Reason) ->
    gen_server:cast(?SERVER, {peer_disconnected, Node, Reason}).

-spec peer_failed(node(), term()) -> ok.
peer_failed(Node, Reason) ->
    gen_server:cast(?SERVER, {peer_failed, Node, Reason}).

%% Called by shuffle timer
-spec initiate_shuffle(node(), pos_integer()) -> ok.
initiate_shuffle(Target, ShuffleLength) ->
    gen_server:cast(?SERVER, {initiate_shuffle, Target, ShuffleLength}).

%% Churn stats for adaptive shuffle
-spec get_churn_stats() -> {Joins :: non_neg_integer(), Leaves :: non_neg_integer()}.
get_churn_stats() ->
    gen_server:call(?SERVER, get_churn_stats).

%% Trigger passive view cleanup (called by cleanup worker)
-spec cleanup_passive_view() -> ok.
cleanup_passive_view() ->
    gen_server:cast(?SERVER, cleanup_passive_view).

%% Called by mycelium_protocol when HyParView message received
-spec handle_msg(hyparview_msg(), node()) -> ok.
handle_msg(Msg, From) ->
    gen_server:cast(?SERVER, {protocol_msg, Msg, From}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    Self = #peer{
        id = node(),
        address = get_self_address(),
        port = get_self_port(),
        connected = true,
        priority = high
    },
    Now = erlang:monotonic_time(millisecond),
    State = #view_state{
        active_size = maps:get(active_size, Config, 5),
        passive_size = maps:get(passive_size, Config, 30),
        arwl = maps:get(arwl, Config, 6),
        prwl = maps:get(prwl, Config, 3),
        shuffle_length = maps:get(shuffle_length, Config, 8),
        shuffle_period = maps:get(shuffle_period, Config, 10000),
        self = Self,
        %% Churn handling
        max_fail_count = maps:get(max_fail_count, Config, 5),
        base_backoff_ms = maps:get(base_backoff_ms, Config, 1000),
        passive_max_age_ms = maps:get(passive_max_age_ms, Config, 300000),
        churn_window_ms = maps:get(churn_window_ms, Config, 30000),
        churn_window_start = Now
    },
    {ok, State}.

handle_call({join, ContactNode}, _From, State) ->
    case ContactNode =:= node() of
        true ->
            {reply, {error, cannot_join_self}, State};
        false ->
            case is_blocked(ContactNode, State) of
                true ->
                    {reply, ok, State};
                false ->
                    Pending = insert_pending(
                        ContactNode, join, State#view_state.pending
                    ),
                    mycelium_bridge:request_connect(ContactNode),
                    {reply, ok, State#view_state{pending = Pending}}
            end
    end;
handle_call(leave, _From, State) ->
    Self = State#view_state.self,
    %% HyParView-level leave: tell every active peer we no longer
    %% participate in gossip. Dist channels stay up; mycelium_dist_gc
    %% will reap them once they go idle and carry no live user streams.
    maps:foreach(
        fun(Node, _Peer) ->
            mycelium_protocol:send(Node, {disconnect, Self})
        end,
        State#view_state.active_view
    ),
    mycelium_hyparview_events:notify(left),
    {reply, ok, State#view_state{active_view = #{}, passive_view = #{}}};
handle_call({block_peers, Nodes}, _From, State) ->
    %% Drop the blocked nodes from both views and notify peer_down so
    %% Plumtree stops gossiping to them. The guards (is_blocked/2) keep
    %% them out until unblock_peers/0.
    [
        mycelium_hyparview_events:notify({peer_down, N, blocked})
     || N <- Nodes, maps:is_key(N, State#view_state.active_view)
    ],
    Blocked = lists:usort(Nodes ++ State#view_state.blocked),
    State1 = State#view_state{
        blocked = Blocked,
        active_view = maps:without(Nodes, State#view_state.active_view),
        passive_view = maps:without(Nodes, State#view_state.passive_view)
    },
    {reply, ok, State1};
handle_call(unblock_peers, _From, State) ->
    {reply, ok, State#view_state{blocked = []}};
handle_call(active_view, _From, State) ->
    {reply, maps:keys(State#view_state.active_view), State};
handle_call(passive_view, _From, State) ->
    {reply, maps:keys(State#view_state.passive_view), State};
handle_call(get_churn_stats, _From, State) ->
    %% Reset window if expired
    Now = erlang:monotonic_time(millisecond),
    State1 = maybe_reset_churn_window(Now, State),
    {reply, {State1#view_state.recent_joins, State1#view_state.recent_leaves}, State1};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({peer_connected, Node, _DHandle}, State) ->
    case maps:take(Node, State#view_state.pending) of
        {{join, _Ref, TimerRef}, NewPending} ->
            %% Initial join - send JOIN message
            cancel_pending_timer(TimerRef),
            Peer = make_peer(Node),
            Active = maps:put(Node, Peer, State#view_state.active_view),
            mycelium_protocol:send(Node, {join, State#view_state.self}),
            mycelium_hyparview_events:notify({peer_up, Node}),
            {noreply, State#view_state{active_view = Active, pending = NewPending}};
        {{connect, _Ref, TimerRef}, NewPending} ->
            %% Regular connect from passive promotion
            cancel_pending_timer(TimerRef),
            Peer = make_peer(Node),
            State1 = add_to_active_view(Peer, State#view_state{pending = NewPending}),
            mycelium_hyparview_events:notify({peer_up, Node}),
            {noreply, State1};
        error ->
            %% Incoming connection or unknown
            case maps:is_key(Node, State#view_state.active_view) of
                true ->
                    %% Already in active view
                    {noreply, State};
                false ->
                    Peer = make_peer(Node),
                    State1 = add_to_active_view(Peer, State),
                    mycelium_hyparview_events:notify({peer_up, Node}),
                    {noreply, State1}
            end
    end;
handle_cast({peer_disconnected, Node, Reason}, State) ->
    handle_peer_removal(Node, Reason, graceful, State);
handle_cast({peer_failed, Node, Reason}, State) ->
    handle_peer_removal(Node, Reason, failed, State);
handle_cast(cleanup_passive_view, State) ->
    State1 = do_cleanup_passive_view(State),
    {noreply, State1};
handle_cast({initiate_shuffle, Target, ShuffleLength}, State) ->
    case maps:is_key(Target, State#view_state.active_view) of
        true ->
            Peers = random_peers(State, ShuffleLength),
            TTL = State#view_state.prwl,
            Self = State#view_state.self,
            mycelium_protocol:send(Target, {shuffle, TTL, Peers, Self}),
            {noreply, State};
        false ->
            {noreply, State}
    end;
handle_cast({protocol_msg, {join, Sender}, _From}, State) ->
    State1 = handle_join(Sender, State),
    {noreply, State1};
handle_cast({protocol_msg, {forward_join, NewPeer, TTL, Sender}, _From}, State) ->
    State1 = handle_forward_join(NewPeer, TTL, Sender, State),
    {noreply, State1};
handle_cast({protocol_msg, {disconnect, Sender}, _From}, State) ->
    State1 = handle_disconnect(Sender, State),
    {noreply, State1};
handle_cast({protocol_msg, {neighbor, Priority, Sender}, _From}, State) ->
    State1 = handle_neighbor(Priority, Sender, State),
    {noreply, State1};
handle_cast({protocol_msg, {neighbor_reply, Accept, Sender}, _From}, State) ->
    State1 = handle_neighbor_reply(Accept, Sender, State),
    {noreply, State1};
handle_cast({protocol_msg, {shuffle, TTL, Peers, Sender}, _From}, State) ->
    State1 = handle_shuffle(TTL, Peers, Sender, State),
    {noreply, State1};
handle_cast({protocol_msg, {shuffle_reply, Peers, _Sender}, _From}, State) ->
    State1 = handle_shuffle_reply(Peers, State),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({pending_timeout, Node}, State) ->
    %% Backstop: a peer never produced peer_connected or peer_failed.
    %% Drop the pending entry so it doesn't leak across long uptime.
    case maps:take(Node, State#view_state.pending) of
        {_Entry, NewPending} ->
            mycelium_metrics:pending_timeout(Node),
            {noreply, State#view_state{pending = NewPending}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Pending-entry helpers
%%====================================================================

%% Default backstop for pending entries. Backstop must be longer
%% than `auth_handshake_timeout' (default 10s) so the happy-path
%% peer_connected/peer_failed reach us first.
-define(DEFAULT_PENDING_TIMEOUT_MS, 30000).

%% Insert a `{Kind, Ref, TimerRef}' entry into the pending map.
insert_pending(Node, Kind, Pending) ->
    Ref = make_ref(),
    Timeout = application:get_env(
        mycelium, pending_timeout_ms, ?DEFAULT_PENDING_TIMEOUT_MS
    ),
    TimerRef = erlang:send_after(Timeout, self(), {pending_timeout, Node}),
    maps:put(Node, {Kind, Ref, TimerRef}, Pending).

pending_timer({_Kind, _Ref, TimerRef}) -> TimerRef;
pending_timer(_) -> undefined.

cancel_pending_timer(TimerRef) when is_reference(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    ok;
cancel_pending_timer(_) ->
    ok.

%%====================================================================
%% HyParView Protocol Handlers
%%====================================================================

handle_join(Sender, State) ->
    %% Track churn event
    State0 = record_churn_event(join, State),

    %% Add sender to active view
    State1 = add_to_active_view(Sender, State0),

    %% Surface the new peer to subscribers (plumtree's eager_peers list,
    %% the replica drivers, etc). Without this, broadcast trees miss any
    %% peer that joined via the JOIN handshake.
    case maps:is_key(Sender#peer.id, State#view_state.active_view) of
        true ->
            %% Already had them; no transition.
            ok;
        false ->
            mycelium_hyparview_events:notify({peer_up, Sender#peer.id})
    end,

    %% Forward join to all active peers (except sender)
    TTL = State1#view_state.arwl,
    Self = State1#view_state.self,
    maps:foreach(
        fun
            (Node, _P) when Node =/= Sender#peer.id ->
                mycelium_protocol:send(Node, {forward_join, Sender, TTL, Self});
            (_, _) ->
                ok
        end,
        State1#view_state.active_view
    ),

    State1.

handle_forward_join(NewPeer, 0, _Sender, State) ->
    %% TTL expired - add to passive view if not self
    case NewPeer#peer.id =:= node() of
        true ->
            State;
        false ->
            Passive = add_to_passive(
                NewPeer,
                State#view_state.passive_view,
                State#view_state.passive_size
            ),
            State#view_state{passive_view = Passive}
    end;
handle_forward_join(NewPeer, _TTL, _Sender, State) when NewPeer#peer.id =:= node() ->
    %% Ignore forward_join for self
    State;
handle_forward_join(NewPeer, TTL, Sender, State) ->
    ActiveSize = maps:size(State#view_state.active_view),
    PRWL = State#view_state.prwl,

    %% If TTL = PRWL, add to passive view
    State1 =
        case TTL =:= PRWL of
            true ->
                Passive = add_to_passive(
                    NewPeer,
                    State#view_state.passive_view,
                    State#view_state.passive_size
                ),
                State#view_state{passive_view = Passive};
            false ->
                State
        end,

    case
        ActiveSize < State1#view_state.active_size andalso
            not is_blocked(NewPeer#peer.id, State1)
    of
        true ->
            %% Room in active view - connect to new peer
            Pending = insert_pending(
                NewPeer#peer.id, connect, State1#view_state.pending
            ),
            mycelium_bridge:request_connect(NewPeer#peer.id),
            State1#view_state{pending = Pending};
        false ->
            %% Forward to random active peer
            Exclude = [Sender#peer.id, NewPeer#peer.id],
            case random_active_peer(State1#view_state.active_view, Exclude) of
                {ok, Target} ->
                    Self = State1#view_state.self,
                    mycelium_protocol:send(Target, {forward_join, NewPeer, TTL - 1, Self});
                none ->
                    ok
            end,
            State1
    end.

handle_disconnect(Sender, State) ->
    case maps:is_key(Sender#peer.id, State#view_state.active_view) of
        true ->
            %% Track churn event
            State0 = record_churn_event(leave, State),
            Active = maps:remove(Sender#peer.id, State0#view_state.active_view),
            Passive = add_to_passive(
                Sender,
                State0#view_state.passive_view,
                State0#view_state.passive_size
            ),
            mycelium_hyparview_events:notify({peer_down, Sender#peer.id, graceful}),
            State1 = State0#view_state{active_view = Active, passive_view = Passive},
            maybe_promote_passive(State1);
        false ->
            State
    end.

handle_neighbor(Priority, Sender, State) ->
    ActiveSize = maps:size(State#view_state.active_view),
    Accept =
        case Priority of
            high -> true;
            low -> ActiveSize < State#view_state.active_size
        end,
    mycelium_protocol:send(Sender#peer.id, {neighbor_reply, Accept, State#view_state.self}),
    case Accept of
        true ->
            add_to_active_view(Sender, State);
        false ->
            State
    end.

handle_neighbor_reply(true, Sender, State) ->
    add_to_active_view(Sender, State);
handle_neighbor_reply(false, _Sender, State) ->
    %% Rejected - try another passive peer
    maybe_promote_passive(State).

handle_shuffle(TTL, Peers, Sender, State) ->
    %% Filter out self from received peers
    FilteredPeers = [P || P <- Peers, P#peer.id =/= node()],

    %% Add received peers to passive view
    Passive = lists:foldl(
        fun(P, Acc) ->
            case maps:is_key(P#peer.id, State#view_state.active_view) of
                true -> Acc;
                false -> add_to_passive(P, Acc, State#view_state.passive_size)
            end
        end,
        State#view_state.passive_view,
        FilteredPeers
    ),

    %% Send reply with our random peers
    ReplyPeers = random_peers(State, length(FilteredPeers)),
    mycelium_protocol:send(Sender#peer.id, {shuffle_reply, ReplyPeers, State#view_state.self}),

    case TTL > 0 of
        true ->
            %% Forward to random active peer
            Exclude = [Sender#peer.id],
            case random_active_peer(State#view_state.active_view, Exclude) of
                {ok, Target} ->
                    Self = State#view_state.self,
                    mycelium_protocol:send(Target, {shuffle, TTL - 1, FilteredPeers, Self});
                none ->
                    ok
            end;
        false ->
            ok
    end,

    State#view_state{passive_view = Passive}.

handle_shuffle_reply(Peers, State) ->
    %% Filter out self and nodes already in active view
    FilteredPeers = [
        P
     || P <- Peers,
        P#peer.id =/= node(),
        not maps:is_key(P#peer.id, State#view_state.active_view)
    ],
    Passive = lists:foldl(
        fun(P, Acc) ->
            add_to_passive(P, Acc, State#view_state.passive_size)
        end,
        State#view_state.passive_view,
        FilteredPeers
    ),
    State#view_state{passive_view = Passive}.

%%====================================================================
%% Internal Functions
%%====================================================================

handle_peer_removal(Node, Reason, Type, State) ->
    %% Track churn event
    State0 = record_churn_event(leave, State),
    case maps:is_key(Node, State0#view_state.active_view) of
        true ->
            Active = maps:remove(Node, State0#view_state.active_view),
            State1 =
                case Type of
                    graceful ->
                        Peer = maps:get(Node, State0#view_state.active_view),
                        Passive = add_to_passive(
                            Peer,
                            State0#view_state.passive_view,
                            State0#view_state.passive_size
                        ),
                        State0#view_state{active_view = Active, passive_view = Passive};
                    failed ->
                        %% Track failure in passive view if present
                        Passive = record_peer_failure(Node, State0),
                        State0#view_state{active_view = Active, passive_view = Passive}
                end,
            mycelium_hyparview_events:notify({peer_down, Node, Reason}),
            State2 = maybe_promote_passive(State1),
            {noreply, State2};
        false ->
            %% Check if this was a pending connection that failed
            case maps:take(Node, State0#view_state.pending) of
                {{neighbor, _Ref, TimerRef}, NewPending} ->
                    %% Failed neighbor request - track failure and try another
                    cancel_pending_timer(TimerRef),
                    Passive = record_peer_failure(Node, State0),
                    State1 = State0#view_state{pending = NewPending, passive_view = Passive},
                    State2 = maybe_promote_passive(State1),
                    {noreply, State2};
                {Entry, NewPending} ->
                    cancel_pending_timer(pending_timer(Entry)),
                    Passive = record_peer_failure(Node, State0),
                    {noreply, State0#view_state{pending = NewPending, passive_view = Passive}};
                error ->
                    {noreply, State0}
            end
    end.

make_peer(Node) ->
    #peer{
        id = Node,
        address = undefined,
        port = undefined,
        connected = true,
        priority = low,
        last_seen = erlang:monotonic_time()
    }.

add_to_active_view(Peer, State) ->
    case is_blocked(Peer#peer.id, State) of
        true ->
            %% Refuse: a blocked peer must never enter the overlay.
            State;
        false ->
            do_add_to_active_view(Peer, State)
    end.

do_add_to_active_view(Peer, State) ->
    Active = State#view_state.active_view,
    case maps:size(Active) < State#view_state.active_size of
        true ->
            NewActive = maps:put(Peer#peer.id, Peer, Active),
            State#view_state{active_view = NewActive};
        false ->
            %% Need to drop someone
            Exclude = [Peer#peer.id],
            {DroppedNode, DroppedPeer} = random_active_peer_pair(Active, Exclude),

            %% Send HyParView-level disconnect: the peer learns we no
            %% longer treat it as an active gossip peer. The dist channel
            %% itself stays up; mycelium_dist_gc reaps it later if it goes
            %% idle and carries no live user streams.
            mycelium_protocol:send(DroppedNode, {disconnect, State#view_state.self}),
            mycelium_hyparview_events:notify({peer_down, DroppedNode, dropped}),

            Active1 = maps:remove(DroppedNode, Active),
            Active2 = maps:put(Peer#peer.id, Peer, Active1),

            %% Move dropped to passive
            Passive = add_to_passive(
                DroppedPeer,
                State#view_state.passive_view,
                State#view_state.passive_size
            ),
            State#view_state{active_view = Active2, passive_view = Passive}
    end.

add_to_passive(Peer, Passive, MaxSize) ->
    %% Don't add self to passive view
    case Peer#peer.id =:= node() of
        true ->
            Passive;
        false ->
            case maps:is_key(Peer#peer.id, Passive) of
                true ->
                    %% Update existing entry
                    maps:put(Peer#peer.id, Peer#peer{connected = false}, Passive);
                false when map_size(Passive) >= MaxSize ->
                    %% Full, need to drop random
                    {ToRemove, _} = random_active_peer_pair(Passive, [Peer#peer.id]),
                    Passive1 = maps:remove(ToRemove, Passive),
                    maps:put(Peer#peer.id, Peer#peer{connected = false}, Passive1);
                false ->
                    maps:put(Peer#peer.id, Peer#peer{connected = false}, Passive)
            end
    end.

maybe_promote_passive(State) ->
    ActiveSize = maps:size(State#view_state.active_view),
    case ActiveSize < State#view_state.active_size of
        true ->
            Now = erlang:monotonic_time(millisecond),
            case find_eligible_passive_peer(State, Now) of
                {ok, Node, _Peer} ->
                    Passive = maps:remove(Node, State#view_state.passive_view),

                    %% Send NEIGHBOR request with priority based on active view state
                    Priority =
                        case ActiveSize of
                            0 -> high;
                            _ -> low
                        end,

                    Pending = insert_pending(
                        Node, neighbor, State#view_state.pending
                    ),
                    mycelium_protocol:send(Node, {neighbor, Priority, State#view_state.self}),
                    mycelium_bridge:request_connect(Node),
                    State#view_state{passive_view = Passive, pending = Pending};
                none ->
                    State
            end;
        false ->
            State
    end.

%% Find a passive peer eligible for promotion (not in backoff, not too many
%% failures, not blocked).
find_eligible_passive_peer(State, Now) ->
    MaxFails = State#view_state.max_fail_count,
    Blocked = State#view_state.blocked,
    Candidates = maps:to_list(State#view_state.passive_view),
    Eligible = [
        {N, P}
     || {N, P} <- Candidates,
        P#peer.fail_count < MaxFails,
        is_backoff_expired(P, Now),
        not lists:member(N, Blocked)
    ],
    case Eligible of
        [] ->
            none;
        _ ->
            %% Prefer recently seen peers (more likely to be alive)
            Sorted = lists:sort(
                fun({_, A}, {_, B}) ->
                    last_seen_cmp(A, B)
                end,
                Eligible
            ),
            {Node, Peer} = hd(Sorted),
            {ok, Node, Peer}
    end.

%% Test-support: is this node currently blocked (partition held)? Always
%% false in normal operation (the blocked set defaults empty).
is_blocked(Node, #view_state{blocked = Blocked}) ->
    lists:member(Node, Blocked).

%% Compare peers by last_seen (more recent first)
last_seen_cmp(#peer{last_seen = undefined}, #peer{last_seen = _}) -> false;
last_seen_cmp(#peer{last_seen = _}, #peer{last_seen = undefined}) -> true;
last_seen_cmp(#peer{last_seen = A}, #peer{last_seen = B}) -> A > B.

%% Check if backoff period has expired
is_backoff_expired(#peer{backoff_until = undefined}, _Now) -> true;
is_backoff_expired(#peer{backoff_until = Until}, Now) -> Now >= Until.

random_active_peer(Active, Exclude) ->
    Candidates = maps:keys(Active) -- Exclude,
    case Candidates of
        [] -> none;
        _ -> {ok, lists:nth(rand:uniform(length(Candidates)), Candidates)}
    end.

random_active_peer_pair(Map, Exclude) ->
    Candidates = maps:to_list(Map),
    Filtered = [{K, V} || {K, V} <- Candidates, not lists:member(K, Exclude)],
    case Filtered of
        [] -> error(no_candidates);
        _ -> lists:nth(rand:uniform(length(Filtered)), Filtered)
    end.

random_peers(State, N) ->
    Active = maps:values(State#view_state.active_view),
    Passive = maps:values(State#view_state.passive_view),
    All = [State#view_state.self | Active ++ Passive],
    %% Shuffle and take N
    Shuffled = [X || {_, X} <- lists:sort([{rand:uniform(), P} || P <- All])],
    lists:sublist(Shuffled, N).

%%====================================================================
%% Churn Handling Functions
%%====================================================================

%% Record a churn event (join or leave)
record_churn_event(Type, State) ->
    Now = erlang:monotonic_time(millisecond),
    State1 = maybe_reset_churn_window(Now, State),
    case Type of
        join ->
            State1#view_state{recent_joins = State1#view_state.recent_joins + 1};
        leave ->
            State1#view_state{recent_leaves = State1#view_state.recent_leaves + 1}
    end.

%% Reset churn window if expired
maybe_reset_churn_window(Now, State) ->
    WindowStart = State#view_state.churn_window_start,
    WindowMs = State#view_state.churn_window_ms,
    case WindowStart =:= undefined orelse (Now - WindowStart) > WindowMs of
        true ->
            State#view_state{
                recent_joins = 0,
                recent_leaves = 0,
                churn_window_start = Now
            };
        false ->
            State
    end.

%% Record a peer failure in the passive view with exponential backoff
record_peer_failure(Node, State) ->
    Passive = State#view_state.passive_view,
    case maps:get(Node, Passive, undefined) of
        undefined ->
            Passive;
        Peer ->
            Now = erlang:monotonic_time(millisecond),
            NewFailCount = Peer#peer.fail_count + 1,
            MaxFails = State#view_state.max_fail_count,
            case NewFailCount >= MaxFails of
                true ->
                    %% Too many failures - remove from passive view
                    maps:remove(Node, Passive);
                false ->
                    %% Calculate exponential backoff: base * 2^fail_count
                    Base = State#view_state.base_backoff_ms,
                    %% 2^fail_count
                    BackoffMs = Base * (1 bsl NewFailCount),
                    %% Cap at 5 minutes
                    CappedBackoff = min(BackoffMs, 300000),
                    UpdatedPeer = Peer#peer{
                        fail_count = NewFailCount,
                        backoff_until = Now + CappedBackoff
                    },
                    maps:put(Node, UpdatedPeer, Passive)
            end
    end.

%% Cleanup passive view - remove stale and too-failed entries
do_cleanup_passive_view(State) ->
    Now = erlang:monotonic_time(millisecond),
    MaxAge = State#view_state.passive_max_age_ms,
    MaxFails = State#view_state.max_fail_count,

    Passive = maps:filter(
        fun(_Node, Peer) ->
            %% Keep if: not too many failures AND (recently seen OR no last_seen)
            FailOk = Peer#peer.fail_count < MaxFails,
            AgeOk =
                case Peer#peer.last_seen of
                    undefined -> true;
                    LastSeen -> (Now - LastSeen) < MaxAge
                end,
            FailOk andalso AgeOk
        end,
        State#view_state.passive_view
    ),

    State#view_state{passive_view = Passive}.

get_self_address() ->
    case application:get_env(mycelium, address) of
        {ok, Addr} -> Addr;
        undefined -> {127, 0, 0, 1}
    end.

get_self_port() ->
    case application:get_env(mycelium, listen_port) of
        {ok, Port} -> Port;
        undefined -> 0
    end.
