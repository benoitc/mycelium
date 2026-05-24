%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Cluster-wide singleton / leader election.
%%%
%%% Processes campaign for a named singleton with `lead/2'. The winner
%%% is computed independently and identically on every node from the
%%% replicated candidate set: highest `priority', ties broken by the
%%% lowest node atom. No consensus is involved, matching mycelium's
%%% AP/gossip posture.
%%%
%%% Each leadership term is handed a fencing token minted from the HLC
%%% and advanced past a replicated per-name high-water mark, so it is
%%% strictly monotonic within a connected partition. The leader stamps
%%% the token on writes; the protected resource rejects any operation
%%% whose token is not strictly greater than the highest it accepted.
%%%
%%% The candidate set is an OR-Map keyed `{Name, node()}', replicated
%%% through a `mycelium_replica' instance (gossip deltas, full-sync on
%%% peer_up, prune on peer_down). Fencing high-water marks ride the same
%%% instance as a custom broadcast.
-module(mycelium_leader).
-behaviour(gen_server).
-behaviour(mycelium_replica).

%% Registered name of this feature's replication instance.
-define(REPLICA, mycelium_leader_replica).

%% Public API
-export([lead/1, lead/2, resign/1, leader/1, is_leader/1, fence/1,
         candidates/1]).

%% Internal API (used by the replica callbacks below)
-export([start_link/0]).
-export([merge_remote/1, merge_fence/2, apply_full_sync/2, remove_node/1,
         get_state/0, high_water/1]).

%% mycelium_replica callbacks
-export([replica_merge_delta/2, replica_merge_custom/2,
         replica_apply_full_sync/2, replica_full_sync_snapshot/1,
         replica_remove_node/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-type name()  :: term().
-type fence() :: non_neg_integer().

-export_type([name/0, fence/0]).

-record(state, {
    %% This node's campaigns: Name -> {CandidatePid, MonitorRef, Priority}
    local = #{} :: #{name() => {pid(), reference(), integer()}},
    %% Replicated candidate set: {Name, node()} -> {Pid, Priority}
    cands = mycelium_ormap:new() :: mycelium_ormap:ormap(),
    %% Last-computed leader identity per *local* Name (transition detection)
    leaders = #{} :: #{name() => {node(), pid()}},
    %% Replicated per-name high-water mark (greatest term token observed)
    fences = #{} :: #{name() => mycelium_hlc:timestamp()},
    %% Token self minted for its current term (only while self leads)
    my_fence = #{} :: #{name() => mycelium_hlc:timestamp()}
}).

%%====================================================================
%% Public API
%%====================================================================

-spec lead(name()) -> {ok, {leader, fence()}} | {ok, follower} | {error, term()}.
lead(Name) ->
    lead(Name, #{}).

-spec lead(name(), map()) ->
    {ok, {leader, fence()}} | {ok, follower} | {error, term()}.
lead(Name, Opts) when is_map(Opts) ->
    Priority = maps:get(priority, Opts, 0),
    gen_server:call(?SERVER, {lead, Name, Priority}).

-spec resign(name()) -> ok.
resign(Name) ->
    gen_server:call(?SERVER, {resign, Name}).

-spec leader(name()) -> {ok, node(), pid()} | {error, no_leader}.
leader(Name) ->
    gen_server:call(?SERVER, {leader, Name}).

-spec is_leader(name()) -> boolean().
is_leader(Name) ->
    gen_server:call(?SERVER, {is_leader, Name}).

-spec fence(name()) -> {ok, fence()} | {error, not_leader}.
fence(Name) ->
    gen_server:call(?SERVER, {fence, Name}).

-spec candidates(name()) -> [node()].
candidates(Name) ->
    gen_server:call(?SERVER, {candidates, Name}).

%%====================================================================
%% Internal API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec merge_remote(mycelium_ormap:ormap()) -> ok.
merge_remote(Delta) ->
    gen_server:cast(?SERVER, {merge_remote, Delta}).

-spec merge_fence(name(), mycelium_hlc:timestamp()) -> ok.
merge_fence(Name, Fence) ->
    gen_server:cast(?SERVER, {merge_fence, Name, Fence}).

-spec apply_full_sync(mycelium_ormap:ormap(),
                      #{name() => mycelium_hlc:timestamp()}) -> ok.
apply_full_sync(Cands, Fences) ->
    gen_server:cast(?SERVER, {apply_full_sync, Cands, Fences}).

-spec remove_node(node()) -> ok.
remove_node(Node) ->
    gen_server:cast(?SERVER, {remove_node, Node}).

%% Returns {Cands, Fences} for a peer's full sync.
-spec get_state() -> {mycelium_ormap:ormap(), #{name() => mycelium_hlc:timestamp()}}.
get_state() ->
    gen_server:call(?SERVER, get_state).

%% Packed high-water token for a name, or undefined. For ops/debug.
-spec high_water(name()) -> fence() | undefined.
high_water(Name) ->
    gen_server:call(?SERVER, {high_water, Name}).

%%====================================================================
%% mycelium_replica callbacks
%%====================================================================

replica_merge_delta(_Inst, Delta) ->
    merge_remote(Delta).

replica_merge_custom(_Inst, {Name, Fence}) ->
    merge_fence(Name, Fence).

replica_apply_full_sync(_Inst, {Cands, Fences}) ->
    apply_full_sync(Cands, Fences).

replica_full_sync_snapshot(_Inst) ->
    {Cands, Fences} = get_state(),
    case mycelium_ormap:is_empty(Cands) andalso map_size(Fences) =:= 0 of
        true  -> empty;
        false -> {sync, {Cands, Fences}}
    end.

replica_remove_node(_Inst, Node) ->
    remove_node(Node).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({lead, Name, Priority}, {Pid, _Tag}, State) ->
    case maps:is_key(Name, State#state.local) of
        true ->
            {reply, {error, already_candidate}, State};
        false ->
            Ref = monitor(process, Pid),
            Key = {Name, node()},
            Val = {Pid, Priority},
            Cands = mycelium_ormap:add(Key, Val, State#state.cands),
            Local = maps:put(Name, {Pid, Ref, Priority}, State#state.local),
            mycelium_replica:broadcast_update(?REPLICA, {add, Key, Val}),
            State1 = State#state{cands = Cands, local = Local},
            %% We just added ourselves, so leader_of/2 is non-empty.
            {LNode, LPid} = leader_of(Name, Cands),
            case LNode =:= node() of
                true ->
                    {Fence, State2} = become_leader(Name, State1),
                    Leaders = maps:put(Name, {LNode, LPid}, State2#state.leaders),
                    {reply, {ok, {leader, Fence}},
                     State2#state{leaders = Leaders}};
                false ->
                    Leaders = maps:put(Name, {LNode, LPid}, State1#state.leaders),
                    {reply, {ok, follower}, State1#state{leaders = Leaders}}
            end
    end;

handle_call({resign, Name}, _From, State) ->
    {reply, ok, drop_local(Name, demonitor, State)};

handle_call({leader, Name}, _From, State) ->
    case leader_of(Name, State#state.cands) of
        none          -> {reply, {error, no_leader}, State};
        {Node, Pid}   -> {reply, {ok, Node, Pid}, State}
    end;

handle_call({is_leader, Name}, _From, State) ->
    Reply = case leader_of(Name, State#state.cands) of
        {Node, _Pid} -> Node =:= node();
        none         -> false
    end,
    {reply, Reply, State};

handle_call({fence, Name}, _From, State) ->
    Reply = case is_self_leader(maps:get(Name, State#state.leaders, none)) of
        true ->
            case maps:get(Name, State#state.my_fence, undefined) of
                undefined -> {error, not_leader};
                F         -> {ok, mycelium_hlc:pack(F)}
            end;
        false ->
            {error, not_leader}
    end,
    {reply, Reply, State};

handle_call({candidates, Name}, _From, State) ->
    Nodes = [Node || {{N, Node}, _V} <- mycelium_ormap:to_list(State#state.cands),
                     N =:= Name],
    {reply, lists:usort(Nodes), State};

handle_call({high_water, Name}, _From, State) ->
    Reply = case maps:get(Name, State#state.fences, undefined) of
        undefined -> undefined;
        F         -> mycelium_hlc:pack(F)
    end,
    {reply, Reply, State};

handle_call(get_state, _From, State) ->
    {reply, {State#state.cands, State#state.fences}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({merge_remote, Delta}, State) ->
    mycelium_ormap:absorb_clock(Delta),
    Cands = mycelium_ormap:merge(State#state.cands, Delta),
    {noreply, recompute_local(State#state{cands = Cands})};

handle_cast({merge_fence, Name, F}, State) ->
    mycelium_hlc:update(F),
    Fences = raise_fence(State#state.fences, Name, F),
    {noreply, State#state{fences = Fences}};

handle_cast({apply_full_sync, RemoteCands, RemoteFences}, State) ->
    mycelium_ormap:absorb_clock(RemoteCands),
    Cands = mycelium_ormap:merge(State#state.cands, RemoteCands),
    Fences = merge_fences(State#state.fences, RemoteFences),
    {noreply, recompute_local(State#state{cands = Cands, fences = Fences})};

handle_cast({remove_node, Node}, State) ->
    Cands = maps:filter(fun({_Name, N}, _Entry) -> N =/= Node end,
                        State#state.cands),
    {noreply, recompute_local(State#state{cands = Cands})};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    case find_name_by_ref(Ref, State#state.local) of
        {ok, Name} ->
            %% Monitor already cleared by the process exit; just drop.
            {noreply, drop_local(Name, no_demonitor, State)};
        error ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Remove this node's candidacy for Name: tombstone the OR-Map key,
%% drop bookkeeping, broadcast the removal. `Demon' controls whether we
%% demonitor (resign) or leave it (the monitored process already died).
drop_local(Name, Demon, State) ->
    case maps:take(Name, State#state.local) of
        {{_Pid, Ref, _Prio}, Local} ->
            case Demon of
                demonitor    -> demonitor(Ref, [flush]);
                no_demonitor -> ok
            end,
            Key = {Name, node()},
            Cands = mycelium_ormap:remove(Key, State#state.cands),
            mycelium_replica:broadcast_update(?REPLICA, {remove, Key}),
            State#state{
                local    = Local,
                cands    = Cands,
                leaders  = maps:remove(Name, State#state.leaders),
                my_fence = maps:remove(Name, State#state.my_fence)
            };
        error ->
            State
    end.

%% Mint a fencing token for a term that self is starting: advance the
%% local HLC past the replicated high-water, take a fresh timestamp
%% (now strictly greater), raise + gossip the high-water, and record it
%% as our current term token. Returns the packed integer token.
become_leader(Name, State) ->
    HighWater = maps:get(Name, State#state.fences, undefined),
    case HighWater of
        undefined -> ok;
        _         -> mycelium_hlc:update(HighWater)
    end,
    F = mycelium_hlc:now(),
    Fences = raise_fence(State#state.fences, Name, F),
    MyFence = maps:put(Name, F, State#state.my_fence),
    mycelium_replica:broadcast_custom(?REPLICA, {Name, F}),
    {mycelium_hlc:pack(F), State#state{fences = Fences, my_fence = MyFence}}.

%% Recompute leadership for every local campaign and fire transitions.
recompute_local(State) ->
    lists:foldl(fun recompute_name/2, State, maps:keys(State#state.local)).

recompute_name(Name, State) ->
    New = leader_of(Name, State#state.cands),
    Old = maps:get(Name, State#state.leaders, none),
    case New =:= Old of
        true  -> State;
        false -> apply_transition(Name, Old, New, State)
    end.

apply_transition(Name, Old, New, State) ->
    WasSelf = is_self_leader(Old),
    IsSelf  = is_self_leader(New),
    State1 =
        if
            IsSelf andalso not WasSelf ->
                {Fence, S} = become_leader(Name, State),
                notify_local(Name, {elected, Fence}, S),
                S;
            WasSelf andalso not IsSelf ->
                S = State#state{
                    my_fence = maps:remove(Name, State#state.my_fence)},
                notify_local(Name, revoked, S),
                S;
            true ->
                State
        end,
    Leaders = case New of
        none -> maps:remove(Name, State1#state.leaders);
        _    -> maps:put(Name, New, State1#state.leaders)
    end,
    State1#state{leaders = Leaders}.

notify_local(Name, Msg, State) ->
    case maps:get(Name, State#state.local, undefined) of
        {Pid, _Ref, _Prio} -> Pid ! {mycelium_leader, Name, Msg};
        undefined          -> ok
    end.

%% Winner among live candidates for Name: highest priority, ties to the
%% lowest node atom. `none' when there are no candidates.
leader_of(Name, ORMap) ->
    Cands = [{Node, Pid, Prio}
             || {{N, Node}, {Pid, Prio}} <- mycelium_ormap:to_list(ORMap),
                N =:= Name],
    case Cands of
        [] ->
            none;
        _ ->
            [{BNode, BPid, _} | _] = lists:sort(fun cmp_cand/2, Cands),
            {BNode, BPid}
    end.

cmp_cand({Na, _Pa, Pria}, {Nb, _Pb, Prib}) ->
    {-Pria, Na} =< {-Prib, Nb}.

is_self_leader({Node, _Pid}) -> Node =:= node();
is_self_leader(none)         -> false.

%% Keep the HLC-greater token for a name.
raise_fence(Fences, Name, F) ->
    case maps:get(Name, Fences, undefined) of
        undefined ->
            maps:put(Name, F, Fences);
        Old ->
            case mycelium_hlc:compare(F, Old) of
                gt -> maps:put(Name, F, Fences);
                _  -> Fences
            end
    end.

merge_fences(F1, F2) ->
    maps:fold(fun(Name, F, Acc) -> raise_fence(Acc, Name, F) end, F1, F2).

find_name_by_ref(Ref, Local) ->
    case [N || {N, {_P, R, _Pr}} <- maps:to_list(Local), R =:= Ref] of
        [Name | _] -> {ok, Name};
        []         -> error
    end.
