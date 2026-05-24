%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Durable reminders: cluster-wide, replicated, fire-at-most-once
%%% timers. `erlang:send_after/3' dies with the node; a reminder set
%%% here survives the node that armed it, because the reminder store is
%%% replicated and the firing node is chosen by placement.
%%%
%%% A reminder is `Key => {FireAtMs, Payload, H}' in a `mycelium_replica'
%%% OR-Map (instance `mycelium_reminder_replica'). `H' is a version HLC
%%% generated once when the reminder is set; it is identical on every
%%% node and names this exact reminder version (the OR-Map dot HLCs are
%%% separate and only resolve merges).
%%%
%%% The OWNER of a reminder is `mycelium_shard:place(Key)'. Only the
%%% owner arms a local `erlang:send_after' and fires. Timers are
%%% VERSIONED HINTS: when `{fire, Key, H}' arrives we re-read the live
%%% entry and fire only if it still exists, still names version `H', this
%%% node is still the owner, and the fire time has been reached. That
%%% makes a stale timer harmless after a re-`remind' (new `H'), a
%%% `cancel_reminder/1', an ownership change, or a cancel that raced an
%%% already-queued timeout.
%%%
%%% Firing is TOMBSTONE-FIRST: write + gossip the tombstone, then deliver
%%% `{mycelium_reminder, Key, Payload, Fence}' (Fence = pack(H)) to LOCAL
%%% subscribers. Committing "fired" before delivering biases toward not
%%% double-firing.
%%%
%%% Guarantee: exactly once in steady state (member set converged);
%%% best-effort under churn (two nodes can briefly both own a key and
%%% both fire before the tombstone propagates) or a crash at the fire
%%% instant (a crash between tombstone and delivery drops the fire; a
%%% crash before the tombstone gossips can let a survivor fire again).
%%% The delivered Fence lets a handler dedup if it wants at-least-once
%%% with idempotency. Fire time is wall-clock ("cluster-time"), subject
%%% to the same skew caveat as membership.
-module(mycelium_reminder).
-behaviour(gen_server).
-behaviour(mycelium_replica).

-include_lib("hlc/include/hlc.hrl").

%% Registered name of this feature's replication instance.
-define(REPLICA, mycelium_reminder_replica).

%% Public API
-export([remind/3, remind_after/3, cancel_reminder/1, subscribe/1,
         unsubscribe/1]).

%% Internal API
-export([start_link/0]).

%% mycelium_replica callbacks
-export([replica_merge_delta/2, replica_apply_full_sync/2,
         replica_full_sync_snapshot/1, replica_remove_node/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_SCAN_MS, 1000).
-define(DEFAULT_TOMBSTONE_TTL_MS, (60 * 60 * 1000)).
%% Cap on a single send_after delay. A far-future reminder arms for at
%% most this long; the periodic scan re-arms it as the fire time nears.
%% Stays well under erlang's ~49-day send_after ceiling.
-define(MAX_TIMER_MS, (4 * 24 * 60 * 60 * 1000)).

-type key()     :: term().
-type payload() :: term().
-type fence()   :: non_neg_integer().

-export_type([fence/0]).

-record(state, {
    scan_ms          :: pos_integer(),
    tombstone_ttl_ms :: non_neg_integer(),
    reminders   = mycelium_ormap:new() :: mycelium_ormap:ormap(),
    %% Locally armed timers: Key -> {TimerRef, VersionHLC}
    timers      = #{} :: #{key() => {reference(), mycelium_hlc:timestamp()}},
    subscribers = #{} :: #{pid() => reference()},
    watch       = #{} :: mycelium_source_monitor:watch()
}).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Set a reminder for `Key' to fire at absolute wall-clock
%% `FireAtMs' (milliseconds, `erlang:system_time(millisecond)' scale),
%% delivering `Payload' on whichever node owns `Key' at fire time.
%% Re-setting an existing `Key' replaces it. Stability: beta.
-spec remind(key(), integer(), payload()) -> ok.
remind(Key, FireAtMs, Payload) ->
    gen_server:call(?SERVER, {remind, Key, FireAtMs, Payload}).

%% @doc Like `remind/3' but `DelayMs' from now; converted to an absolute
%% target so every node agrees on the fire time. Stability: beta.
-spec remind_after(key(), non_neg_integer(), payload()) -> ok.
remind_after(Key, DelayMs, Payload) ->
    gen_server:call(?SERVER, {remind_after, Key, DelayMs, Payload}).

%% @doc Cancel a pending reminder cluster-wide. Stability: beta.
-spec cancel_reminder(key()) -> ok.
cancel_reminder(Key) ->
    gen_server:call(?SERVER, {cancel, Key}).

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
    gen_server:cast(?SERVER, {merge_delta, Delta}).

replica_apply_full_sync(_Name, Snapshot) ->
    gen_server:cast(?SERVER, {apply_full_sync, Snapshot}).

replica_full_sync_snapshot(_Name) ->
    gen_server:call(?SERVER, snapshot).

%% A reminder must survive the node that armed it, so a peer leaving the
%% active view never drops reminders (re-ownership handles the rest).
replica_remove_node(_Name, _Node) ->
    ok.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    Scan = cfg(reminder_scan_ms, ?DEFAULT_SCAN_MS),
    TombTtl = cfg(reminder_tombstone_ttl_ms, ?DEFAULT_TOMBSTONE_TTL_MS),
    %% Ownership transitions tell us when to take over (acquired) or hand
    %% off (released) a partition's reminders. Keep the subscription alive
    %% across a shard restart.
    Watch = mycelium_source_monitor:start([mycelium_shard]),
    arm_scan(Scan),
    {ok, #state{scan_ms = Scan, tombstone_ttl_ms = TombTtl, watch = Watch}}.

handle_call({remind, Key, FireAtMs, Payload}, _From, State) ->
    {reply, ok, do_remind(Key, FireAtMs, Payload, State)};

handle_call({remind_after, Key, DelayMs, Payload}, _From, State) ->
    {reply, ok, do_remind(Key, now_ms() + DelayMs, Payload, State)};

handle_call({cancel, Key}, _From, State) ->
    Reminders = mycelium_ormap:remove(Key, State#state.reminders),
    mycelium_replica:broadcast_update(?REPLICA, {remove, Key}),
    {reply, ok, disarm(Key, State#state{reminders = Reminders})};

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
    Reply = case mycelium_ormap:is_empty(State#state.reminders) of
        true  -> empty;
        false -> {sync, State#state.reminders}
    end,
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({merge_delta, Delta}, State) ->
    {noreply, ingest(Delta, State)};

handle_cast({apply_full_sync, Snapshot}, State) ->
    {noreply, ingest(Snapshot, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Versioned-hint timeout. Re-validate everything before firing.
handle_info({fire, Key, H}, State) ->
    State1 = clear_spent_timer(Key, H, State),
    case should_fire(Key, H, State1) of
        {true, Payload} -> {noreply, fire(Key, Payload, H, State1)};
        false           -> {noreply, State1}
    end;

%% We became the owner of partition P: arm its reminders (this is how a
%% survivor picks up a dead owner's un-fired reminders).
handle_info({mycelium_shard, {acquired, P}}, State) ->
    {noreply, reconcile_keys(keys_in_partition(P, State), State)};

%% We lost partition P: disarm its reminders.
handle_info({mycelium_shard, {released, P}}, State) ->
    {noreply, reconcile_keys(keys_in_partition(P, State), State)};

%% Safety sweep: re-arm anything we own and missed, disarm anything we no
%% longer own, re-arm far-future reminders as their fire time nears, and GC
%% old fire/cancel tombstones so the replicated store stays bounded.
handle_info(scan, State) ->
    State1 = reconcile_keys(mycelium_ormap:keys(State#state.reminders), State),
    Cutoff = now_ms() - State1#state.tombstone_ttl_ms,
    Reminders = mycelium_ormap:gc_tombstones(State1#state.reminders, Cutoff),
    arm_scan(State1#state.scan_ms),
    {noreply, State1#state{reminders = Reminders}};

%% Re-subscribe if a watched source (the shard) restarted.
handle_info({mycelium_source_monitor, retry, Source}, State) ->
    {noreply, State#state{
        watch = mycelium_source_monitor:retry(Source, State#state.watch)}};

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

%% Merge a received delta/snapshot, rejecting malformed entries FIRST via
%% mycelium_crdt_wire (it validates the whole wrapper - dots and HLCs -
%% before absorb_clock/merge, which would otherwise crash this gen_server
%% or the shared mycelium_hlc server). We add the reminder-specific leaf
%% check on top: the payload must be our {FireAt, _, VersionHLC} shape.
ingest(Map, State) ->
    {Reminders, Accepted} =
        mycelium_crdt_wire:ingest(State#state.reminders, Map, fun valid_leaf/1),
    reconcile_keys(maps:keys(Accepted), State#state{reminders = Reminders}).

%% A well-formed reminder leaf payload: {FireAt, Payload, VersionHLC}.
valid_leaf({FireAt, _Payload, #timestamp{}}) when is_integer(FireAt) -> true;
valid_leaf(_) -> false.

%% Insert locally first (broadcast does not mutate owner state), gossip
%% the add, then arm if we own the key.
-spec do_remind(key(), integer(), payload(), #state{}) -> #state{}.
do_remind(Key, FireAtMs, Payload, State) ->
    H = mycelium_hlc:now(),
    Val = {FireAtMs, Payload, H},
    Reminders = mycelium_ormap:add(Key, Val, State#state.reminders),
    mycelium_replica:broadcast_update(?REPLICA, {add, Key, Val}),
    reconcile_key(Key, State#state{reminders = Reminders}).

%% Bring local timer state in line with the live entry and ownership:
%% own + live  -> armed for the entry's version,
%% live, not owned, or tombstoned -> disarmed.
reconcile_keys(Keys, State) ->
    lists:foldl(fun reconcile_key/2, State, Keys).

reconcile_key(Key, State) ->
    case mycelium_ormap:get(Key, State#state.reminders) of
        {ok, {FireAt, _Payload, #timestamp{} = H}} when is_integer(FireAt) ->
            case mycelium_shard:place(Key) =:= node() of
                true  -> arm(Key, FireAt, H, State);
                false -> disarm(Key, State)
            end;
        %% Not a live, well-formed reminder (tombstoned, or a malformed
        %% value that slipped in): make sure nothing is armed for it.
        _ ->
            disarm(Key, State)
    end.

%% Arm is idempotent on version: an existing timer for the same H is left
%% in place; a different (or no) timer is (re)armed.
arm(Key, FireAt, H, State) ->
    case maps:get(Key, State#state.timers, undefined) of
        {_Ref, H} ->
            State;
        {OldRef, _OtherH} ->
            erlang:cancel_timer(OldRef),
            do_arm(Key, FireAt, H, State);
        undefined ->
            do_arm(Key, FireAt, H, State)
    end.

do_arm(Key, FireAt, H, State) ->
    Delay = min(max(0, FireAt - now_ms()), ?MAX_TIMER_MS),
    Ref = erlang:send_after(Delay, self(), {fire, Key, H}),
    State#state{timers = maps:put(Key, {Ref, H}, State#state.timers)}.

disarm(Key, State) ->
    case maps:take(Key, State#state.timers) of
        {{Ref, _H}, Timers} ->
            erlang:cancel_timer(Ref),
            State#state{timers = Timers};
        error ->
            State
    end.

%% Drop the timer entry for a fired hint, but only if it is the timer we
%% armed for this version (a re-remind may have replaced it).
clear_spent_timer(Key, H, State) ->
    case maps:get(Key, State#state.timers, undefined) of
        {_Ref, H} -> State#state{timers = maps:remove(Key, State#state.timers)};
        _         -> State
    end.

%% Fire only if the entry still exists at the same version, we still own
%% it, and the fire time has been reached.
-spec should_fire(key(), mycelium_hlc:timestamp(), #state{}) ->
    {true, payload()} | false.
should_fire(Key, H, State) ->
    case mycelium_ormap:get(Key, State#state.reminders) of
        {ok, {FireAt, Payload, H}} ->
            case mycelium_shard:place(Key) =:= node()
                 andalso now_ms() >= FireAt of
                true  -> {true, Payload};
                false -> false
            end;
        _ ->
            false
    end.

%% Tombstone-first: commit + gossip the removal, then deliver locally.
fire(Key, Payload, H, State) ->
    Reminders = mycelium_ormap:remove(Key, State#state.reminders),
    mycelium_replica:broadcast_update(?REPLICA, {remove, Key}),
    notify(State#state.subscribers, Key, Payload, mycelium_hlc:pack(H)),
    State#state{reminders = Reminders}.

notify(Subs, Key, Payload, Fence) ->
    maps:foreach(
        fun(Pid, _Ref) -> Pid ! {mycelium_reminder, Key, Payload, Fence} end,
        Subs).

keys_in_partition(P, State) ->
    [K || K <- mycelium_ormap:keys(State#state.reminders),
          mycelium_shard:partition(K) =:= P].

arm_scan(Scan) ->
    erlang:send_after(Scan, self(), scan).

now_ms() ->
    erlang:system_time(millisecond).

cfg(Key, Default) ->
    application:get_env(mycelium, Key, Default).
