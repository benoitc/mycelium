%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Public replicated key-value map.
%%%
%%% A named, gossiped, last-write-wins map for small cluster-wide
%%% control-plane state (config, feature flags, routing/placement tables,
%%% presence). Built on the `mycelium_replica' substrate: writes broadcast
%%% OR-Map deltas, peers converge eventually, and a freshly started map
%%% pulls existing state from peers.
%%%
%%% Each named map is one owner gen_server (the sole writer; holds the
%%% OR-Map and a lock-free ETS read cache) plus its own `mycelium_replica'
%%% instance. Reads (`get'/`keys'/`to_list') hit the ETS table directly and
%%% never block on writes.
%%%
%%% A map is NODE-LOCAL: `new/2' starts it on the calling node only. To be
%%% cluster-wide it must run on every participating node - declare it in the
%%% `replicated_maps' app env (started on every node at boot) or call
%%% `new/2' on each node. Not for bulk data, high write rates, or data
%%% needing custom conflict resolution (use the `mycelium_replica' behaviour
%%% directly for that).
%%%
%%% State is in-memory + gossip by default (a full-cluster restart loses it).
%%% Pass `persist => true' to back the map with a `mycelium_replica_log' WAL +
%%% snapshot (under `mycelium_map_data_dir', default `data/maps'): writes are
%%% fsynced and the map is recovered on boot, so it is durable across a
%%% full-cluster restart. As with reminders, persisted values must be
%%% restart-safe data (no pids/ports/refs/funs).

-module(mycelium_map).
-behaviour(gen_server).
-behaviour(mycelium_replica).

%% Public API
-export([
    new/1, new/2,
    delete_map/1,
    put/3,
    remove/2,
    delete/2,
    get/2,
    keys/1,
    to_list/1,
    subscribe/1, subscribe/2,
    unsubscribe/1, unsubscribe/2
]).

%% Name helpers (used by the instance supervisor).
-export([owner_name/1, replica_name/1, tab_name/1]).

%% Owner process start.
-export([start_link/2]).

%% mycelium_replica callbacks.
-export([
    replica_merge_delta/2,
    replica_apply_full_sync/2,
    replica_full_sync_snapshot/1,
    replica_remove_node/2,
    replica_anti_entropy/0
]).

%% gen_server callbacks.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(OWNER_PREFIX, "mycelium_map$").
-define(REPLICA_PREFIX, "mycelium_map_replica$").
-define(TAB_PREFIX, "mycelium_map_tab$").
-define(STORE_PREFIX, "mycelium_map_store$").
-define(DEFAULT_MAP_DIR, "data/maps").

-type opts() :: #{
    validator => fun((term()) -> boolean()) | {module(), atom()},
    tombstone_ttl_ms => non_neg_integer(),
    scan_ms => pos_integer(),
    prune_on_peer_down => boolean(),
    persist => boolean()
}.
-export_type([opts/0]).

-record(state, {
    name :: atom(),
    replica :: atom(),
    tab :: ets:tid(),
    map :: mycelium_ormap:ormap(),
    validator :: fun((term()) -> boolean()),
    scan_ms :: pos_integer(),
    tombstone_ttl_ms :: non_neg_integer(),
    prune :: boolean(),
    subscribers = #{} :: #{pid() => reference()},
    %% Optional disk persistence (opt-in via `persist'): WAL+snapshot handle
    %% and a dirty flag so the scan only snapshots on churn.
    log = undefined :: mycelium_replica_log:handle(),
    dirty = false :: boolean()
}).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Start a replicated map named `Name' on this node. Idempotent.
-spec new(atom()) -> {ok, pid()} | {error, term()}.
new(Name) -> new(Name, #{}).

-spec new(atom(), opts()) -> {ok, pid()} | {error, term()}.
new(Name, Opts) when is_atom(Name), is_map(Opts) ->
    mycelium_map_sup:start_map(Name, Opts);
new(_Name, _Opts) ->
    {error, invalid_map_name}.

%% @doc Stop the map on THIS node (node-local; not a cluster-wide erase).
%% Also removes any persisted files, so a later `new/2' starts fresh rather
%% than reloading stale data. `stop_map/1' is synchronous (it waits for the
%% owner to terminate, which closes the log), so the delete runs strictly
%% AFTER the log is closed - no `terminate/2' write can race the delete. The
%% file delete is a no-op for a map that did not persist.
-spec delete_map(atom()) -> ok.
delete_map(Name) when is_atom(Name) ->
    ok = mycelium_map_sup:stop_map(Name),
    mycelium_replica_log:delete(store_name(Name), cfg(mycelium_map_data_dir, ?DEFAULT_MAP_DIR)).

%% @doc Put `Value' under `Key'. Rejected with `{error, invalid_value}' if
%% the map's validator rejects the value.
-spec put(atom(), term(), term()) -> ok | {error, invalid_value | no_such_map}.
put(Name, Key, Value) ->
    call(Name, {put, Key, Value}).

%% @doc Remove `Key'.
-spec remove(atom(), term()) -> ok | {error, no_such_map}.
remove(Name, Key) ->
    call(Name, {remove, Key}).

%% @doc Alias for `remove/2'.
-spec delete(atom(), term()) -> ok | {error, no_such_map}.
delete(Name, Key) -> remove(Name, Key).

%% @doc Look up a live value (lock-free ETS read).
-spec get(atom(), term()) -> {ok, term()} | not_found.
get(Name, Key) ->
    case tab(Name) of
        undefined ->
            not_found;
        Tab ->
            case ets:lookup(Tab, Key) of
                [{_, Value}] -> {ok, Value};
                [] -> not_found
            end
    end.

%% @doc Live keys (lock-free ETS read).
-spec keys(atom()) -> [term()].
keys(Name) ->
    case tab(Name) of
        undefined -> [];
        Tab -> [K || {K, _V} <- ets:tab2list(Tab)]
    end.

%% @doc Live key/value pairs (lock-free ETS read).
-spec to_list(atom()) -> [{term(), term()}].
to_list(Name) ->
    case tab(Name) of
        undefined -> [];
        Tab -> ets:tab2list(Tab)
    end.

%% @doc Subscribe the calling process to `{mycelium_map, Name, Event}'
%% change events, where Event is `{put, Key, Value}' or `{remove, Key}'.
-spec subscribe(atom()) -> ok | {error, no_such_map}.
subscribe(Name) -> subscribe(Name, self()).

-spec subscribe(atom(), pid()) -> ok | {error, no_such_map}.
subscribe(Name, Pid) when is_pid(Pid) ->
    call(Name, {subscribe, Pid}).

-spec unsubscribe(atom()) -> ok | {error, no_such_map}.
unsubscribe(Name) -> unsubscribe(Name, self()).

-spec unsubscribe(atom(), pid()) -> ok | {error, no_such_map}.
unsubscribe(Name, Pid) when is_pid(Pid) ->
    call(Name, {unsubscribe, Pid}).

%%====================================================================
%% Name helpers
%%====================================================================

owner_name(Name) -> derived(?OWNER_PREFIX, Name).
replica_name(Name) -> derived(?REPLICA_PREFIX, Name).
tab_name(Name) -> derived(?TAB_PREFIX, Name).
store_name(Name) -> derived(?STORE_PREFIX, Name).

derived(Prefix, Name) ->
    list_to_atom(Prefix ++ atom_to_list(Name)).

%% Owner registered name recovered from the replica instance name (the
%% callbacks run in the replica process and only receive that name). The
%% owner is always registered by the time a callback fires (the instance
%% supervisor starts owner-then-replica), so the atom exists.
owner_of(ReplicaName) ->
    ?REPLICA_PREFIX ++ Suffix = atom_to_list(ReplicaName),
    list_to_existing_atom(?OWNER_PREFIX ++ Suffix).

%%====================================================================
%% mycelium_replica callbacks (run in the replica process)
%%====================================================================

replica_merge_delta(ReplicaName, Delta) ->
    gen_server:cast(owner_of(ReplicaName), {merge, Delta}).

replica_apply_full_sync(ReplicaName, Snapshot) ->
    gen_server:cast(owner_of(ReplicaName), {merge, Snapshot}).

replica_full_sync_snapshot(ReplicaName) ->
    gen_server:call(owner_of(ReplicaName), snapshot).

replica_remove_node(ReplicaName, Node) ->
    gen_server:cast(owner_of(ReplicaName), {remove_node, Node}).

%% A replicated map carries values that must reconverge after a heal even
%% without a fresh peer_up, so anti-entropy is intrinsic (no opt-out).
replica_anti_entropy() ->
    true.

%%====================================================================
%% Owner gen_server
%%====================================================================

start_link(Name, Opts) ->
    gen_server:start_link({local, owner_name(Name)}, ?MODULE, {Name, Opts}, []).

init({Name, Opts}) ->
    %% Trap exits so terminate/2 runs on supervisor shutdown and closes the
    %% disk_log cleanly (we link to it via open).
    process_flag(trap_exit, true),
    Tab = ets:new(
        tab_name(Name),
        [named_table, protected, set, {read_concurrency, true}]
    ),
    Scan = opt(scan_ms, Opts, cfg(mycelium_map_scan_ms, 1000)),
    Ttl = opt(
        tombstone_ttl_ms,
        Opts,
        cfg(mycelium_map_tombstone_ttl_ms, 3600000)
    ),
    %% Optional disk recovery. Seeds both the OR-Map and the ETS read cache
    %% from the persisted state; absorb_clock keeps the HLC monotonic.
    {Log, Map0} = open_store(Name, maps:get(persist, Opts, false)),
    seed_ets(Tab, Map0),
    arm_scan(Scan),
    {ok, #state{
        name = Name,
        replica = replica_name(Name),
        tab = Tab,
        map = Map0,
        validator = normalise_validator(maps:get(validator, Opts, undefined)),
        scan_ms = Scan,
        tombstone_ttl_ms = Ttl,
        prune = maps:get(prune_on_peer_down, Opts, false),
        log = Log
    }}.

%% Open the persistent store when `persist' is set; otherwise run with no
%% log handle (every log call is a no-op). A failed open degrades to
%% in-memory only, never fatal.
open_store(_Name, false) ->
    {undefined, mycelium_ormap:new()};
open_store(Name, true) ->
    Dir = cfg(mycelium_map_data_dir, ?DEFAULT_MAP_DIR),
    case mycelium_replica_log:open(store_name(Name), Dir) of
        {ok, Log, Map} ->
            ok = mycelium_ormap:absorb_clock(Map),
            {Log, Map};
        {error, Reason} ->
            logger:warning(
                "mycelium_map ~p: persistence disabled, open "
                "failed: ~p",
                [Name, Reason]
            ),
            {undefined, mycelium_ormap:new()}
    end.

seed_ets(Tab, Map) ->
    lists:foreach(
        fun({K, V}) -> ets:insert(Tab, {K, V}) end,
        mycelium_ormap:to_list(Map)
    ).

handle_call({put, Key, Value}, _From, State) ->
    case run_validator(State#state.validator, Value) of
        true ->
            Map = mycelium_ormap:add(Key, Value, State#state.map),
            ets:insert(State#state.tab, {Key, Value}),
            notify(State, {put, Key, Value}),
            mycelium_replica:broadcast_update(State#state.replica, {add, Key, Value}),
            {reply, ok, persist_key(Key, sync, State#state{map = Map})};
        false ->
            {reply, {error, invalid_value}, State}
    end;
handle_call({remove, Key}, _From, State) ->
    Map = mycelium_ormap:remove(Key, State#state.map),
    ets:delete(State#state.tab, Key),
    notify(State, {remove, Key}),
    mycelium_replica:broadcast_update(State#state.replica, {remove, Key}),
    {reply, ok, persist_key(Key, sync, State#state{map = Map})};
handle_call({subscribe, Pid}, _From, State) ->
    case maps:is_key(Pid, State#state.subscribers) of
        true ->
            {reply, ok, State};
        false ->
            Ref = monitor(process, Pid),
            {reply, ok, State#state{
                subscribers = maps:put(Pid, Ref, State#state.subscribers)
            }}
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
    Reply =
        case mycelium_ormap:is_empty(State#state.map) of
            true -> empty;
            false -> {sync, State#state.map}
        end,
    {reply, Reply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% Both gossip deltas and full-sync snapshots flow here. Validate the
%% wrapper, merge, then project exactly the accepted keys into ETS and
%% emit diff events.
handle_cast({merge, Incoming}, State) ->
    {Map, Accepted} =
        mycelium_crdt_wire:ingest(State#state.map, Incoming, State#state.validator),
    State1 = persist_keys(maps:keys(Accepted), nosync, State#state{map = Map}),
    {noreply, project_keys(maps:keys(Accepted), State1)};
handle_cast({remove_node, Node}, State = #state{prune = true}) ->
    Drop = [
        K
     || K <- mycelium_ormap:keys(State#state.map),
        owned_only_by(K, Node, State#state.map)
    ],
    {noreply, lists:foldl(fun drop_key/2, State, Drop)};
handle_cast({remove_node, _Node}, State) ->
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Periodic tombstone GC so the replicated store stays bounded. Snapshots
%% to disk (and truncates the WAL) when the store changed since the last
%% snapshot, including a GC that dropped tombstones.
handle_info(scan, State) ->
    Cutoff = now_ms() - State#state.tombstone_ttl_ms,
    Old = State#state.map,
    Map = mycelium_ormap:gc_tombstones(Old, Cutoff),
    Dirty = State#state.dirty orelse map_size(Map) =/= map_size(Old),
    State1 = State#state{map = Map, dirty = false},
    case Dirty of
        true -> _ = mycelium_replica_log:snapshot(State1#state.log, Map);
        false -> ok
    end,
    arm_scan(State1#state.scan_ms),
    {noreply, State1};
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:get(Pid, State#state.subscribers, undefined) of
        Ref -> {noreply, State#state{subscribers = maps:remove(Pid, State#state.subscribers)}};
        _ -> {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = mycelium_replica_log:close(State#state.log),
    ok.

%%====================================================================
%% Internal
%%====================================================================

%% Recompute the ETS projection + emit a diff event for each changed key.
project_keys(Keys, State) ->
    lists:foldl(fun(K, S) -> project_key(K, S) end, State, Keys).

project_key(Key, State) ->
    case mycelium_ormap:get(Key, State#state.map) of
        {ok, Value} ->
            case ets:lookup(State#state.tab, Key) of
                %% unchanged
                [{_, Value}] ->
                    State;
                _ ->
                    ets:insert(State#state.tab, {Key, Value}),
                    notify(State, {put, Key, Value}),
                    State
            end;
        not_found ->
            case ets:lookup(State#state.tab, Key) of
                [] ->
                    State;
                _ ->
                    ets:delete(State#state.tab, Key),
                    notify(State, {remove, Key}),
                    State
            end
    end.

%% Local-only prune of a departed node's entries (prune_on_peer_down). Mark
%% dirty so the next scan snapshots the prune to disk (no WAL entry: the key
%% is hard-removed, not tombstoned).
drop_key(Key, State) ->
    Map = maps:remove(Key, State#state.map),
    ets:delete(State#state.tab, Key),
    notify(State, {remove, Key}),
    State#state{map = Map, dirty = true}.

owned_only_by(Key, Node, Map) ->
    case mycelium_ormap:get_entry(Key, Map) of
        {ok, {value, _V, Dots}} ->
            lists:all(fun({N, _HLC}) -> N =:= Node end, maps:keys(Dots));
        _ ->
            false
    end.

notify(#state{name = Name, subscribers = Subs}, Event) ->
    maps:foreach(fun(Pid, _Ref) -> Pid ! {mycelium_map, Name, Event} end, Subs).

%% Append the current entry (value or tombstone) for the given key(s) to the
%% WAL, marking the store dirty. `sync' forces it to disk before returning
%% (user-facing writes); `nosync' defers durability to the scan snapshot
%% (gossip merges). A no-op when persistence is off (log = undefined).
persist_key(Key, Sync, State) ->
    persist_keys([Key], Sync, State).

persist_keys(Keys, Sync, #state{log = Log, map = Map} = State) ->
    Delta = entries_of(Keys, Map),
    ok = mycelium_replica_log:append(Log, Delta),
    case Sync of
        sync -> ok = mycelium_replica_log:sync(Log);
        nosync -> ok
    end,
    State#state{dirty = State#state.dirty orelse map_size(Delta) > 0}.

entries_of(Keys, Map) ->
    lists:foldl(
        fun(Key, Acc) ->
            case mycelium_ormap:get_entry(Key, Map) of
                {ok, Entry} -> Acc#{Key => Entry};
                not_found -> Acc
            end
        end,
        #{},
        Keys
    ).

call(Name, Msg) ->
    try
        gen_server:call(owner_name(Name), Msg)
    catch
        exit:{noproc, _} -> {error, no_such_map};
        exit:{{nodedown, _}, _} -> {error, no_such_map}
    end.

tab(Name) ->
    ets:whereis(tab_name(Name)).

normalise_validator(undefined) -> fun(_) -> true end;
normalise_validator(Fun) when is_function(Fun, 1) -> Fun;
normalise_validator({M, F}) -> fun(V) -> M:F(V) end.

run_validator(Fun, Value) ->
    try
        Fun(Value) =:= true
    catch
        _:_ -> false
    end.

arm_scan(Scan) -> erlang:send_after(Scan, self(), scan).

now_ms() -> erlang:system_time(millisecond).

cfg(Key, Default) -> application:get_env(mycelium, Key, Default).

opt(Key, Opts, Default) -> maps:get(Key, Opts, Default).
