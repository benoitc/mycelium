%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_registry).
-behaviour(gen_server).
-behaviour(mycelium_replica).

-include("mycelium.hrl").
-include_lib("hlc/include/hlc.hrl").

%% Registered name of this feature's replication instance.
-define(REPLICA, mycelium_registry_replica).

%% API
-export([start_link/0]).
-export([register_service/2, register_service/3, unregister_service/1]).
-export([lookup/1, lookup_local/1, list_services/0]).
-export([get_all_local/0, get_local_ormap/0]).
-export([overlay_lookup/1]).
-export([ensure_proxy/2, get_proxy/1]).

%% Internal API (used by sync)
-export([merge_remote/1, remove_node_entries/1, remove_entry/2]).

%% mycelium_replica callbacks
-export([
    replica_merge_delta/2,
    replica_apply_full_sync/2,
    replica_full_sync_snapshot/1,
    replica_remove_node/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    %% Local services: {name, node} -> #service_entry{} (OR-Map)
    local = mycelium_ormap:new() :: mycelium_ormap:ormap(),
    %% Remote services: {name, node} -> #service_entry{} (OR-Map)
    remote = mycelium_ormap:new() :: mycelium_ormap:ormap(),
    %% Monitor refs: ref -> {name, pid}
    monitors = #{} :: #{reference() => {atom() | binary(), pid()}}
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register_service(atom() | binary(), map()) -> ok | {error, term()}.
register_service(Name, Meta) ->
    gen_server:call(?SERVER, {register, Name, Meta}).

%% @doc Register a service with a specific pid (for via callbacks)
-spec register_service(atom() | binary(), pid(), map()) -> ok | {error, term()}.
register_service(Name, Pid, Meta) when is_pid(Pid) ->
    gen_server:call(?SERVER, {register_pid, Name, Pid, Meta}).

-spec unregister_service(atom() | binary()) -> ok.
unregister_service(Name) ->
    gen_server:call(?SERVER, {unregister, Name}).

-spec lookup(atom() | binary()) -> {ok, [#service_entry{}]} | {error, not_found}.
lookup(Name) ->
    gen_server:call(?SERVER, {lookup, Name}).

-spec lookup_local(atom() | binary()) -> {ok, pid()} | {error, not_found}.
lookup_local(Name) ->
    gen_server:call(?SERVER, {lookup_local, Name}).

-spec list_services() -> [atom() | binary()].
list_services() ->
    gen_server:call(?SERVER, list_services).

-spec get_all_local() -> [#service_entry{}].
get_all_local() ->
    gen_server:call(?SERVER, get_all_local).

%% Get the local OR-Map for full sync
-spec get_local_ormap() -> mycelium_ormap:ormap().
get_local_ormap() ->
    gen_server:call(?SERVER, get_local_ormap).

%% Lookup using overlay routing (when not found locally/remotely).
%% Normalises the router's `{found, Node, Pid}' shape to the contract
%% callers expect; any other router return is reported as not_found.
-spec overlay_lookup(atom() | binary()) -> {ok, node(), pid()} | {error, not_found}.
overlay_lookup(Name) ->
    case mycelium_router:find_service(Name) of
        {found, Node, Pid} -> {ok, Node, Pid};
        _ -> {error, not_found}
    end.

%% Ensure a proxy exists for a remote service
-spec ensure_proxy(atom() | binary(), node()) -> {ok, pid()} | {error, term()}.
ensure_proxy(Name, TargetNode) ->
    mycelium_proxy_sup:start_proxy(Name, TargetNode).

%% Get existing proxy for a service
-spec get_proxy(atom() | binary()) -> {ok, pid()} | not_found.
get_proxy(Name) ->
    mycelium_proxy_sup:get_proxy(Name).

%% Internal API

%% Merge remote OR-Map delta
-spec merge_remote(mycelium_ormap:ormap()) -> ok.
merge_remote(DeltaMap) ->
    gen_server:cast(?SERVER, {merge_remote, DeltaMap}).

-spec remove_node_entries(node()) -> ok.
remove_node_entries(Node) ->
    gen_server:cast(?SERVER, {remove_node, Node}).

-spec remove_entry(atom() | binary(), node()) -> ok.
remove_entry(Name, Node) ->
    gen_server:cast(?SERVER, {remove_entry, Name, Node}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({register, Name, Meta}, {Pid, _}, State) ->
    do_register(Name, Pid, Meta, State);
handle_call({register_pid, Name, Pid, Meta}, _From, State) ->
    do_register(Name, Pid, Meta, State);
handle_call({unregister, Name}, _From, State) ->
    Key = {Name, node()},
    case mycelium_ormap:get(Key, State#state.local) of
        {ok, _Entry} ->
            Local = mycelium_ormap:remove(Key, State#state.local),
            %% Find and remove monitor
            MonitorRef = find_monitor_for_name(Name, State#state.monitors),
            Monitors =
                case MonitorRef of
                    undefined ->
                        State#state.monitors;
                    Ref ->
                        demonitor(Ref, [flush]),
                        maps:remove(Ref, State#state.monitors)
                end,
            %% Broadcast removal
            mycelium_replica:broadcast_update(?REPLICA, {remove, Key}),
            %% Emit service event
            mycelium_service_events:notify({service_unregistered, Name, node()}),
            {reply, ok, State#state{local = Local, monitors = Monitors}};
        not_found ->
            {reply, ok, State}
    end;
handle_call({lookup, Name}, _From, State) ->
    %% Collect entries from local and remote OR-Maps
    LocalEntries = collect_entries_by_name(Name, State#state.local),
    RemoteEntries = collect_entries_by_name(Name, State#state.remote),
    case LocalEntries ++ RemoteEntries of
        [] -> {reply, {error, not_found}, State};
        Entries -> {reply, {ok, Entries}, State}
    end;
handle_call({lookup_local, Name}, _From, State) ->
    Key = {Name, node()},
    case mycelium_ormap:get(Key, State#state.local) of
        not_found -> {reply, {error, not_found}, State};
        {ok, #service_entry{pid = Pid}} -> {reply, {ok, Pid}, State}
    end;
handle_call(list_services, _From, State) ->
    LocalNames = [N || {N, _Node} <- mycelium_ormap:keys(State#state.local)],
    RemoteNames = [N || {N, _Node} <- mycelium_ormap:keys(State#state.remote)],
    {reply, lists:usort(LocalNames ++ RemoteNames), State};
handle_call(get_all_local, _From, State) ->
    Entries = [E || {_Key, E} <- mycelium_ormap:to_list(State#state.local)],
    {reply, Entries, State};
handle_call(get_local_ormap, _From, State) ->
    {reply, State#state.local, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({merge_remote, DeltaMap}, State) ->
    %% Validate the wrapper (dots/HLCs) and the service-entry leaf before
    %% absorbing the clock and merging, so a malformed peer delta cannot
    %% crash this gen_server or the shared mycelium_hlc server. Only valid
    %% entries are absorbed/merged; the rest are dropped.
    {Remote, _Accepted} = mycelium_crdt_wire:ingest(
        State#state.remote, DeltaMap, fun valid_service_entry/1
    ),
    {noreply, State#state{remote = Remote}};
handle_cast({remove_node, Node}, State) ->
    %% Remove all entries from the specified node
    Remote = maps:filter(
        fun({_Name, N}, _Entry) ->
            N =/= Node
        end,
        State#state.remote
    ),
    %% Invalidate routes to this node
    mycelium_router:invalidate_route(Node),
    {noreply, State#state{remote = Remote}};
handle_cast({remove_entry, Name, Node}, State) ->
    %% Remove specific entry from remote cache
    Key = {Name, Node},
    Remote = mycelium_ormap:remove(Key, State#state.remote),
    %% Invalidate route cache for this service
    mycelium_router:invalidate_route(Name),
    {noreply, State#state{remote = Remote}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    case maps:take(Ref, State#state.monitors) of
        {{Name, Pid}, Monitors} ->
            Key = {Name, node()},
            Local = mycelium_ormap:remove(Key, State#state.local),
            mycelium_replica:broadcast_update(?REPLICA, {remove, Key}),
            %% Emit service down event
            mycelium_service_events:notify({service_down, Name, node(), Reason}),
            {noreply, State#state{local = Local, monitors = Monitors}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% mycelium_replica callbacks
%%====================================================================

%% Merge a peer's delta into the remote map, then surface the change
%% to service-event subscribers. A broadcast delta is single-key, so
%% the key's node is the originating node.
replica_merge_delta(_Name, Delta) ->
    merge_remote(Delta),
    %% Emit events only for entries that pass validation (the same set the
    %% merge accepts); iterating the raw Delta would crash on a malformed
    %% entry that does not match {value,_,_} | {tombstone,_}.
    Accepted = mycelium_crdt_wire:accept(Delta, fun valid_service_entry/1),
    maps:foreach(
        fun
            ({Name, Node}, {value, _Entry, _Dots}) ->
                mycelium_service_events:notify({service_registered, Name, Node});
            ({Name, Node}, {tombstone, _HLC}) ->
                mycelium_router:invalidate_route(Name),
                mycelium_service_events:notify({service_unregistered, Name, Node})
        end,
        Accepted
    ),
    ok.

%% A full sync carries a peer's local map; merge it silently (no
%% per-entry events, matching the prior behaviour).
replica_apply_full_sync(_Name, RemoteORMap) ->
    merge_remote(RemoteORMap),
    ok.

replica_full_sync_snapshot(_Name) ->
    ORMap = get_local_ormap(),
    case mycelium_ormap:is_empty(ORMap) of
        true -> empty;
        false -> {sync, ORMap}
    end.

replica_remove_node(_Name, Node) ->
    remove_node_entries(Node),
    mycelium_router:invalidate_route(Node),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Leaf validator for gossiped service entries: a well-formed
%% #service_entry{}. Used by mycelium_crdt_wire to reject malformed peer
%% values before they reach the OR-Map merge.
valid_service_entry(#service_entry{name = N, pid = P, node = Nd, meta = M}) when
    (is_atom(N) orelse is_binary(N)) andalso
        is_pid(P) andalso is_atom(Nd) andalso is_map(M)
->
    true;
valid_service_entry(_) ->
    false.

%% Common registration logic for both register and register_pid
do_register(Name, Pid, Meta, State) ->
    Key = {Name, node()},
    case mycelium_ormap:get(Key, State#state.local) of
        {ok, _} ->
            {reply, {error, already_registered}, State};
        not_found ->
            Entry = #service_entry{name = Name, pid = Pid, node = node(), meta = Meta},
            Local = mycelium_ormap:add(Key, Entry, State#state.local),
            Ref = monitor(process, Pid),
            Monitors = maps:put(Ref, {Name, Pid}, State#state.monitors),
            %% Broadcast delta via sync
            mycelium_replica:broadcast_update(?REPLICA, {add, Key, Entry}),
            %% Emit service event
            mycelium_service_events:notify({service_registered, Name, node()}),
            {reply, ok, State#state{local = Local, monitors = Monitors}}
    end.

find_monitor_for_name(Name, Monitors) ->
    case [Ref || {Ref, {N, _Pid}} <- maps:to_list(Monitors), N =:= Name] of
        [Ref | _] -> Ref;
        [] -> undefined
    end.

%% Collect all entries matching a service name from an OR-Map
collect_entries_by_name(Name, ORMap) ->
    [Entry || {{N, _Node}, Entry} <- mycelium_ormap:to_list(ORMap), N =:= Name].
