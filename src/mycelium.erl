%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium).

-include("mycelium.hrl").

%% Retry configuration
-define(DEFAULT_RETRIES, 3).
-define(BASE_BACKOFF_MS, 100).
-define(MAX_BACKOFF_MS, 2000).

%% Public API
-export([
    join/1,
    leave/0,
    active_view/0,
    passive_view/0,
    subscribe/0,
    subscribe/1,
    unsubscribe/1
]).

%% Service Registry API
-export([
    register_service/1,
    register_service/2,
    register_service/3,
    unregister_service/1,
    lookup/1,
    lookup_local/1,
    list_services/0,
    whereis_service/1,
    whereis_service/2,
    global_register/1,
    get_proxy/1
]).

%% Service Events API
-export([
    subscribe_services/0,
    subscribe_services/1,
    unsubscribe_services/1
]).

%% Leader Election / Singletons API
-export([
    lead/1,
    lead/2,
    resign/1,
    leader/1,
    is_leader/1,
    fence/1
]).

%% Sharded Placement API
-export([
    place/1,
    owners/2,
    is_owner/1,
    partition/1,
    members/0,
    subscribe_shard/0,
    subscribe_shard/1
]).

%% Durable Reminders API
-export([
    remind/3,
    remind_after/3,
    cancel_reminder/1,
    subscribe_reminders/0,
    subscribe_reminders/1,
    unsubscribe_reminders/1
]).

%% Via callbacks for {via, mycelium, Name} registration
-export([
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).

%% Connection migration (RFC 9000 §9 path migration)
-export([
    migrate_peer/1,
    migrate_peer/2
]).

%% Replicated maps (mycelium_map)
-export([
    new_map/1, new_map/2,
    delete_map/1,
    map_put/3,
    map_remove/2,
    map_get/2,
    map_keys/1,
    map_to_list/1,
    subscribe_map/1, subscribe_map/2,
    unsubscribe_map/1, unsubscribe_map/2
]).

%% Test helpers (for integration tests)
-export([
    start_service_holder/1,
    stop_service_holder/1
]).

%%====================================================================
%% HyParView API
%%
%% Stability tiers per `docs/features.md'. Each export below carries a
%% `Stability:' line so grep, ex_doc and reviewers can spot the
%% contract level at a glance.
%%====================================================================

%% Stability: supported.
-spec join(node()) -> ok | {error, term()}.
join(ContactNode) ->
    mycelium_hyparview:join(ContactNode).

%% Stability: supported.
-spec leave() -> ok.
leave() ->
    mycelium_hyparview:leave().

%% Stability: supported.
-spec active_view() -> [node()].
active_view() ->
    mycelium_hyparview:active_view().

%% Stability: supported.
-spec passive_view() -> [node()].
passive_view() ->
    mycelium_hyparview:passive_view().

%% Stability: supported.
-spec subscribe() -> ok.
subscribe() ->
    mycelium_hyparview_events:subscribe(self()).

%% Stability: supported.
-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
    mycelium_hyparview_events:subscribe(Pid).

%% Stability: supported.
-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
    mycelium_hyparview_events:unsubscribe(Pid).

%%====================================================================
%% Service Registry API
%%====================================================================

%% Stability: supported.
-spec register_service(atom() | binary()) -> ok | {error, term()}.
register_service(Name) ->
    register_service(Name, #{}).

%% Stability: supported.
-spec register_service(atom() | binary(), map()) -> ok | {error, term()}.
register_service(Name, Meta) ->
    mycelium_registry:register_service(Name, Meta).

%% Register a specific pid (not the caller) under Name. The registry
%% monitors Pid and unregisters the service when it exits.
%% Stability: supported.
-spec register_service(atom() | binary(), pid(), map()) -> ok | {error, term()}.
register_service(Name, Pid, Meta) when is_pid(Pid) ->
    mycelium_registry:register_service(Name, Pid, Meta).

%% Stability: supported.
-spec unregister_service(atom() | binary()) -> ok.
unregister_service(Name) ->
    mycelium_registry:unregister_service(Name).

%% Stability: supported.
-spec lookup(atom() | binary()) -> {ok, [tuple()]} | {error, not_found}.
lookup(Name) ->
    mycelium_registry:lookup(Name).

%% Stability: supported.
-spec lookup_local(atom() | binary()) -> {ok, pid()} | {error, not_found}.
lookup_local(Name) ->
    mycelium_registry:lookup_local(Name).

%% Stability: supported.
-spec list_services() -> [atom() | binary()].
list_services() ->
    mycelium_registry:list_services().

%% Find service with overlay routing fallback and transparent retry
%% Checks local, then remote cache, then overlay routing.
%% Stability: supported.
-spec whereis_service(atom() | binary()) -> {ok, pid()} | {ok, node(), pid()} | {error, not_found}.
whereis_service(Name) ->
    whereis_service(Name, #{}).

%% Stability: supported.
-spec whereis_service(atom() | binary(), map()) ->
    {ok, pid()} | {ok, node(), pid()} | {error, not_found}.
whereis_service(Name, Opts) ->
    Retries = maps:get(retries, Opts, ?DEFAULT_RETRIES),
    whereis_service_retry(Name, Retries, ?BASE_BACKOFF_MS).

whereis_service_retry(Name, 0, _Delay) ->
    do_whereis_service(Name);
whereis_service_retry(Name, Retries, Delay) ->
    case do_whereis_service(Name) of
        {ok, _} = Success ->
            Success;
        {ok, _, _} = Success ->
            Success;
        {error, not_found} ->
            ActualDelay = min(Delay, ?MAX_BACKOFF_MS),
            timer:sleep(ActualDelay + rand:uniform(ActualDelay div 2)),
            whereis_service_retry(Name, Retries - 1, Delay * 2)
    end.

do_whereis_service(Name) ->
    %% First try local
    case mycelium_registry:lookup_local(Name) of
        {ok, Pid} ->
            {ok, Pid};
        {error, not_found} ->
            %% Try remote cache
            case mycelium_registry:lookup(Name) of
                {ok, [Entry | _]} ->
                    %% Found in remote cache
                    {ok, Entry#service_entry.node, Entry#service_entry.pid};
                {error, not_found} ->
                    %% Try overlay routing
                    mycelium_registry:overlay_lookup(Name)
            end
    end.

%% Register a service with global for transparency
%% Creates a local proxy and registers it with global:register_name
%% Stability: beta.
-spec global_register(atom() | binary()) -> {ok, pid()} | {error, term()}.
global_register(Name) ->
    case whereis_service(Name) of
        {ok, Pid} when node(Pid) =:= node() ->
            %% Local service - register directly with global
            case global:register_name(Name, Pid) of
                yes -> {ok, Pid};
                no -> {error, already_registered}
            end;
        {ok, TargetNode, _Pid} ->
            %% Remote service - create proxy and register
            case mycelium_registry:ensure_proxy(Name, TargetNode) of
                {ok, Proxy} ->
                    case global:register_name(Name, Proxy) of
                        yes -> {ok, Proxy};
                        no -> {error, already_registered}
                    end;
                {error, _} = Error ->
                    Error
            end;
        {ok, Pid} ->
            %% Local pid returned from overlay lookup
            case global:register_name(Name, Pid) of
                yes -> {ok, Pid};
                no -> {error, already_registered}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% Get existing proxy for a service
%% Stability: beta.
-spec get_proxy(atom() | binary()) -> {ok, pid()} | not_found.
get_proxy(Name) ->
    mycelium_registry:get_proxy(Name).

%%====================================================================
%% Service Events API
%%====================================================================

%% Subscribe to service events (register, unregister, down)
%% Stability: beta. Event shape may evolve across 0.x minors.
-spec subscribe_services() -> ok.
subscribe_services() ->
    mycelium_service_events:subscribe(self()).

%% Stability: beta.
-spec subscribe_services(pid()) -> ok.
subscribe_services(Pid) ->
    mycelium_service_events:subscribe(Pid).

%% Stability: beta.
-spec unsubscribe_services(pid()) -> ok.
unsubscribe_services(Pid) ->
    mycelium_service_events:unsubscribe(Pid).

%%====================================================================
%% Leader Election / Singletons
%%
%% "Exactly one node runs this job." The calling process campaigns for
%% a named singleton; the cluster elects one leader (highest priority,
%% ties broken by the lowest node atom) and re-elects on membership
%% change. Each term carries a fencing token so a stale leader cannot
%% corrupt shared state. See `docs/concepts/leader-election.md'.
%%====================================================================

%% @doc Campaign for leadership of the singleton `Name'. The calling
%% process becomes a candidate and is monitored; if it dies it stops
%% being a candidate. The return value is the caller's initial role:
%%   - `{ok, {leader, Fence}}' if it holds leadership now
%%   - `{ok, follower}'        otherwise
%% On every later transition the caller is sent one of:
%%   - `{mycelium_leader, Name, {elected, Fence}}'
%%   - `{mycelium_leader, Name, revoked}'
%%
%% `Fence' is a `non_neg_integer()' fencing token, strictly increasing
%% across leadership terms within a connected partition. Stamp it on
%% writes to a shared resource and have the resource reject any
%% operation whose token is not strictly greater than the highest it
%% has accepted; that is what makes "exactly one" safe when an old
%% leader is paused or partitioned.
%%
%% Partition caveat: under a network partition each side may elect its
%% own leader, and token monotonicity is only guaranteed within a
%% connected component. Safety then rests on the resource's fence check.
%%
%% Stability: beta. The message and return shapes may change across a
%% 0.x minor bump.
-spec lead(term()) ->
    {ok, {leader, non_neg_integer()}}
    | {ok, follower}
    | {error, term()}.
lead(Name) ->
    mycelium_leader:lead(Name).

%% @doc As `lead/1' with options. `#{priority => integer()}' (default
%% `0') biases the election: higher priority wins, ties fall back to
%% the lowest node atom.
%% Stability: beta.
-spec lead(term(), map()) ->
    {ok, {leader, non_neg_integer()}}
    | {ok, follower}
    | {error, term()}.
lead(Name, Opts) ->
    mycelium_leader:lead(Name, Opts).

%% @doc Stop campaigning for `Name' and yield leadership if held. No
%% `revoked' message is sent (the caller asked to step down).
%% Stability: beta.
-spec resign(term()) -> ok.
resign(Name) ->
    mycelium_leader:resign(Name).

%% @doc The current leader for `Name' cluster-wide, if any.
%% Stability: beta.
-spec leader(term()) -> {ok, node(), pid()} | {error, no_leader}.
leader(Name) ->
    mycelium_leader:leader(Name).

%% @doc Whether this node currently holds leadership for `Name'.
%% Stability: beta.
-spec is_leader(term()) -> boolean().
is_leader(Name) ->
    mycelium_leader:is_leader(Name).

%% @doc This node's fencing token for `Name', valid only while it leads.
%% Stability: beta.
-spec fence(term()) -> {ok, non_neg_integer()} | {error, not_leader}.
fence(Name) ->
    mycelium_leader:fence(Name).

%%====================================================================
%% Sharded Placement
%%
%% Consistent (rendezvous) hashing over a replicated live-node set:
%% given a key, every node agrees (eventually) on the owner. Subscribe
%% to ownership events to hand off / take over partitions on churn. See
%% `docs/concepts/sharded-placement.md'.
%%====================================================================

%% @doc The node that should own `Key' cluster-wide. Agreement is
%% eventual: under churn nodes can briefly disagree until the member set
%% converges. Stability: beta.
-spec place(term()) -> node() | undefined.
place(Key) ->
    mycelium_shard:place(Key).

%% @doc The top-`N' distinct owner nodes for `Key' (best first), for
%% replicated placement. Stability: beta.
-spec owners(term(), pos_integer()) -> [node()].
owners(Key, N) ->
    mycelium_shard:owners(Key, N).

%% @doc Whether this node currently owns `Key'. Stability: beta.
-spec is_owner(term()) -> boolean().
is_owner(Key) ->
    mycelium_shard:is_owner(Key).

%% @doc The ring partition `Key' falls in (0..ring_size-1). Use it to map
%% keys to partitions when reacting to ownership events. Stability: beta.
-spec partition(term()) -> non_neg_integer().
partition(Key) ->
    mycelium_shard:partition(Key).

%% @doc The current live member set (sorted). Stability: beta.
-spec members() -> [node()].
members() ->
    mycelium_shard:members().

%% @doc Subscribe the caller to ownership events. Receives
%% `{mycelium_shard, {acquired, Partition}}' when this node gains a
%% partition and `{mycelium_shard, {released, Partition}}' when it loses
%% one. Stability: beta.
-spec subscribe_shard() -> ok.
subscribe_shard() ->
    mycelium_shard:subscribe(self()).

%% Stability: beta.
-spec subscribe_shard(pid()) -> ok.
subscribe_shard(Pid) ->
    mycelium_shard:subscribe(Pid).

%%====================================================================
%% Durable Reminders
%%
%% Cluster-wide, replicated, fire-at-most-once timers. The reminder
%% survives the node that armed it; it fires on whichever node owns the
%% key at fire time (via sharded placement). Subscribe to receive
%% `{mycelium_reminder, Key, Payload, Fence}'. See
%% `docs/concepts/durable-reminders.md'.
%%====================================================================

%% @doc Set a reminder for `Key' to fire at absolute wall-clock
%% `FireAtMs' (`erlang:system_time(millisecond)' scale), delivering
%% `Payload' to subscribers on the owning node. Re-setting a `Key'
%% replaces it. Fires exactly once in steady state; best-effort under
%% churn or a crash at the fire instant. Stability: beta.
-spec remind(term(), integer(), term()) -> ok.
remind(Key, FireAtMs, Payload) ->
    mycelium_reminder:remind(Key, FireAtMs, Payload).

%% @doc Like `remind/3' but `DelayMs' from now, converted to an absolute
%% target so all nodes agree. Stability: beta.
-spec remind_after(term(), non_neg_integer(), term()) -> ok.
remind_after(Key, DelayMs, Payload) ->
    mycelium_reminder:remind_after(Key, DelayMs, Payload).

%% @doc Cancel a pending reminder cluster-wide. Stability: beta.
-spec cancel_reminder(term()) -> ok.
cancel_reminder(Key) ->
    mycelium_reminder:cancel_reminder(Key).

%% @doc Subscribe the caller to reminder deliveries. Receives
%% `{mycelium_reminder, Key, Payload, Fence}' on the node that owns the
%% key when it fires. Subscribe on every node where the handler may run.
%% Stability: beta.
-spec subscribe_reminders() -> ok.
subscribe_reminders() ->
    mycelium_reminder:subscribe(self()).

%% Stability: beta.
-spec subscribe_reminders(pid()) -> ok.
subscribe_reminders(Pid) ->
    mycelium_reminder:subscribe(Pid).

%% Stability: beta.
-spec unsubscribe_reminders(pid()) -> ok.
unsubscribe_reminders(Pid) ->
    mycelium_reminder:unsubscribe(Pid).

%%====================================================================
%% Replicated maps - gossiped, eventually-consistent key-value state
%%====================================================================

%% @doc Start a replicated map named `Name' on this node. A map is
%% node-local: to be cluster-wide it must run on every participating node
%% (declare it in the `replicated_maps' env, or call this on each node).
%% Idempotent. See `mycelium_map' for the full API and caveats.
%% Stability: beta.
-spec new_map(atom()) -> {ok, pid()} | {error, term()}.
new_map(Name) ->
    mycelium_map:new(Name).

%% Stability: beta.
-spec new_map(atom(), mycelium_map:opts()) -> {ok, pid()} | {error, term()}.
new_map(Name, Opts) ->
    mycelium_map:new(Name, Opts).

%% @doc Stop a map on this node (node-local; not a cluster-wide erase).
%% Stability: beta.
-spec delete_map(atom()) -> ok.
delete_map(Name) ->
    mycelium_map:delete_map(Name).

%% Stability: beta.
-spec map_put(atom(), term(), term()) ->
    ok | {error, invalid_value | no_such_map}.
map_put(Name, Key, Value) ->
    mycelium_map:put(Name, Key, Value).

%% Stability: beta.
-spec map_remove(atom(), term()) -> ok | {error, no_such_map}.
map_remove(Name, Key) ->
    mycelium_map:remove(Name, Key).

%% Stability: beta.
-spec map_get(atom(), term()) -> {ok, term()} | not_found.
map_get(Name, Key) ->
    mycelium_map:get(Name, Key).

%% Stability: beta.
-spec map_keys(atom()) -> [term()].
map_keys(Name) ->
    mycelium_map:keys(Name).

%% Stability: beta.
-spec map_to_list(atom()) -> [{term(), term()}].
map_to_list(Name) ->
    mycelium_map:to_list(Name).

%% @doc Subscribe the caller to a map's change events:
%% `{mycelium_map, Name, {put, Key, Value} | {remove, Key}}'.
%% Stability: beta.
-spec subscribe_map(atom()) -> ok | {error, no_such_map}.
subscribe_map(Name) ->
    mycelium_map:subscribe(Name).

%% Stability: beta.
-spec subscribe_map(atom(), pid()) -> ok | {error, no_such_map}.
subscribe_map(Name, Pid) ->
    mycelium_map:subscribe(Name, Pid).

%% Stability: beta.
-spec unsubscribe_map(atom()) -> ok | {error, no_such_map}.
unsubscribe_map(Name) ->
    mycelium_map:unsubscribe(Name).

%% Stability: beta.
-spec unsubscribe_map(atom(), pid()) -> ok | {error, no_such_map}.
unsubscribe_map(Name, Pid) ->
    mycelium_map:unsubscribe(Name, Pid).

%%====================================================================
%% Via Callbacks - for use with {via, mycelium, Name}
%%====================================================================
%% These callbacks implement the standard name registration interface,
%% allowing mycelium to be used as a process registry with gen_server,
%% gen_statem, etc.
%%
%% Example usage:
%%   %% Start a gen_server registered with mycelium
%%   gen_server:start({via, mycelium, my_service}, ?MODULE, [], [])
%%
%%   %% Call the service by name
%%   gen_server:call({via, mycelium, my_service}, request)
%%
%%   %% Send a message to a remote service
%%   mycelium:send(my_service, {data, Payload})
%%
%% For remote services, use whereis_service/1 which returns {ok, Node, Pid}
%% for remote processes, then send directly:
%%   case mycelium:whereis_service(remote_svc) of
%%       {ok, Pid} -> Pid ! Msg;                    %% local
%%       {ok, _Node, Pid} -> Pid ! Msg;             %% remote
%%       {error, not_found} -> handle_not_found()
%%   end

%% Stability: supported.
-spec register_name(Name :: term(), Pid :: pid()) -> yes | no.
register_name(Name, Pid) when is_pid(Pid) ->
    case mycelium_registry:register_service(Name, Pid, #{}) of
        ok -> yes;
        {error, _} -> no
    end.

%% Stability: supported.
-spec unregister_name(Name :: term()) -> ok.
unregister_name(Name) ->
    mycelium_registry:unregister_service(Name).

%% Stability: supported.
-spec whereis_name(Name :: term()) -> pid() | undefined.
whereis_name(Name) ->
    case whereis_service(Name) of
        {ok, Pid} -> Pid;
        {ok, _Node, Pid} -> Pid;
        {error, not_found} -> undefined
    end.

%% Stability: supported.
-spec send(Name :: term(), Msg :: term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            erlang:error({badarg, {Name, Msg}});
        Pid ->
            Pid ! Msg,
            Pid
    end.

%%====================================================================
%% Connection Migration
%%====================================================================

%% @doc Trigger RFC 9000 §9 path migration on the QUIC connection
%% backing the dist channel to `Node'. The connection rebinds to a
%% new local 4-tuple via PATH_CHALLENGE/PATH_RESPONSE; keys, streams,
%% and any open circuits ride through transparently. Useful when the
%% local network changes (NIC/IP swap, tethering, multi-link policy).
%%
%% Returns `ok' on successful path validation. Common errors:
%% - `{error, not_connected}' — no current dist channel to `Node'
%% - `{error, no_conn}' — controller alive but underlying conn gone
%% - `{error, peer_disable_migration}' — peer set the transport-param
%%   flag forbidding migration; treat as terminal for this connection
%% - `{error, timeout}' — path validation didn't complete in time
%%
%% Stability: beta. The opts map may grow keys; existing keys stay.
-spec migrate_peer(node()) -> ok | {error, term()}.
migrate_peer(Node) ->
    migrate_peer(Node, #{}).

%% Stability: beta.
-spec migrate_peer(node(), #{timeout => pos_integer()}) ->
    ok | {error, term()}.
migrate_peer(Node, Opts) when is_atom(Node), is_map(Opts) ->
    Result =
        case mycelium_path_stats:connection(Node) of
            {ok, Conn} -> quic:migrate(Conn, Opts);
            Err -> Err
        end,
    Outcome =
        case Result of
            ok -> ok;
            _ -> fail
        end,
    mycelium_metrics:migrate_result(Node, Outcome),
    Result.

%%====================================================================
%% Test Helpers
%%====================================================================

%% Start a persistent process that holds a service registration
%% Used by integration tests to avoid RPC process lifetime issues
%% Stability: experimental. Likely to move out of `mycelium.erl' into
%% a dedicated test-helpers module before 1.0.
-spec start_service_holder(atom() | binary()) -> {ok, pid()} | {error, term()}.
start_service_holder(ServiceName) ->
    Parent = self(),
    Pid = spawn(fun() -> service_holder_init(ServiceName, Parent) end),
    receive
        {Pid, ok} -> {ok, Pid};
        {Pid, {error, Reason}} -> {error, Reason}
    after 5000 ->
        exit(Pid, kill),
        {error, timeout}
    end.

%% Stop a service holder process
%% Stability: experimental.
-spec stop_service_holder(pid()) -> ok.
stop_service_holder(Pid) ->
    Pid ! stop,
    ok.

%% Internal: service holder initialization
service_holder_init(ServiceName, Parent) ->
    case register_service(ServiceName, #{}) of
        ok ->
            Parent ! {self(), ok},
            service_holder_loop(ServiceName);
        {error, Reason} ->
            Parent ! {self(), {error, Reason}}
    end.

%% Internal: service holder loop. The holder is documented as
%% experimental test-helper API; its lifecycle is governed solely by
%% the explicit `stop' message and an idle backstop. We avoid
%% monitoring the spawning process because callers reach this code
%% via `rpc:call/4', which means `Parent' is a short-lived RPC worker
%% that dies immediately after the call returns.
service_holder_loop(ServiceName) ->
    receive
        stop ->
            unregister_service(ServiceName),
            ok;
        _ ->
            service_holder_loop(ServiceName)
    end.
