# Service registry

Mycelium's service registry lets a process register itself under
a name; every other node in the cluster can then find that
process. The registration is replicated across the cluster
without coordination, and it merges deterministically under
concurrent updates.

This page covers the data model, the lifecycle, and the patterns
you reach for when building on top.

## The data model

The registry is an **Observed-Remove Map** (OR-Map): a CRDT
designed for the "named registrations that come and go" pattern.

Three properties matter:

- **Adds and removes commute.** Two concurrent adds of the same
  name produce two entries. A subsequent remove only deletes
  the dots it has observed; new concurrent adds survive.
- **Tombstones are bounded.** Old removes carry only the dots
  they observed and are garbage-collected once every node has
  applied them.
- **Causal merging.** Two replicas merge by union of dots plus
  the rule that a tombstoned dot stays tombstoned.

Each registration carries a *dot*, a `{node, hlc_timestamp}`
pair. The hybrid logical clock guarantees that two registrations
from the same node are ordered and that two registrations from
different nodes can be compared causally. The CRDT does not need
synchronised wall clocks.

For the underlying clock, see
[hybrid logical clocks](hybrid-logical-clocks.md).

## The registration lifecycle

The typical flow:

```erlang
%% On node A
1> mycelium:register_service(my_worker, #{role => primary}).
ok

%% A moment later, on node B. lookup/1 returns #service_entry{}
%% records (fields: name, pid, node, meta).
2> mycelium:lookup(my_worker).
{ok, [{service_entry, my_worker, <0.123.0>, 'node1@host', #{role => primary}}]}

3> {ok, Node, Pid} = mycelium:whereis_service(my_worker).
{ok, 'node1@host', <0.123.0>}

4> Pid ! Msg.   %% standard Erlang send, opens dist on demand
```

What happens under the hood:

1. `register_service/2` inserts an entry into the local OR-Map
   with a fresh dot.
2. The registry's `mycelium_replica` instance builds a delta of
   "what changed" and broadcasts it through
   [Plumtree](gossip-broadcast.md).
3. Each peer receives the delta and merges it into its own
   OR-Map.
4. From the moment the merge completes on node B, B's
   `whereis_service/1` finds the registration.

Replication is asynchronous and eventually consistent. The
delay between registration on A and visibility on B is
typically under one second on a healthy cluster. Code that
registers and immediately looks up on a different node should
expect to retry briefly; `whereis_service/2` accepts a
`retries` option that does this for you.

## Local vs remote lookups

Three lookup primitives, in increasing scope:

```erlang
%% Local pids only.
mycelium:lookup_local(Name) -> {ok, pid()} | {error, not_found}.

%% Every replica we know about, including remote ones.
mycelium:lookup(Name) -> {ok, [#service_entry{}]} | {error, not_found}.

%% Single best match: local preferred, then remote, then overlay.
mycelium:whereis_service(Name) ->
    {ok, pid()}              %% local
  | {ok, node(), pid()}      %% remote
  | {error, not_found}.

mycelium:whereis_service(Name, #{retries => 3}).
```

`whereis_service/1` is the function you reach for from
application code. It tries the local registry, then the local
cache of remote registrations, and finally an overlay route
request. The `retries` option absorbs the small replication
delay after a fresh registration.

## Metadata

The third argument to `register_service/2` is a map. The map is
replicated alongside the name and pid:

```erlang
mycelium:register_service(worker_service, #{
    shard => 1,
    capacity => 10000,
    version => <<"1.4.2">>
}).
```

You can put anything that fits a Map there. Two common uses:

- **Load shedding.** A worker periodically re-registers with an
  updated load metric; a load balancer reads the map at lookup
  time and picks the least-loaded worker.
- **Capability discovery.** A service publishes a list of
  capabilities; clients filter by capability when picking a
  service instance.

The metadata is not indexed: `lookup/1` returns all instances
and the caller filters. For most workloads this is fine; if you
need a queryable index, build it on top.

## Multiple instances per name

The OR-Map permits multiple entries under the same name, one per
registering node:

```erlang
%% On node A
mycelium:register_service(worker, #{shard => 1}).

%% On node B
mycelium:register_service(worker, #{shard => 2}).

%% From node C
{ok, Entries} = mycelium:lookup(worker).
%% length(Entries) =:= 2
```

This is the natural shape for a worker pool. If you want
exclusivity, layer it on top: only one node calls
`register_service/2` at a time, or your application checks the
existing entries before registering.

## Service events

A subscriber receives messages when services come and go:

```erlang
mycelium:subscribe_services().
%% Receives:
%%   {mycelium_service_event, {service_registered, Name, Node}}
%%   {mycelium_service_event, {service_unregistered, Name, Node}}
%%   {mycelium_service_event, {service_down, Name, Node, Reason}}
```

`service_down` fires when the registered pid dies (the registry
monitors every pid it stores). The local node's `service_down`
fires immediately; remote nodes see it after the gossip
propagates.

## Via callbacks

Mycelium implements the standard via-name interface, so
`gen_server` (and any module that uses the same convention) can
use mycelium names directly:

```erlang
%% Start a gen_server registered through mycelium
gen_server:start({via, mycelium, my_service}, my_module, [], []).

%% Call it by name
gen_server:call({via, mycelium, my_service}, request).

%% Send to it
mycelium:send(my_service, Msg).
```

This makes mycelium a drop-in replacement for `gproc`-style
name registration when the name needs to be cluster-wide.

For remote services, the registry returns a *service proxy*: a
local pid that forwards `gen_server` calls and casts to the
remote service. The proxy is what makes via-`mycelium` work for
processes that live on another node.

## Common patterns

### Pool of workers

```erlang
-include_lib("mycelium/include/mycelium.hrl").

least_loaded(Name) ->
    {ok, Entries} = mycelium:lookup(Name),
    Sorted = lists:sort(
        fun(A, B) ->
            maps:get(load, A#service_entry.meta, 0)
            =< maps:get(load, B#service_entry.meta, 0)
        end,
        Entries
    ),
    hd(Sorted).
```

A worker that re-registers periodically with an updated `load`
key publishes a load signal that callers can use.

### Graceful degradation

```erlang
call_service(Name, Request) ->
    case mycelium:whereis_service(Name, #{retries => 3}) of
        {ok, Pid} ->
            call_with_timeout(Pid, Request);
        {ok, _Node, Pid} ->
            call_with_timeout(Pid, Request);
        {error, not_found} ->
            {error, unavailable}
    end.

call_with_timeout(Pid, Request) ->
    try gen_server:call(Pid, Request, 5000) of
        Reply -> Reply
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _}  -> {error, gone}
    end.
```

### Cache invalidation via service events

```erlang
init(_) ->
    mycelium:subscribe_services(),
    {ok, #{cache => #{}}}.

handle_info({mycelium_service_event, {service_down, Name, _N, _R}}, S) ->
    {noreply, S#{cache := maps:remove(Name, maps:get(cache, S))}};
handle_info(_, S) ->
    {noreply, S}.
```

## Configuration knobs

The registry itself has no operator-tunable knobs; it follows
the cluster topology. Performance settings that matter live
under the [gossip broadcast](gossip-broadcast.md) layer.

## API

```erlang
%% Register and unregister.
register_service(Name) -> ok | {error, term()}.
register_service(Name, Meta) -> ok | {error, term()}.
register_service(Name, Pid, Meta) -> ok | {error, term()}.
unregister_service(Name) -> ok.

%% Lookup.
lookup(Name) -> {ok, [#service_entry{}]} | {error, not_found}.
lookup_local(Name) -> {ok, pid()} | {error, not_found}.
list_services() -> [Name].
whereis_service(Name) ->
    {ok, pid()} | {ok, node(), pid()} | {error, not_found}.
whereis_service(Name, #{retries => N}) -> same.

%% Via callbacks.
register_name(Name, Pid) -> yes | no.
unregister_name(Name) -> ok.
whereis_name(Name) -> pid() | undefined.
send(Name, Msg) -> pid().

%% Events.
subscribe_services() -> ok.
subscribe_services(Pid) -> ok.
unsubscribe_services(Pid) -> ok.

%% Global integration (beta).
global_register(Name) -> {ok, pid()} | {error, term()}.
get_proxy(Name) -> {ok, pid()} | not_found.
```

## Related

- [Gossip broadcast](gossip-broadcast.md) is the protocol
  underneath the registry's replication.
- [Hybrid logical clocks](hybrid-logical-clocks.md) are the
  timestamps in each OR-Map dot.
- [Cluster membership](cluster-membership.md) — the registry
  builds on top of the active view.
- [Distributed chat tutorial](../tutorials/distributed-chat.md)
  applies the registry to a real application.
