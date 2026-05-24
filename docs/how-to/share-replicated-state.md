# Share replicated state

You want a small piece of state that every node can read and any node can
update: a cluster-wide config table, a set of feature flags, a routing
table. You want a write on one node to show up on the others, without
running Consul or etcd. `mycelium_map` is a named, gossiped, last-write-
wins key-value map for exactly that.

The example here is a cluster-wide **feature-flag** map called `flags`.

## Host the map on every node

A map is node-local: it only converges across the nodes that actually run
it. For a cluster-wide map, host it everywhere. The simplest way is to
declare it in the app env so every node starts it at boot:

```erlang
%% sys.config
{mycelium, [
    {replicated_maps, [
        {flags, #{}}
    ]}
]}.
```

Since every node ships the same config, `flags` exists cluster-wide with
no per-node call. For a runtime or ad-hoc map, call `new_map/2` instead on
each node that should host it (for example from your own application
start):

```erlang
{ok, _} = mycelium:new_map(flags).
```

`new_map` is idempotent, so calling it again on a node that already runs
the map is a no-op.

## Put and get

```erlang
ok = mycelium:map_put(flags, dark_mode, true),

{ok, true} = mycelium:map_get(flags, dark_mode),   %% on any node
not_found  = mycelium:map_get(flags, missing).
```

Reads are lock-free ETS reads; they never block on writes. `map_keys/1`
and `map_to_list/1` return the live entries:

```erlang
[dark_mode]          = mycelium:map_keys(flags),
[{dark_mode, true}]  = mycelium:map_to_list(flags).
```

A write on one node converges to the others eventually, not instantly.
After `map_put` on node A, node B sees the value once the delta gossips.

## React to changes

Subscribe to receive a message on every change, local or remote:

```erlang
init(_) ->
    ok = mycelium:subscribe_map(flags),
    {ok, #{}}.

handle_info({mycelium_map, flags, {put, Key, Value}}, S) ->
    {noreply, apply_flag(Key, Value, S)};
handle_info({mycelium_map, flags, {remove, Key}}, S) ->
    {noreply, clear_flag(Key, S)}.
```

Subscribe on each node where a process needs to react: each node emits the
events for its own copy of the map. The owner monitors subscribers and
drops them automatically when they exit.

## Validate values

Reject bad writes (local puts and incoming gossip) with a validator. It
runs on the writing node and on every node merging a delta, so a malformed
value never lands in the map:

```erlang
%% via new_map/2
{ok, _} = mycelium:new_map(flags, #{validator => fun erlang:is_boolean/1}),

{error, invalid_value} = mycelium:map_put(flags, dark_mode, "yes").
```

In `replicated_maps` config, supply the validator as `{Mod, Fun}` (a fun
is not config-friendly):

```erlang
{replicated_maps, [
    {flags, #{validator => {erlang, is_boolean}}}
]}.
```

## Tune tombstone GC

A remove leaves a tombstone so the deletion propagates; a periodic sweep
drops old tombstones so the store stays bounded. The defaults (sweep every
second, drop after an hour) suit most maps. Lower the TTL only if you
remove keys often and want the store to shrink faster, and keep it well
above your gossip propagation time plus the membership lease:

```erlang
{ok, _} = mycelium:new_map(flags, #{scan_ms => 5000,
                                    tombstone_ttl_ms => 600000}).
```

## Remove and delete

`map_remove/2` deletes a key cluster-wide (it converges like a put):

```erlang
ok = mycelium:map_remove(flags, dark_mode).
```

`delete_map/1` is different and node-local: it stops the map on the
calling node only. It is NOT a cluster-wide erase. To tear a map down
across the cluster, stop it on every node (or stop declaring it and
restart the nodes).

## Mind the contract

`mycelium_map` is for small, cluster-wide, eventually-consistent
control-plane state. State is in memory plus gossip: it survives
individual node deaths (a survivor full-syncs a restarted node) but not a
whole-cluster restart. Reads are eventually consistent. If you need custom
conflict resolution, large values, durable storage, or linearizable
reads, see [the replicated maps concept](../concepts/replicated-maps.md#what-it-guarantees-and-when-not-to-use-it)
for the boundaries and drop to [the substrate
behaviour](../reference/replicated-substrate.md) when you need custom
merge.

## See also

- [Replicated maps](../concepts/replicated-maps.md) for the model and the
  exact guarantees.
- [The replicated substrate](../reference/replicated-substrate.md) for the
  low-level behaviour behind the map.
