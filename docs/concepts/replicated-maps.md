# Replicated maps

A replicated map answers one question: **how do I keep a small piece of
state that every node can read and any node can update, without standing
up an external store?** Config, feature flags, a routing or placement
table, presence, a small catalogue. `mycelium_map` is a named,
gossiped, last-write-wins key-value map for exactly that: control-plane
state that should be cluster-wide and eventually consistent.

It is built on the same gossip substrate as the service registry, leader
election, and durable reminders ([the replicated
substrate](../reference/replicated-substrate.md)). Writes broadcast
OR-Map deltas, peers converge, and a map started after the cluster has
already formed pulls existing state from its peers.

## The model

Each named map is a [last-write-wins OR-Map](service-registry.md): a key
holds a value tagged with the [hybrid-logical-clock](hybrid-logical-clocks.md)
stamp of the write that set it. Concurrent writes to the same key on
different nodes resolve deterministically by that stamp, so every node
ends up with the same value. A remove leaves a tombstone, so a delayed
add cannot resurrect a deleted key.

Reads are the live view of that map: `get/2` returns the current value of
a key, `keys/1` and `to_list/1` the live entries. Tombstones and the dot
bookkeeping are not visible to readers.

## One owner, lock-free reads

Each named map on a node is one owner process plus its own gossip
instance:

- The **owner** is the sole writer. Every `put`/`remove` and every merged
  delta or full-sync serialises through its mailbox, so the OR-Map is
  never torn. This bounds write throughput per map, which is the right
  trade for low-rate control-plane state.
- **Reads never touch the owner.** The owner projects the live map into a
  per-map ETS table (`protected`, `{read_concurrency, true}`), and
  `get`/`keys`/`to_list` read that table directly. Reads never block on or
  contend with writes.

On every change the owner updates the OR-Map, writes the ETS projection
(`ets:insert`/`ets:delete`, atomic per key), then emits the change event.
So a subscriber that reads after receiving an event always sees the new
value. ETS gives per-key atomicity but no cross-key snapshot: a multi-key
merge may be observed half-applied. That matches the eventual-consistency
contract.

Each map is its own owner process and its own ETS table, so maps never
contend with one another.

## A map is node-local

`mycelium:new_map/1,2` starts a map on the **calling node only**. A named
map converges across exactly the nodes that run it; a node that never
calls `new_map` has no replica for that map and silently misses its data.
This mirrors the built-in features, which run on every node because
`mycelium` itself starts them.

To make a map cluster-wide you host it on every participating node, one of
two ways:

- **Declare it** in the `replicated_maps` app env. Every node ships the
  same config, so the map exists cluster-wide with no per-node call. This
  is the recommended shape for a fixed, cluster-wide map.
- **Call `new_map/2`** on each node that should host it (from your app
  start, or fanned out) for runtime or ad-hoc maps.

`delete_map/1` is likewise node-local: it stops the map on the calling
node only. It is NOT a cluster-wide erase.

## Joining after the cluster formed

A map started after its peers are already connected does not get a
`peer_up` for them, so on start it seeds its peer set from the current
active view and pulls a full sync from those peers. The new map populates
from existing state and emits a `{put, _, _}` for every key it learns,
through the same path a live delta takes. A restarted owner recovers the
same way.

## Events

Subscribe to observe changes, on local AND remote writes:

```erlang
ok = mycelium:subscribe_map(config),

handle_info({mycelium_map, config, {put, Key, Value}}, S) -> ...;
handle_info({mycelium_map, config, {remove, Key}}, S) -> ...
```

The owner monitors subscriber pids and drops them on `DOWN`. Subscribe
on each node where a process needs to react, since each node emits the
events for its own copy of the map.

## Membership churn

By default a map keeps its entries when a peer leaves the active view
(`prune_on_peer_down` is `false`). A `peer_down` is HyParView topology
churn, not necessarily node death, so dropping data on it would be
surprising for a config or routing map: the entries simply stay and
re-converge.

Set `prune_on_peer_down => true` for presence-style maps, where an entry
means "node X is here". Then when a node leaves, the keys whose writes were
all authored by it are dropped locally. The prune is local and eventual,
not a coordinated delete.

## Tombstones and GC

A remove leaves a tombstone so the deletion propagates and a delayed add
cannot resurrect the key. A periodic sweep (`scan_ms`) drops tombstones
older than `tombstone_ttl_ms` (default one hour) so the store stays
bounded. As with reminders, the horizon must comfortably exceed gossip
propagation plus the membership lease, or a long partition could replay an
add older than a dropped tombstone and spuriously re-create a key. GC never
touches live values.

## What it guarantees, and when not to use it

State is **in memory plus gossip by default**. A map survives the death of
individual nodes (a survivor full-syncs a restarted one), but a whole-cluster
restart loses its contents unless you opt into persistence.

Pass `persist => true` to back the map with a write-ahead log plus periodic
snapshots on disk (under `mycelium_map_data_dir`, default `data/maps`, per
node). Writes are flushed before the call returns and the map is recovered on
boot, so a persisted map survives a full-cluster restart. Persisted values
must be restart-safe data (no pids/ports/refs/funs, which reload as stale
references). Host the persisted map on every node (each keeps its own copy);
recover-then-re-converge on restart works best from a quiesced cluster.

Reads are eventually consistent: there is no consensus, so after a write
other nodes converge once the delta has propagated, and during a partition
two sides can briefly disagree. This is the same trade every other
eventually-consistent piece of mycelium makes. After a partition heals the
map reconverges on its own via periodic anti-entropy (a background full-sync
pull every `replica_anti_entropy_ms`, default 30s), even on a node whose link
survived the split and got no fresh connection event.

`mycelium_map` fits small, cluster-wide, eventually-consistent
control-plane state. It is the wrong tool for:

- data that needs custom conflict resolution (drop to the
  [behaviour](../reference/replicated-substrate.md)),
- high write rates or large values (single writer per map; every value is
  gossiped),
- durable storage beyond `persist => true` (no per-key history, no
  cross-key transactions; it is a recovered OR-Map, not a database),
- unbounded keyspaces (every node holds every key), or
- linearizable reads (reads are eventually consistent).

## Configuration

Per-map options (via `new/2`, or the config-friendly subset in
`replicated_maps`):

| Option               | Default        | Meaning                                                        |
|----------------------|----------------|----------------------------------------------------------------|
| `validator`          | accept all     | `fun((term()) -> boolean())` or `{Mod, Fun}`; rejects bad puts and bad incoming values. |
| `tombstone_ttl_ms`   | env default    | Drop tombstones older than this.                               |
| `scan_ms`            | env default    | Tombstone-GC sweep cadence.                                    |
| `prune_on_peer_down` | `false`        | Drop a departed node's entries on `peer_down` (presence maps). |
| `persist`            | `false`        | Back the map with an on-disk WAL + snapshot; durable across a full-cluster restart. |

App env defaults:

| Key                              | Default | Meaning                                  |
|----------------------------------|---------|------------------------------------------|
| `replicated_maps`                | `[]`    | `[{Name, Opts}]` maps started on boot.   |
| `mycelium_map_scan_ms`           | 1000    | Default `scan_ms` for maps.              |
| `mycelium_map_tombstone_ttl_ms`  | 3600000 | Default `tombstone_ttl_ms` for maps.     |
| `mycelium_map_data_dir`          | `data/maps` | Per-node directory for persisted maps.|

## Related

- [Share replicated state](../how-to/share-replicated-state.md) is the
  worked recipe.
- [The replicated substrate](../reference/replicated-substrate.md) is the
  low-level `mycelium_replica` behaviour underneath, for custom merge.
- [Service registry](service-registry.md) is the same OR-Map model
  specialised for process names.
- [Gossip broadcast](gossip-broadcast.md) carries the deltas and
  full-syncs.
- [Hybrid logical clocks](hybrid-logical-clocks.md) stamp the writes the
  merge resolves on.
