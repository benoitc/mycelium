# The replicated substrate

`mycelium_replica` is the low-level gossip/CRDT engine behind
[`mycelium_map`](../concepts/replicated-maps.md), the service registry,
leader election, sharded placement, and durable reminders. It drives a
gossiped OR-Map: it broadcasts add/remove deltas, routes incoming deltas
to your merge callback, full-syncs state to peers on connect (and pulls
state on start), and drops a node's entries on `peer_down`.

Reach for it directly only when `mycelium_map` does not fit, that is when
you need **custom merge or snapshot semantics**: layering extra invariants
on top of the OR-Map (leader election layers fencing tokens), a different
projection, or a tailored full-sync. For an ordinary replicated key-value
map, use `mycelium_map`. Stability: beta.

## Starting an instance

```erlang
mycelium_replica:start_link(#{name => my_instance, callback => my_module}).
```

`name` is BOTH the registered process name AND the Plumtree tag that
scopes this instance's broadcasts, so it must be a unique atom. All
instances share the one Plumtree bus; each ignores payloads carrying
another instance's tag. One callback module can back several
independently-named instances, because every callback receives the
instance `name` as its first argument (this is how every `mycelium_map`
shares the `mycelium_map` module).

The OWNER process holds the actual OR-Map and implements the callbacks, so
it can run its side effects synchronously. Start the owner BEFORE its
replica instance: the callbacks run in the replica process and cast into
the owner, which must already exist. The per-instance supervisor uses
`rest_for_one` (owner then replica) to enforce that on every restart.

## The callback contract

```erlang
-callback replica_merge_delta(Name, Delta)        -> ok.
-callback replica_apply_full_sync(Name, Snapshot) -> ok.
-callback replica_full_sync_snapshot(Name)        -> {sync, Snapshot} | empty.
-callback replica_remove_node(Name, node())       -> ok.
-callback replica_merge_custom(Name, Payload)     -> ok.   %% optional
```

- `replica_merge_delta/2` — merge an incoming `{Key, entry}` delta into the
  owner's map and run its side effects.
- `replica_apply_full_sync/2` — apply a snapshot received from a peer on
  connect (or pulled on start). Same path as a delta for most consumers.
- `replica_full_sync_snapshot/1` — produce the snapshot to push to a newly
  connected peer, or `empty` when there is nothing to send.
- `replica_remove_node/2` — drop entries owned by a node that left or
  failed. A no-op is a valid choice (most built-ins keep their data; see
  the `prune_on_peer_down` discussion in [replicated
  maps](../concepts/replicated-maps.md#membership-churn)).
- `replica_merge_custom/2` — merge a feature-specific broadcast (see
  below). Optional; omit it if you do not use `broadcast_custom/2`.

## Broadcasting

```erlang
mycelium_replica:broadcast_update(Name, {add, Key, Value}).
mycelium_replica:broadcast_update(Name, {remove, Key}).
mycelium_replica:broadcast_custom(Name, Payload).
```

`broadcast_update/2` gossips OR-Map add/remove deltas: an add carries a
fresh dot, a remove a tombstone, so the receiver's merge resolves against
any in-flight value by HLC. `broadcast_custom/2` gossips an arbitrary
payload on the instance's tag, delivered to `replica_merge_custom/2`. Use
it for invariants the plain OR-Map cannot express: leader election
broadcasts `{Name, Fence}` this way to publish the fencing token alongside
the election (`mycelium_leader.erl:142`, `mycelium_leader.erl:316`).

## Late start and recovery

An instance started after the cluster has already formed gets no `peer_up`
for already-connected peers, so on start it seeds its peer set from the
active view and pulls a full sync from those peers via
`replica_full_sync_snapshot/1` / `replica_apply_full_sync/2`. A restarted
owner recovers state the same way. There is nothing to do in your
callbacks for this; it is built into the driver.

## Wire safety

Your callbacks receive entries straight off gossip. There are two distinct
concerns:

- **Wrapper safety.** Feeding malformed dots/HLCs, an empty dot map, or a
  non-map payload to `mycelium_ormap:absorb_clock/merge` can crash the
  merge or the shared `mycelium_hlc` server. An implementer that merges
  deltas from sources it does not fully control SHOULD validate the
  wrapper before merging.
- **Leaf/payload validation** (is this value well-formed for my app?) is
  entirely your own concern.

[`mycelium_crdt_wire`](#mycelium_crdt_wire) is the provided helper for the
first, with an optional leaf hook for the second. Using it is
**recommended, not enforced**: an implementer with full control of its
writers, or its own validation, may skip it. Be aware that of the
built-ins only the reminder validates today; the registry, leader, and
shard have purely internal writers, so they do not. Validate if your deltas
can come from a source you do not fully control.

## mycelium_crdt_wire

`mycelium_crdt_wire` is the safe gossip-ingest surface. Stability:
supported.

```erlang
%% Wrapper validity (and an optional leaf-value check).
mycelium_crdt_wire:valid_entry(Entry).
mycelium_crdt_wire:valid_entry(Entry, fun is_my_value/1).

%% Keep only the valid entries of a (possibly non-map) payload.
Accepted = mycelium_crdt_wire:accept(Payload, LeafFun).

%% accept + absorb_clock + merge, in one step.
{Merged, Accepted} = mycelium_crdt_wire:ingest(LocalMap, Incoming, LeafFun).
```

`accept/2` and `ingest/3` guard the top-level argument: a non-map payload
(a malformed broadcast could deliver `{delta, Node, garbage}`) returns
`#{}` / leaves the local map unchanged, never crashing the caller on any
peer-supplied term. `ingest/3` returns BOTH the merged OR-Map AND the
accepted (validated, filtered) sub-map, so you can reconcile and emit
events for exactly the keys that changed instead of rescanning. On a
full-sync snapshot, `Accepted` is the whole validated snapshot, which is
how a map populates on first sync.

## Transport coupling

Gossip rides `mycelium_plumtree` + `mycelium_hyparview_events` over
mycelium's distribution carrier, so a consumer must run on mycelium's
distribution. A pluggable transport (for apps with their own membership)
is future work.

## Related

- [Replicated maps](../concepts/replicated-maps.md) is the high-level map
  built on this behaviour; start there unless you need custom merge.
- [Leader election](../concepts/leader-election.md) is the canonical
  custom-merge consumer (fencing via `broadcast_custom/2`).
- [Service registry](../concepts/service-registry.md) explains the OR-Map
  model the deltas carry.
