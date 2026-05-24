# Core concepts

These pages explain how each piece of mycelium works. They are
not API references; the goal is to give you a mental model.
Reach for the [Reference](../reference/index.md) when you need
exact function signatures or configuration keys.

The concepts are mostly independent. You can read them in any
order, but if it is your first pass, the order below is the
natural one.

## Membership and reachability

- [Cluster membership](cluster-membership.md) — HyParView's
  bounded active and passive views, joining, shuffle, failure
  handling.
- [Gossip broadcast](gossip-broadcast.md) — Plumtree
  push-lazy-push trees, the self-healing graft/prune dance.

## Naming and replication

- [Service registry](service-registry.md) — the OR-Map CRDT
  behind `register_service/2`, `whereis_service/1`, and the
  eventually-consistent service catalogue.
- [Hybrid logical clocks](hybrid-logical-clocks.md) — the
  timestamps the CRDT uses to merge concurrent updates.
- [Leader election](leader-election.md) — cluster-wide singletons
  via `mycelium:lead/2`, with fencing tokens for safety.
- [Sharded placement](sharded-placement.md) — consistent hashing
  over a replicated live-node set; `place/1` and ownership events.
- [Durable reminders](durable-reminders.md) — replicated,
  fire-at-most-once timers that survive the node that armed them.
- [Replicated maps](replicated-maps.md) — `mycelium_map`, a
  gossiped last-write-wins key-value map for cluster-wide
  control-plane state.

## The transport

- [Dist channel](dist-channel.md) — the `-proto_dist mycelium`
  shim over `quic_dist`, the discovery chain, the idle GC.
- [Authentication](authentication.md) — Ed25519 mutual
  challenge-response between TLS handshake and Erlang dist
  handshake.
- [Streams](streams.md) — application-level multiplex over the
  same QUIC connection.
- [Connection migration](connection-migration.md) — RFC 9000 §9
  path rebind for laptops, tunnels, and CGNATs.

## After the concepts

- The [Tutorials](../tutorials/index.md) put these concepts to
  work in a real application.
- The [How-to guides](../how-to/index.md) cover operational
  tasks: production deployment, observability, troubleshooting,
  rotation, relays, testing.
- The [Reference](../reference/index.md) has the API, the full
  configuration list, and the architecture deep dive.
