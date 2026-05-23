# Hybrid logical clocks

The [service registry](service-registry.md)'s CRDT needs a way
to order events across nodes that does not assume synchronised
wall clocks. Mycelium uses **hybrid logical clocks** (HLC), an
algorithm that combines a wall-clock timestamp with a logical
counter.

This page explains how HLCs work and where mycelium uses them.

## The problem

Two events on different nodes can have arbitrarily related
wall-clock timestamps. A node's clock may drift, an NTP step
may move it backwards, a virtual machine may pause and resume.
Comparing two wall timestamps tells you nothing about which
event happened first.

A pure logical clock (a Lamport clock) gives you ordering, but
it loses any connection to physical time. You cannot ask "did
this event happen in the last minute" with a Lamport clock.

Hybrid logical clocks combine the two: they order events
across nodes (like a Lamport clock) while staying close to
physical wall time (close enough to use for human-readable
timestamps and for "in the last minute" queries).

## The shape

An HLC timestamp is a pair `{wall_ms, logical}`:

```erlang
-record(timestamp, {
    wall_time :: integer(),  %% milliseconds since epoch
    logical   :: integer()   %% counter, breaks ties
}).
```

The `wall_time` field is approximately the local wall clock.
The `logical` field is incremented when the local clock did not
move forward fast enough to distinguish two adjacent events,
and when receiving a timestamp from a peer with the same or
higher wall_time.

## The operations

There are two operations:

**Generate a new local timestamp.**

```erlang
mycelium_hlc:now() -> #timestamp{}.
```

The implementation reads the local wall clock and ensures the
returned timestamp is strictly greater than any previously
generated timestamp on this node.

**Update on receiving a peer timestamp.**

```erlang
mycelium_hlc:update(PeerTs) -> ok.
```

Advances the local HLC to be strictly greater than both the
local reading and the peer's timestamp. The next
`mycelium_hlc:now/0` returns a value greater than `PeerTs`.

The algorithm preserves the invariant: for any two events A and
B in the cluster, if A's HLC < B's HLC, then either A happened
before B in the local-clock sense, or A's effect was observed
before B was generated.

## Comparing timestamps

```erlang
mycelium_hlc:compare(T1, T2) -> lt | eq | gt.
```

Lexicographic order on `(wall_time, logical)`. A timestamp with
a larger wall_time is greater; ties on wall_time are broken by
logical.

## Where mycelium uses HLCs

Three subsystems:

- **The service registry.** Each OR-Map dot is
  `{node, hlc_timestamp}`. The HLC guarantees that two
  registrations from the same node are ordered, and that two
  registrations from different nodes can be compared causally.
- **The router's route cache.** Cached overlay routes are
  stamped with an HLC and aged out by wall-time TTL.
- **Anywhere an application wants causally-ordered timestamps
  across the cluster.** The `mycelium_hlc` module is public.

## On-the-wire encoding

For network transmission, HLC timestamps encode to a fixed-size
binary:

```erlang
mycelium_hlc:to_binary(Ts) -> binary().    %% 12 bytes
mycelium_hlc:from_binary(Bin) -> Ts.
```

The format is `<<WallTime:64/big, Logical:32/big>>`. Endian and
size are stable; any future change would be a wire-protocol
break and would land in the CHANGELOG.

## Clock requirements

HLCs work best when wall clocks are roughly synchronised. NTP-level
precision (within a few hundred milliseconds across the cluster)
is sufficient; nanosecond precision is unnecessary.

If wall clocks drift very far apart, two consequences:

- The `logical` field stays small in steady state, but two
  registrations from clock-disagreeing peers may have a large
  gap in wall_time order. This is fine: the algorithm still
  orders them correctly relative to causality.
- Wall-time queries ("which entries are older than five
  minutes") may be off by the drift amount. If you need
  millisecond-precise expiry across the cluster, do not use
  HLC alone.

Mycelium's own use cases tolerate drift up to a few minutes
without operational impact.

## Comparison with alternatives

| Property                   | Wall clock | Lamport | HLC |
|----------------------------|------------|---------|-----|
| Orders across nodes        | No         | Yes     | Yes |
| Close to physical time     | Yes        | No      | Yes |
| Survives NTP step          | No         | Yes     | Yes |
| Bounded by clock drift     | n/a        | n/a     | Yes |

HLCs give you the union of properties; the cost is one extra
counter per timestamp.

## API

```erlang
mycelium_hlc:now() -> #timestamp{}.
mycelium_hlc:update(PeerTs) -> ok.
mycelium_hlc:compare(T1, T2) -> lt | eq | gt.
mycelium_hlc:wall_time(Ts) -> integer().     %% extract wall_ms
mycelium_hlc:logical(Ts) -> integer().       %% extract logical
mycelium_hlc:to_binary(Ts) -> binary().      %% 12 bytes
mycelium_hlc:from_binary(Bin) -> Ts.
```

The HLC API is `supported` in
[features.md](../features.md).

## Further reading

The original paper:

> Logical Physical Clocks. Sandeep S. Kulkarni, Murat Demirbas,
> Deepak Madappa, Bharadwaj Avva, Marcelo Leone. OPODIS 2014.

The algorithm is small enough to fit on a card and has been
re-implemented in many distributed systems (CockroachDB, MongoDB,
Yugabyte, others use variants).

## Related

- [Service registry](service-registry.md) is the main consumer
  of HLCs in mycelium.
- [Architecture](../reference/architecture.md) covers the
  supervision tree position of `mycelium_hlc`.
