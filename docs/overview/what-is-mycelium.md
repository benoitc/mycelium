# What is mycelium?

Mycelium is a peer-to-peer distribution layer for Erlang/OTP. It
replaces the default TCP-based Erlang distribution with three
opinionated pieces, while keeping `Pid ! Msg`, `gen_server`,
`rpc:call/4`, and `global` working exactly as you expect.

The three pieces are:

- **A bounded membership protocol** (HyParView). Each node keeps a
  small, bounded set of gossip peers. The cluster as a whole
  remains fully addressable: any node can reach any other node on
  demand, through OTP's auto-connect.
- **A secure transport** (QUIC + Ed25519). One QUIC connection per
  peer carries the dist channel; an Ed25519 mutual handshake
  proves identity before any Erlang traffic flows. Connection
  migration moves the session to a new local network path without
  dropping streams.
- **A service registry** (CRDT-backed). Processes register under a
  name; the name is replicated through the cluster and lookups
  return either a local pid or the remote node + pid. No external
  service-discovery service is required.

Visually, this is the shape of a mycelium cluster from one node's
point of view:

![HyParView active view: a node connects to a small set of gossip peers, with additional known peers held in a passive cache.](diagrams/active-view.png)

Each node holds a small *active view* (its current gossip peers,
typically five) and a larger *passive view* (known but
disconnected peers used as warm spares). The cluster topology
scales: a node with a thousand peers in its passive view still
keeps only five active links.

## What you get out of the box

| Capability | What it gives you | Where to read more |
|------------|-------------------|--------------------|
| `-proto_dist mycelium` boot flag | Drop-in replacement for the default dist | [Dist channel concept](../concepts/dist-channel.md) |
| HyParView membership | Bounded connection count, gossip-based reachability | [Cluster membership concept](../concepts/cluster-membership.md) |
| Ed25519 mutual auth | TOFU or strict per-peer key pinning | [Authentication concept](../concepts/authentication.md) |
| CRDT service registry | Cluster-wide named services with eventual consistency | [Service registry concept](../concepts/service-registry.md) |
| Replicated maps | Gossiped key-value state for config, flags, routing tables | [Replicated maps concept](../concepts/replicated-maps.md) |
| Plumtree gossip | Efficient broadcast for service-state updates | [Gossip broadcast concept](../concepts/gossip-broadcast.md) |
| Tagged user streams | App-level multiplex over the same QUIC connection | [Streams concept](../concepts/streams.md) |
| Connection migration | RFC 9000 §9 path rebind without restart | [Migration concept](../concepts/connection-migration.md) |
| `instrument` metrics | OpenTelemetry-style counters and histograms | [Observe a cluster](../how-to/observe-cluster.md) |

## What mycelium does not do

A few things are explicitly *not* part of the project:

- **NAT traversal**, STUN, UPnP, hole punching. Nodes are expected
  to reach each other directly. If they cannot, an external relay
  is wired through the connect-time override hook
  ([route through a relay](../how-to/route-through-relay.md)).
- **Topologies other than HyParView**. There is no full-mesh, no
  client-server, no hub-and-spoke mode. If you need topology
  flexibility, [Partisan](../reference/comparison-with-partisan.md)
  is the right library.
- **A standalone HTTP metrics server**. The `instrument` library
  provides formatters; you wire them into your existing HTTP
  layer.

## When to use mycelium

Mycelium is a good fit when:

- You want a partial-membership topology to replace Erlang's
  full mesh, without giving up `Pid ! Msg`.
- You need cluster-wide service discovery without standing up a
  separate registry (Consul, etcd).
- You want encryption between peers by default, with no
  certificate authority to operate.
- You prefer opinionated defaults to a wide configuration
  surface.

Mycelium is the *wrong* fit when:

- You need multiple topology backends.
- You need explicit message channels with per-channel parallelism.
- You are building research on distributed protocols.

For each of those, [Partisan](../reference/comparison-with-partisan.md)
is the better library.

## Next

- [Benefits](benefits.md) lists the trade-offs in more detail.
- [Introduction](introduction.md) is the longer narrative, ideal
  if you want to understand *why* each piece is shaped the way it
  is before reading the per-concept pages.
- [Getting started](getting-started.md) gets a two-node cluster
  running.
