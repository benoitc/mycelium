# Benefits and trade-offs

This page is a short, honest list. What you get with mycelium,
what you give up, and what is intentionally out of scope.

## What you get

### A bounded connection count

Default Erlang distribution is full mesh: every node opens a TCP
connection to every other node. The cost is `O(n²)` connections
across the cluster. With a few dozen nodes that is fine; past a
few hundred it starts to bite (kernel resources, monitoring
overhead, slow startup as joins ripple).

Mycelium keeps each node connected to a small, bounded set of
*gossip peers* (the HyParView active view, typically five). The
cluster as a whole stays fully addressable through OTP's
demand-driven auto-connect. When you send to a pid on a peer
outside the active view, a fresh QUIC channel opens on demand and
is later reaped if it goes idle. The result: connection count is
`O(active_size)`, not `O(n)`.

### Secure by default

The QUIC TLS layer encrypts everything on the wire. The Ed25519
mutual handshake then proves each peer's identity before any
Erlang traffic flows. There is no certificate authority to
operate; nodes self-sign their TLS material and pin each other's
Ed25519 public keys on first contact (TOFU) or via pre-shared
keys (strict mode).

The handshake is bound to the TLS channel (the signature covers a
hash of the server's TLS certificate), so it holds against an
active on-path attacker, not only a passive one: a relayed
handshake lands on a different certificate and fails to verify.

### Service discovery without a registry service

Names are cluster-wide. A `register_service/2` call replicates
through a CRDT and is observable on every peer within a fraction
of a second. `whereis_service/1` returns either a local pid or
the remote node + pid; you reach for it from any node and treat
the result as a normal pid.

### Standard Erlang patterns still work

`Pid ! Msg`, `gen_server:call/2`, `rpc:call/4`, `global`, links,
and monitors all work over the mycelium carrier exactly as they
do over the default TCP carrier. If you ever need to drop down to
raw distribution semantics, you do not have to rewrite any code
paths.

### Connection migration

A QUIC connection can rebind to a new local UDP 4-tuple without
losing keys or streams. `mycelium:migrate_peer/1,2` is the
trigger. Useful when a node's local network changes (laptop
switching from Wi-Fi to cellular; a CGNAT shuffle changes the
outbound IP; a tunnel reconnects).

### One UDP port per node

The full externally-visible network footprint is one UDP socket.
No EPMD. No second TCP control channel. The QUIC handshake
handles encryption, multiplexing, and migration on that single
socket.

## What you give up

### Topology flexibility

There is one membership protocol (HyParView). No full mesh, no
client-server, no custom topology backend. If you need any of
those, mycelium is not the right library; see
[Partisan](../reference/comparison-with-partisan.md).

### Per-channel parallelism

There are no explicit message channels with per-channel
parallelism. All Erlang dist traffic flows over one bidirectional
QUIC stream per peer, multiplexed at the QUIC layer. For most
workloads this matches or exceeds what you get from multiple TCP
connections; if you require explicit channel control, mycelium is
not the right shape.

### Tunable consistency for the registry

The service registry is eventually consistent. Adds and removes
commute and converge without coordination, but a registration on
node A is only visible from node B "soon" (typically under a
second). If you need linearizable naming, build that on top.

### NAT traversal, hole punching, relay autodiscovery

None of these ship in mycelium. If two peers cannot reach each
other directly, the answer is to wire an external relay through
the connect-time override hook
([route through a relay](../how-to/route-through-relay.md)).

## When the trade-offs are wrong

If you can answer "yes" to any of these, look elsewhere:

- "I need full-mesh semantics for a small, trusted cluster."
  Use the default Erlang dist.
- "I need a research toolkit for experimenting with overlay
  protocols." Use [Partisan](../reference/comparison-with-partisan.md).
- "I need linearizable cluster-wide naming." Build on top of
  mycelium or use a coordination service (consul, etcd).
- "I need a custom transport other than QUIC." Mycelium is QUIC
  only.

## Stability tiers

The public API is split into three tiers, tracked in
[docs/features.md](../features.md):

- **supported** — covered by deprecation and breaking-change
  notice across minor bumps.
- **beta** — likely stable, may change shape across minors.
- **experimental** — anything goes.

The cluster membership API, the service registry API, the via-`mycelium`
callbacks, and the Ed25519 trust store are `supported`. The
streams demuxer and connection-migration trigger are `beta`. New
features land as `experimental` first.

See [Versioning policy](../../README.md#versioning-policy) for
the semver contract while we are still pre-1.0.
