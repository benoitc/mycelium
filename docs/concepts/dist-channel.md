# Dist channel

The dist channel is what carries Erlang distribution traffic
between two mycelium nodes. It replaces the default TCP carrier
with QUIC, slots in Ed25519 mutual authentication, and exposes
the discovery chain that lets nodes find each other without
EPMD.

This page covers the layer between Erlang's
`net_kernel`/`erts_dist_main` and the wire.

## Why QUIC

The mycelium proto_dist module sits on top of upstream
[`quic_dist`](https://github.com/benoitc/erlang_quic). QUIC was
chosen for three properties:

- **Encryption is mandatory.** Every byte of Erlang dist
  traffic rides on a TLS-protected connection. There is no
  unencrypted mode, accidental or otherwise.
- **One connection per peer.** A single QUIC connection
  multiplexes the Erlang dist control stream, any
  application-level streams (`mycelium_streams`), and the
  Ed25519 handshake streams. One UDP socket per node.
- **Connection migration.** A QUIC connection can rebind to a
  new local 4-tuple without losing keys or streams. See
  [connection migration](connection-migration.md).

The TLS certs are self-signed: there is no certificate authority
mycelium expects you to trust. Peer identity is established at
the [Ed25519 layer](authentication.md), not at the TLS layer.

## Boot

A mycelium node boots with three flags:

```bash
-proto_dist mycelium
-epmd_module mycelium_epmd
-start_epmd false
```

The first selects `mycelium_dist` as Erlang's distribution
module. The second tells `net_kernel` to use mycelium's
discovery shim instead of the stock EPMD daemon. The third
disables the daemon entirely; mycelium does not need it.

These three lines are the entire dist configuration. Everything
else, certificate paths, the auth callback, the discovery
chain, is projected into the underlying `quic.dist` app env
when the listener starts.

## What `mycelium_dist:listen/1` does

When `net_kernel` starts the listener, `mycelium_dist:listen/1`
runs four steps in order:

1. **Ensures TLS material.** If `data/quic/node.crt` and
   `data/quic/node.key` exist, they are used as is. Otherwise
   `mycelium_quic_cert` generates a self-signed pair.
2. **Loads the `quic` and `mycelium` apps.** Without this, the
   user's `sys.config` entries under `{quic, [{dist, _}]}` and
   `{mycelium, _}` are invisible to `application:get_env/3`.
3. **Projects defaults into `quic.dist`.** Sets
   `auth_callback => {mycelium_dist_auth_callback, authenticate}`,
   `discovery_module => mycelium_discovery`, and the cert/key
   paths. User-supplied values under `{quic, [{dist, _}]}`
   always win; this step only fills missing keys.
4. **Validates the projected config.** If
   `mycelium.auth_enabled = true` but the projected
   `auth_callback` is `undefined` (because the user explicitly
   nulled it), boot fails with `{mycelium_dist,
   auth_enabled_without_callback}`. Loud failure rather than
   silently shipping an unauthenticated cluster.

Then control passes to `quic_dist:listen/1`.

## The handshake

For an outgoing connection, `mycelium_dist:setup/5` is the
proto_dist callback. It opens the QUIC connection, runs the
authentication handshake on a pair of unidirectional QUIC
streams, then hands the dist control stream to the dist
controller.

```
1. quic:connect/4              QUIC TLS handshake (encrypted)
2. mycelium_dist_auth_stream   Ed25519 mutual auth (Hello,
                               Challenge, Response, Ok)
3. dist_util:handshake_*       standard Erlang dist handshake
                               (cookie, version negotiation)
```

Steps 1 and 3 are conventional. Step 2 is what mycelium adds.
See [authentication](authentication.md) for the full protocol.

## The discovery chain

When a peer atom `node@host` needs to be resolved to an IP and
port, mycelium runs through a *chain* of backends. The default
chain has three:

1. **Static**. Reads `{quic, [{dist, [{nodes, [...]}]}]}` from
   sys.config. Each entry is `{NodeAtom, {Host, Port}}`. The
   simplest backend; useful in docker-compose, in tests, and
   for any deployment with a fixed topology.
2. **File**. Reads `data/discovery/<node>.endpoint` files. Each
   file is the JSON-encoded `{host, port}` for one node. A
   shared volume across hosts effectively gives you a
   filesystem-backed registry. Useful on a single host where
   every node writes its own endpoint to the shared
   directory.
3. **DNS**. Resolves the `host` portion of the node atom via
   the system's resolver. Port comes from
   `application:get_env(quic, dist_port, _)`. Useful in
   environments with proper DNS plumbing.

The chain is configurable:

```erlang
{mycelium, [
    {discovery_backends, [
        mycelium_discovery_static,
        mycelium_discovery_file,
        mycelium_discovery_dns
    ]}
]}.
```

Lookups try each backend in order; first hit wins. Registration
(the path that publishes our own endpoint) fans out, so a
node's filesystem entry is visible to siblings regardless of
the lookup order.

You can add your own backend: implement the `quic_discovery`
behaviour and put it in the chain.

## On-demand dist channels

When `Pid ! Msg` targets a node that is *not* in the local
[active view](cluster-membership.md), OTP's `net_kernel`
auto-connect fires. The flow:

1. `net_kernel:connect_node(TargetNode)` is invoked
   implicitly.
2. `mycelium_dist:setup/5` opens a QUIC connection, runs the
   Ed25519 handshake, and starts the dist controller.
3. The message is delivered through the new channel.

From the application's point of view, nothing changed: it
called `Pid ! Msg` and the message arrived. The dist channel
is then a normal Erlang dist link; everything that works over
the default carrier works here too.

If the channel is then idle long enough, the dist GC reaps it
(see below).

## The idle dist GC

Without a reaper, on-demand channels would accumulate over
time as the application talks to more peers ad hoc. The dist
GC keeps the connection count bounded.

The reap predicate is conservative. A channel is eligible only
when **all** of these hold:

- The peer is not in the local HyParView active view.
- `quic_dist:list_streams/1` returns the empty list (no live
  application streams ride the channel).
- The channel is older than `dist_gc_min_age_ms` (default 5
  minutes).

A reaped channel is closed cleanly. If the application sends
to the same peer later, a fresh channel opens on demand.

The GC has **no enable/disable flag**. The
decoupled-from-active-view design relies on its presence; see
[features](../features.md) for the stability tier.
Tunables:

| Key | Default | Purpose |
|-----|---------|---------|
| `dist_gc_sweep_period_ms` | 60000 | Sweep cadence. |
| `dist_gc_min_age_ms` | 300000 | Minimum age before a channel may be reaped. |

## Bridge: from HyParView to net_kernel

`mycelium_bridge` is the small gen_server that translates
between HyParView events and `net_kernel` events. It used to
auto-bind every `nodeup` to the active view; that coupling is
gone (post-decoupling). It now keeps only the bookkeeping
needed for the failure handler to fire HyParView's
`peer_failed/2` when a dist channel drops.

## API and configuration

The proto_dist module has no public API; you select it with
`-proto_dist mycelium`. The relevant public functions:

```erlang
%% Inspect the configured local listen port.
mycelium_dist:listen_port() -> {ok, inet:port_number()} | undefined.

%% Validate a config snapshot without booting (used in tests).
mycelium_dist:validate_auth_config(QuicDistOpts) -> ok.

%% Project the defaults (used in tests).
mycelium_dist:project_defaults() -> ok.
```

Relevant sys.config keys (under `{mycelium, [...]}`):

| Key | Default | Purpose |
|-----|---------|---------|
| `listen_port` | 0 (auto) | UDP port for the listener. |
| `quic_cert_dir` | `data/quic` | Where the TLS material lives. |
| `discovery_backends` | (default chain) | Discovery backend modules in order. |
| `dist_cookie` | `mycelium` | Erlang dist cookie applied at app start. |
| `dist_gc_sweep_period_ms` | 60000 | Idle GC sweep cadence. |
| `dist_gc_min_age_ms` | 300000 | Minimum age before GC may reap. |

Under `{quic, [{dist, [...]}]}` (upstream `quic_dist`):

| Key | Purpose |
|-----|---------|
| `cert_file` | TLS certificate path. |
| `key_file` | TLS private-key path. |
| `auth_callback` | Set by mycelium; see [authentication](authentication.md). |
| `discovery_module` | Set by mycelium to `mycelium_discovery`. |
| `nodes` | Static discovery entries. |

## Related

- [Authentication](authentication.md) is the Ed25519 layer
  that runs between the QUIC TLS handshake and the Erlang
  dist handshake.
- [Cluster membership](cluster-membership.md) builds on top of
  the dist channel for gossip.
- [Streams](streams.md) reuses the same QUIC connection for
  application traffic outside the dist control stream.
- [Connection migration](connection-migration.md) covers
  re-binding an established QUIC connection to a new local
  network path.
- [Route through a relay](../how-to/route-through-relay.md)
  uses the per-node connect-options hook to send dist traffic
  through an external transport.
