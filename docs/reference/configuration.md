# Configuration reference

Every key mycelium reads from `sys.config`, with default, type,
and one-line purpose. Keys live under `{mycelium, [...]}`.

For the underlying QUIC dist options (set under
`{quic, [{dist, [...]}]}`), see the
[upstream `quic_dist` documentation](https://github.com/benoitc/erlang_quic);
mycelium projects defaults into that block at listen time and
respects any user values.

## Network

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `listen_port` | `0` | `non_neg_integer()` | UDP port for the QUIC dist listener. `0` lets the OS choose. Pin in production. |
| `quic_cert_dir` | `"data/quic"` | `string()` | Where TLS material lives. `node.crt` and `node.key` are written under this path on first boot. |

## Membership (HyParView)

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `active_size` | `5` | `pos_integer()` | Maximum concurrent gossip peers. |
| `passive_size` | `30` | `pos_integer()` | Maximum known-but-disconnected peers held as warm spares. |
| `arwl` | `6` | `pos_integer()` | Active random walk length. TTL on FORWARD_JOIN messages during a node join. |
| `prwl` | `3` | `pos_integer()` | Passive random walk length. Threshold inside FORWARD_JOIN at which the receiving node prefers the passive view. |
| `shuffle_length` | `8` | `pos_integer()` | Peers exchanged per shuffle round. |
| `shuffle_period` | `10000` | `pos_integer()` | Milliseconds between shuffle rounds. |
| `max_fail_count` | `5` | `pos_integer()` | Consecutive failures before a peer moves from active to passive. |
| `base_backoff_ms` | `1000` | `pos_integer()` | Initial backoff after a failure. Doubles up to 5 minutes. |
| `passive_max_age_ms` | `300000` | `pos_integer()` | Maximum age before a passive entry is dropped. |
| `pending_timeout_ms` | `30000` | `pos_integer()` | Backstop timer for pending join/connect/neighbor entries when no `peer_connected`/`peer_failed` callback fires. |
| `contact_nodes` | `[]` | `[node()]` | Bootstrap nodes tried on application start. |

## Distribution

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `dist_cookie` | `mycelium` | `atom()` | Erlang dist cookie applied at app start. Override to a high-entropy secret in production. Boot warns when the default is in use, and refuses to start if `cookie_only_nodes` is non-empty while the cookie is still the default. |
| `dist_gc_sweep_period_ms` | `60000` | `pos_integer()` | Idle GC sweep cadence. |
| `dist_gc_min_age_ms` | `300000` | `pos_integer()` | Minimum age before the GC may reap an idle channel. |
| `discovery_backends` | (default chain) | `[module() \| {module(), term()}]` | Discovery backend modules in order. Default chain: `[mycelium_discovery_static, mycelium_discovery_file, mycelium_discovery_dns]`. |
| `discovery_dir` | `"data/discovery"` | `string()` | Directory the file-based discovery backend reads and writes. |

## Authentication

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `auth_enabled` | `true` | `boolean()` | Ed25519 challenge-response between peers, bound to the TLS channel. Disabling removes the only identity layer; the dist cookie becomes the sole gate over an unauthenticated TLS channel, and boot logs a warning. |
| `auth_trust_mode` | `tofu` | `tofu \| strict` | TOFU pins keys on first contact; strict requires every peer's key to be pre-provisioned. |
| `auth_key_dir` | `"data/keys"` | `string()` | Directory holding `node.pub`, `node.key`, and the `trusted/` pin store. |
| `auth_handshake_timeout` | `10000` | `pos_integer()` | Total budget for the Ed25519 round trip. |
| `auth_timestamp_window` | `30000` | `pos_integer()` | Acceptable peer wall-clock skew, in milliseconds. The responder's own duration check uses monotonic time and is unaffected by NTP steps. |
| `cookie_only_nodes` | `[]` | `[atom()]` | Patterns of node atoms exempt from Ed25519, gated by the dist cookie alone. Supports `*` wildcards on either side of `@`. Reduced assurance: such peers have no channel binding and no MITM protection, and boot warns once when one is accepted. Refused at boot if the cookie is still the default. |

## Routing and proxies

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `router_max_in_flight` | `256` | `pos_integer()` | Cap on concurrent overlay route-request handlers. Over-cap requests reply with `{error, overloaded}` and increment `mycelium.router.request_dropped`. |
| `proxy_cast_max_in_flight` | `32` | `pos_integer()` | Per-proxy cap on concurrent overlay-cast helpers. Over-cap casts are dropped and counted via `mycelium.service_proxy.cast_dropped`. |
| `route_cache_sweep_period_ms` | `60000` | `pos_integer()` | Periodic sweep of stale route-cache entries. |

## Placement (`mycelium_shard`)

These govern sharded placement and its lease-based live-node set. `ring_size` MUST be identical on every node, or nodes compute different rings and diverge; treat the lease timings as cluster-wide too.

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `ring_size` | `64` | `pos_integer()` | Number of ring partitions. Granularity of ownership events. Must match on every node. |
| `member_heartbeat_ms` | `2000` | `pos_integer()` | How often a node re-announces itself into the live-node set. |
| `member_ttl_ms` | `6000` | `pos_integer()` | Lease lifetime. A node drops out of the ring once `Now - last heartbeat` exceeds this. Keep well above `member_heartbeat_ms` plus expected clock skew. |
| `member_skew_ms` | `5000` | `non_neg_integer()` | Reject heartbeats whose timestamp is more than this far in the future, so a fast clock cannot pin a dead node. |

## Reminders (`mycelium_reminder`)

Durable reminders build on placement, so they also obey the placement keys above.

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `reminder_scan_ms` | `1000` | `pos_integer()` | Periodic safety sweep that re-arms reminders this node owns and missed, and re-arms far-future reminders as their fire time nears. |
| `reminder_tombstone_ttl_ms` | `3600000` | `non_neg_integer()` | Drop fire/cancel tombstones older than this so the replicated store stays bounded. Must exceed max gossip-propagation plus `member_ttl_ms`. |
| `reminder_data_dir` | `"data/reminders"` | `string()` | Per-node directory for the on-disk reminder store (WAL + snapshot). Reminders persist by default and recover on boot, so they survive a full-cluster restart. Give each node its own path. |

## Replicated maps (`mycelium_map`)

| Key | Default | Type | Purpose |
|-----|---------|------|---------|
| `replicated_maps` | `[]` | `[{atom(), map()}]` | Maps started on every node at boot. Each entry is `{Name, Opts}`. |
| `mycelium_map_scan_ms` | `1000` | `pos_integer()` | Default tombstone-GC sweep cadence for maps (overridable per map). |
| `mycelium_map_tombstone_ttl_ms` | `3600000` | `non_neg_integer()` | Default tombstone TTL for maps (overridable per map). |
| `mycelium_map_data_dir` | `"data/maps"` | `string()` | Per-node directory for maps started with `persist => true` (WAL + snapshot). |

## Examples

### Minimal development config

```erlang
[
    {mycelium, [
        {active_size, 5},
        {passive_size, 30},
        {listen_port, 9100},
        {auth_enabled, true},
        {auth_trust_mode, tofu}
    ]}
].
```

### Production cluster, strict mode, pinned ports

```erlang
[
    {mycelium, [
        {active_size, 5},
        {passive_size, 30},
        {listen_port, 9100},
        {auth_enabled, true},
        {auth_trust_mode, strict},
        {auth_key_dir, "/var/lib/mycelium/keys"},
        {dist_cookie, 'redacted-high-entropy-cookie'},
        {contact_nodes, ['seed1@host', 'seed2@host']},
        {discovery_backends, [mycelium_discovery_static]}
    ]},
    {quic, [
        {dist, [
            {nodes, [
                {'node1@host', {"node1.example.internal", 9100}},
                {'node2@host', {"node2.example.internal", 9100}},
                {'node3@host', {"node3.example.internal", 9100}}
            ]}
        ]}
    ]}
].
```

### Large cluster (200+ nodes)

```erlang
[
    {mycelium, [
        {active_size, 7},
        {passive_size, 60},
        {shuffle_period, 15000},
        {listen_port, 9100},
        {auth_enabled, true},
        {auth_trust_mode, strict}
    ]}
].
```

## Related

- [Run in production](../how-to/run-in-production.md) for
  sizing guidance and operational context.
- [Cluster membership](../concepts/cluster-membership.md) for
  the meaning of active/passive view parameters.
- [Authentication](../concepts/authentication.md) for the
  meaning of the trust-mode keys.
