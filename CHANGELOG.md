# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Security
- The Ed25519 dist handshake is now bound to the QUIC TLS channel. Each
  signed message includes a 32-byte binding (SHA-256 of the server's TLS
  cert), so a relayed handshake lands on a different cert and fails to
  verify. This closes an active on-path MITM that could relay the
  handshake across two TLS legs in both trust modes. The wire protocol is
  bumped to v2; a v1 peer is rejected at HELLO (flag-day change, no
  deployed users pre-1.0).
- The dist handshake no longer mints an atom from an unauthenticated
  peer's claimed node name. The name is carried as a binary and the atom
  is created only after the signature verifies, closing an atom-table
  exhaustion DoS reachable before authentication.
- Boot now warns when the default dist cookie is in use, when
  `auth_enabled = false`, and when a cookie-only peer is accepted, and
  refuses to start when `cookie_only_nodes` is set while the cookie is
  still the default.
- The QUIC TLS certificate is now ECDSA P-256 (was RSA-2048), with
  `notBefore` backdated for peer clock skew.

### Removed
- `mycelium_crypto` (unused X25519/ChaCha20 layer) and the
  `AUTH_KEY_EXCHANGE` wire message. Distribution encryption is provided
  entirely by the QUIC TLS layer.

### Added
- Durable reminders (`mycelium_reminder`): `mycelium:remind/3`,
  `remind_after/3`, `cancel_reminder/1`, `subscribe_reminders/0,1`,
  `unsubscribe_reminders/1`. Replicated, fire-at-most-once timers that
  survive the node that armed them: the owner is `mycelium:place/1`, so
  a survivor takes over and fires after the owner dies. Timers are
  versioned hints (re-validated on fire), firing is tombstone-first, and
  delivery is `{mycelium_reminder, Key, Payload, Fence}` where `Fence`
  is a stable version stamp for idempotent dedup. Exactly once in steady
  state, best-effort under churn or a crash at the fire instant. Durability
  is via replication, not persistence. Config: `reminder_scan_ms`,
  `reminder_tombstone_ttl_ms`. Beta.
- Sharded service placement (`mycelium_shard`): `mycelium:place/1`,
  `owners/2`, `is_owner/1`, `partition/1`, `members/0`,
  `subscribe_shard/0,1`. Rendezvous (HRW) hashing over a replicated,
  lease-based live-node set (periodic heartbeats, wall-clock lease with
  a future-skew bound, not driven by `peer_down`), bucketed into
  `ring_size` partitions. Owners react to
  `{mycelium_shard, {acquired | released, Partition}}` on churn. Config:
  `ring_size` (must match cluster-wide), `member_heartbeat_ms`,
  `member_ttl_ms`, `member_skew_ms`. Beta.
- Cluster-wide singletons / leader election: `mycelium:lead/1,2`,
  `resign/1`, `leader/1`, `is_leader/1`, `fence/1`. A process
  campaigns for a named singleton and is notified with
  `{mycelium_leader, Name, {elected, Fence}}` / `revoked`; the cluster
  elects one leader (highest `priority`, ties to lowest node atom) and
  re-elects on `peer_up`/`peer_down`. Each term carries an HLC-based
  fencing token, strictly monotonic within a connected partition, for
  safe writes to shared resources. Module `mycelium_leader` (election +
  fencing) over a `mycelium_replica` instance. Beta.

### Changed
- Service registry and leader election now share one replication
  driver, `mycelium_replica` (gossiped OR-Map deltas, full-sync on
  `peer_up`, prune on `peer_down`). `mycelium_registry_sync` is removed;
  each feature runs its own `mycelium_replica` instance
  (`mycelium_registry_replica`, `mycelium_leader_replica`).
  `mycelium_hyparview` no longer calls the registry sync directly; the
  replica subscribes to the peer-event bus.

### Fixed
- Event subscriptions survive a restart of the source process. Sources
  keep subscribers in ephemeral state, and a source lives in a different
  supervision subtree from its subscribers, so a source crash previously
  dropped the feed silently. Subscribers now monitor the source and
  re-subscribe via `mycelium_source_monitor` (replicas additionally pull
  a full sync from peers afterwards). Covers the `mycelium_replica`
  instances, `mycelium_shard`, `mycelium_reminder`, `mycelium_streams`,
  `mycelium_service_proxy`, and `mycelium_plumtree`.
- `mycelium_plumtree` removes peers from both eager and lazy lists on
  `peer_down`. The handler clause silently dropped events because of
  an arity mismatch with the producer.
- `mycelium_service_proxy` subscribes to the service event bus and
  matches the real `service_down` shape; dead remote proxies are
  reaped instead of leaking.

### Changed
- `mycelium_router` and `mycelium_service_proxy` bound concurrent
  in-flight handlers. Over-cap route requests reply with
  `{error, overloaded}`; over-cap overlay casts are dropped. Both
  counters are exposed through `instrument` as
  `mycelium.router.request_dropped` and
  `mycelium.service_proxy.cast_dropped`. Tunables:
  `router_max_in_flight` (default 256) and
  `proxy_cast_max_in_flight` (default 32).
- Overlay relay (`mycelium_service_proxy:relay/4`) now carries a TTL
  and visited list, refusing hops that loop back to a visited node
  or exhaust the hop budget. Mismatched route caches can no longer
  ping-pong calls between nodes.
- `mycelium_router` runs a periodic route-cache sweep
  (`route_cache_sweep_period_ms`, default 60s). Stale entries are
  evicted even when no caller re-reads them.
- `mycelium_hyparview` arms a backstop timer
  (`pending_timeout_ms`, default 30s) for every pending
  join/connect/neighbor entry. A peer that goes silent during
  handshake no longer leaks a pending entry. Metric:
  `mycelium.hyparview.pending_timeout`.
- `mycelium_streams` caps the number of inbound streams parked
  awaiting tag-preamble completion at 64. Excess streams are reset
  with `mycelium.streams.preamble_dropped`.
- Peer node names are validated for format and length (255 byte cap,
  `name@host` shape, restricted charset) before the atom table is
  touched. `AUTH_HELLO` frames and on-disk trust-store filenames no
  longer mint atoms from peer-controlled bytes.

### Security
- Private-key and trust-store writes now go through a shared
  `mycelium_file:write_secure/2` helper that chmods the temp file
  to 0600 *before* any plaintext bytes are written, then renames
  atomically. Closes the previously-noted window where Ed25519 and
  QUIC TLS keys were briefly world-readable on disk.
- Cert serial numbers are drawn from `crypto:strong_rand_bytes/1`
  (127-bit positive integer) instead of `rand:uniform/1`.
- X.509 validity dates with year >= 2050 are now encoded as
  GeneralizedTime per RFC 5280. Long-lived self-signed certs no
  longer roll back to 1950 on the wire.
- `mycelium_dist_auth:load_keypair/1` verifies that the on-disk
  public key derives from the on-disk private key. A crash mid
  rotation that left a mismatched pair is now detected with
  `{error, keypair_mismatch}` rather than silently using
  inconsistent material.
- Auth handshake now enforces a wall deadline across all recv
  sites. A peer dribbling bytes can no longer extend the handshake
  beyond `auth_handshake_timeout` by restarting the timer on each
  chunk arrival.
- Replay-window check is now responder-side monotonic and
  cross-host wall-clock. The responder's window comparison uses
  `erlang:monotonic_time/1`, so an NTP step during a slow handshake
  cannot cause a spurious failure. A new `validate_peer_ts/1`
  rejects peer-supplied timestamps that are wildly skewed from
  local wall time (defense in depth against replays carrying old
  CHALLENGEs).
- `mycelium_dist:project_defaults/0` now refuses to boot when
  `mycelium.auth_enabled = true` but the projected `auth_callback`
  is `undefined` (a silent user override that previously shipped an
  unauthenticated cluster).
- Reject TOFU re-pin attempts. When a node is already pinned, the
  handshake refuses any peer presenting a different Ed25519 key,
  regardless of trust mode. Both the server side and the client side
  check the pin before continuing.
- Client requires the dialed peer to be in `cookie_only_nodes` before
  accepting an `AUTH_OK` short-circuit. A rogue server reachable
  through the discovery chain can no longer skip the Ed25519
  exchange.
- `mycelium.auth_enabled` defaults to `true`. Nodes that ran with the
  setting unset over `-proto_dist mycelium` were accepting
  unauthenticated peers; they now refuse them.
- Trust-store pins are written atomically (`.tmp` plus rename). A
  crash mid-write no longer leaves a truncated pin that silently
  drops the trust relation at the next boot.

### Removed
- Legacy socket-based dist auth handshake in `mycelium_dist_auth`.
  The QUIC stream handshake in `mycelium_dist_auth_stream` is the
  only path; pure helpers (key I/O, challenge build/verify,
  `cookie_only_nodes` matching) remain.

### Added
- Idle dist-channel GC (`mycelium_dist_gc`). Always-on reaper that
  drops dist channels which are not part of the HyParView active view,
  carry no live `mycelium_streams` user stream, and have aged past
  `dist_gc_min_age_ms`. Sweep period and min-age are tunable; the GC
  itself has no enable/disable flag.
- `mycelium_metrics` emits counters and histograms through the
  `instrument` library at HyParView, dist auth, plumtree, GC and
  migrate seams. Cached lazily in `persistent_term`; emit sites stay
  off the hot path.
- `mycelium_rotate:rotate_cert/0,1` and `rotate_identity/0,1` for
  QUIC TLS material and Ed25519 identity keys. Atomic backup under
  `<dir>/backups/<UTC-timestamp>/`; identity rotation takes effect on
  the next handshake, cert rotation requires a node restart.
- Property-based tests (PropEr) for the OR-Map CRDT laws, HLC
  monotonicity and binary round-trip, dist-protocol encode/decode
  round-trip plus fuzz survival, and the `mycelium_streams` demuxer
  under random fragmentation.
- Soak suite gated on `MYCELIUM_CT_SOAK=1`. `broadcast_burst` drives
  a burst of plumtree broadcasts across a 5-node cluster and asserts
  every subscriber receives the marker.
- Bench harness: `bench/run.sh` runs `mycelium_sync_bench` and emits
  `bench/results.json`; `bench/compare.sh` diffs against
  `bench/baseline.json` and fails on regression. Soft CI gate
  (`continue-on-error`) until hardware variance settles.
- `docs/features.md` catalogs every public feature with a stability
  tier (`supported`, `beta`, `experimental`) and CT/EUnit coverage.
- `docs/observability.md`, `docs/troubleshooting.md`,
  `docs/deployment.md`.
- README "Versioning policy" section formalising the 0.x semver
  contract.

### Changed
- Dist channels decoupled from HyParView active view. `Pid ! Msg` works
  between any cluster nodes; OTP's demand-driven auto-connect resolves
  through the mycelium discovery chain. HyParView active view tracks
  only the bounded gossip topology.
- `-proto_dist mycelium` ships as the transparent boot shim over
  upstream `quic_dist`; three-arg vm.args replaces the prior init-arg
  dance.
- HyParView's `handle_join/2` now emits `peer_up` for newly accepted
  peers. The broadcast tree previously missed nodes that joined
  through this code path.
- `mycelium_path_stats` extracts the QUIC conn pid defensively. The
  fast path still reads element 2 from the dist controller state;
  the fallback scans the tuple for a pid that answers
  `quic:get_path_stats/1`.
- Every public export in `src/mycelium.erl` carries a `Stability:`
  tag matching `docs/features.md`.

### Removed
- Multi-hop circuits (`mycelium:circuit_*` API and the
  `mycelium_circuit_*` modules). Their stated use case ("reach a node
  outside the active view") is now covered by raw dist; the
  byte-perfect FRAME_RESUME property has no other consumer in scope.
- `mycelium_router:find_path/1,2` and related path-cache internals
  (only consumed by circuits). Service overlay routing
  (`find_route/1`, `find_service/1`) stays.

## [0.1.0] - 2026-05-03

First public release.

### Membership and replication
- HyParView partial-view membership (active/passive views, ARWL/PRWL,
  shuffle, neighbor swap, age-based passive cleanup, churn handling).
- Plumtree epidemic broadcast tree (eager/lazy push, ihave/graft/prune,
  self-healing).
- Service registry on an Observed-Remove Map CRDT, replicated through
  Plumtree, with overlay-routed `whereis_service`, local-pid proxies
  for remote services, and `peer_up`/`peer_down` event subscription.
- Hybrid Logical Clocks for causally-ordered timestamps used by the
  CRDT and routing layers.

### Distribution carrier
- Runs on upstream `quic_dist` (`-proto_dist quic`); one QUIC
  connection per peer carries the Erlang dist channel.
- EPMD-less by default via upstream's `quic_epmd` lookup module.
- Composing discovery chain (`mycelium_discovery`) with three
  built-in backends: static config (`mycelium_discovery_static`),
  on-disk file registry for local auto-discovery
  (`mycelium_discovery_file`), and DNS host fallback
  (`mycelium_discovery_dns`).
- Plugs into upstream's `auth_callback` extension point via
  `mycelium_dist_auth_callback`.
- Ed25519 distribution authentication: TOFU and strict trust modes,
  fingerprint-keyed trust store on disk, `cookie_only_nodes`
  whitelist for c-nodes that can't speak the auth protocol.
- `mycelium_dist_keys:fingerprint/1` SHA-256 helper for diagnostics.

### Streams and circuits
- Tagged user-stream multiplex (`mycelium_streams`): single acceptor
  per node, demultiplexes by `<<TagLen:8, Tag/binary>>` preamble;
  reserved tag `<<"mycelium:circuit">>`, free tag space for apps.
- Multi-hop circuits (`mycelium_circuit`): stream-shaped channels
  between cluster nodes that aren't in each other's active view,
  spliced at intermediate hops with stateless relays.
- Byte-perfect migration across hop failure: 48-bit per-direction
  frame sequence numbers, cumulative ACKs, symmetric `FRAME_RESUME`
  exchange that prunes unacked buffers and replays preserving
  DATA/FIN frame type. No data loss on relay disappearance.

### Observability and routing
- RTT-aware path selection (`mycelium_router:find_path/1,2`) using
  `quic:get_path_stats/1` (upstream) for srtt-based hop ranking.
- `mycelium_path_stats` wrapper exposing `summary/1`, `srtt/1`, and
  `connection/1` (resolves a peer node to the underlying QUIC pid).

### Connection migration
- `mycelium:migrate_peer/1,2` triggers RFC 9000 §9 path migration on
  the per-peer dist channel; rebinds to a new local 4-tuple without
  rekey or HyParView churn. Custom triggers (NIC-change, path-quality
  thresholds, etc.) live in app code via the public events + stats +
  this API; recipe in `docs/migration.md`.

### Pluggable transport overrides
- `quic_dist:set_connect_options/2` is the seam for routing a peer
  through an out-of-tree relay/tunnel adapter (MASQUE, WireGuard,
  SSH ProxyCommand, etc.). Documented in `docs/external-relay.md`.

### Tooling
- `priv/bin/mycelium_call.sh` — `erl_call`-style one-shot RPC helper
  that boots a hidden probe with full Ed25519 identity and runs
  `rpc:call/5` against a live mycelium node. Available to anything
  depending on mycelium via `_build/default/lib/mycelium/priv/bin/`.
- `priv/bin/mycelium_gen_cert.sh` — self-signed cert generator for
  the QUIC dist channel (RSA 2048 by default; `--cn`, `--days`,
  `--key-bits`, `--force` flags; idempotent).

### Tests
- Eunit (72 cases) covering circuit framing, reliability link, stream
  multiplex, path stats, router path selection, dist-key fingerprint,
  and migrate_peer wrapper.
- Common Test (177 cases) covering churn, registry, plumtree, hyparview,
  failure handling, dist-auth, and the basic two/three-node cluster
  mechanics suites.
- `mycelium_circuit_multinode_SUITE` — gated four-node diamond
  topology integration suite driven via upstream `quic_call.sh`
  (`MYCELIUM_CT_QUIC_MULTINODE=1`).
- GitHub Actions matrix CI on OTP 27 and 28: compile, xref, dialyzer,
  eunit, ct, plus the multi-node job.
- Dialyzer- and xref-clean.

### Documentation
- README, `docs/getting-started.md`, `docs/tutorial.md`,
  `docs/internals.md`, `docs/circuits.md`, `docs/migration.md`,
  `docs/authentication.md`, `docs/external-relay.md`,
  `docs/testing.md`, `docs/partisan-comparison.md`.
- LICENSE (Apache-2.0), SECURITY.md.
