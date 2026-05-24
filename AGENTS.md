# Agents

Instructions for AI coding agents working on this project.

## Project Overview

Mycelium is a peer-to-peer distribution layer for Erlang/OTP: HyParView
membership, a QUIC + Ed25519 secure transport (`-proto_dist mycelium`), and a
suite of CRDT-backed cluster services (service registry, leader election,
sharded placement, durable reminders, replicated maps). Requires Erlang/OTP
27+ and rebar3. Depends on `hlc`, `quic`, and `instrument`.

## Required Checks

Every change must be formatted and pass all checks before committing:

```bash
rebar3 fmt                  # Auto-format (always run first)
rebar3 compile              # Must compile cleanly (warnings_as_errors)
rebar3 lint                 # Elvis linter must pass
rebar3 xref                 # Cross-reference analysis must pass
rebar3 dialyzer             # Type checking must pass
rebar3 eunit                # Unit + property tests must pass
rebar3 ct                   # Common Test suites must pass
```

`rebar3 check` is an alias for `xref, dialyzer, eunit, ct`.

## Build & Development Commands

```bash
rebar3 compile                                  # Build
rebar3 eunit                                    # All EUnit tests (incl. PropEr)
rebar3 eunit --module=mycelium_ormap_prop_tests # One test module
rebar3 ct                                       # All Common Test suites
rebar3 ct --name ct@127.0.0.1 --suite=test/mycelium_reminder_e2e_SUITE  # one e2e suite
rebar3 lint                                     # Elvis linter
rebar3 fmt --check                              # Check formatting (erlfmt)
rebar3 fmt                                       # Auto-format code
rebar3 dialyzer                                 # Type checking
rebar3 xref                                     # Cross-reference analysis
rebar3 ex_doc                                   # Generate docs
```

Multi-node e2e suites need a distributed node, so pass
`--name ct@127.0.0.1`. They spawn real BEAM peers under `-proto_dist mycelium`.

## Architecture

### Module Layers

**Public API:** `mycelium.erl` (the facade: membership, registry, leader,
shard, reminders, maps, streams, migration).

**Membership and gossip:** `mycelium_hyparview.erl` (bounded active/passive
views), `mycelium_hyparview_shuffle.erl`, `mycelium_hyparview_events.erl`
(peer-event bus), `mycelium_plumtree.erl` (epidemic broadcast).

**Distribution transport:** `mycelium_dist.erl` (the `-proto_dist` carrier over
`quic_dist`), `mycelium_dist_auth.erl` / `mycelium_dist_auth_stream.erl`
(Ed25519 mutual handshake, TLS channel binding), `mycelium_dist_keys.erl`,
`mycelium_quic_cert.erl`, `mycelium_dist_gc.erl` (idle-channel GC),
`mycelium_epmd.erl`, `mycelium_discovery*.erl` (discovery backends).

**Replicated services (all ride `mycelium_replica`):** `mycelium_registry.erl`,
`mycelium_leader.erl`, `mycelium_shard.erl` (rendezvous-hash placement),
`mycelium_reminder.erl` (durable timers), `mycelium_map.erl` (public
replicated map).

**CRDT, time, persistence:** `mycelium_ormap.erl` (LWW OR-Map),
`mycelium_hlc.erl` (hybrid logical clock), `mycelium_crdt_wire.erl` (safe
gossip ingest), `mycelium_replica.erl` (gossip/CRDT driver behaviour),
`mycelium_replica_log.erl` (WAL + snapshot disk persistence).

**Other:** `mycelium_streams.erl` (tagged user streams), `mycelium_router.erl`,
`mycelium_service_proxy.erl`, `mycelium_bridge.erl`, `mycelium_rotate.erl`
(key/cert rotation), `mycelium_source_monitor.erl` (subscribe resilience).

### Key Files

- `include/mycelium.hrl` — records and shared protocol types.
- `src/mycelium.app.src` and `config/sys.config` — app env and defaults.
- `docs/features.md` — feature matrix with stability tiers and test coverage.
- `docs/` — concept, how-to, reference, and tutorial pages (wired into ex_doc
  via `rebar.config`).

### Replication model

`mycelium_replica` drives a gossiped OR-Map: it broadcasts add/remove deltas,
routes incoming deltas to the owner's merge callback, full-syncs on `peer_up`,
seeds from the active view on start, and prunes on `peer_down`. The owner
gen_server holds the OR-Map; reads are served from an ETS projection. Persisted
consumers (reminders always, maps via `persist => true`) back the OR-Map with
`mycelium_replica_log`. Everything is eventually consistent (AP, no consensus).

### Test Organization

- `test/mycelium_*_tests.erl` — EUnit (unit). PropEr suites are
  `test/*_prop_tests.erl`, run under `rebar3 eunit`.
- `test/mycelium_*_SUITE.erl` — Common Test (single-node logic).
- `test/mycelium_*_e2e_SUITE.erl` — multi-node Common Test; spawn real peers
  under `-proto_dist mycelium`, need `--name ct@127.0.0.1`.
- `test/mycelium_docker_auth_SUITE.erl` is Docker-only (skipped otherwise);
  `test/mycelium_soak_SUITE.erl` is gated behind `MYCELIUM_CT_SOAK=1`.

## Linting and Formatting Notes

- `erlfmt` config and the Elvis rules both live in `rebar.config` (under the
  `erlfmt` and `elvis` keys). Run `rebar3 fmt` before every commit.
- Elvis runs the `erl_files` ruleset with several rules disabled and per-module
  ignores for legitimate existing patterns (the `mycelium` facade is an
  intentional god module; `mycelium_dist`/`mycelium_file` use `catch`;
  validators and discovery backends make dynamic calls; shared types live in
  the `.hrl`). Add a per-module ignore there rather than refactoring tested
  code, matching the existing style.
- Atom naming regex allows the `_SUITE` suffix; max line length is 120.

## Conventions

- Commit messages and PRs are concise; no "Generated by"/"Co-Authored-By"
  trailers, no test-plan section in PRs, and do not use the word
  "comprehensive".
- `mycelium` stays in 0.x; a minor bump may break documented APIs. New features
  ship with concept + how-to + reference docs wired into ex_doc and a row in
  `docs/features.md`. New distributed capabilities need a real multi-node e2e
  test.
