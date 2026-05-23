# Security audit preparation

Working document for the external review of mycelium's crypto and
transport layers ahead of the first public 0.x release. Delete this
file once the audit is complete and findings are tracked in
`docs/features.md` or the CHANGELOG.

Audit owner: TBD.
Target release blocked on this audit: next 0.x minor (e.g. 0.2.0).

Note: an in-house review pass already landed remediations (TLS channel
binding for the handshake, no pre-auth atom minting, weak-config boot
guards, ECDSA P-256 cert, removal of the unused crypto module). See the
Security section of the CHANGELOG. The external audit should review those
fixes as implemented, not assume the prior state. In particular the
identity-binding "channel binding" question below is now implemented as a
TLS-cert-hash binding.

## In scope

The audit should focus on the layers between the OS UDP socket and
the Erlang process inbox of a peer:

| Module                          | Concern                                                                                       |
|---------------------------------|-----------------------------------------------------------------------------------------------|
| `mycelium_dist_auth`            | Ed25519 challenge/response, replay window, timestamp validation, trust-store interaction.     |
| `mycelium_dist_auth_stream`     | Wire framing for the auth handshake, length-prefix bounds, stream lifecycle.                  |
| `mycelium_dist_auth_callback`   | Bridge between `quic_dist`'s auth hook and the protocol.                                      |
| `mycelium_dist_keys`            | Trust-store on-disk format, file permissions, TOFU vs strict semantics.                       |
| `mycelium_dist_protocol`        | Encode/decode of `HELLO`/`CHALLENGE`/`RESPONSE`/`OK`/`FAIL` (v2); length bounds; no atom mint. |
| `mycelium_quic_cert`            | Self-signed ECDSA P-256 cert generation, key permissions.                                     |
| `mycelium_dist`                 | `proto_dist` shim, `application:get_env(quic, dist)` projection, env-override invariants.    |
| `mycelium_rotate`               | Cert/identity rotation, atomic swap, restore-on-failure semantics.                            |

## Out of scope

These are deliberately not in scope for this audit, but worth noting
in the review report:

- HyParView membership protocol (`mycelium_hyparview*`). DoS surface,
  but no confidentiality boundary; treated as a separate concern.
- CRDT layer (`mycelium_ormap`, `mycelium_hlc`). Data-plane.
- Service registry (`mycelium_registry*`). Rides over authenticated
  dist; no separate handshake.
- Discovery backends (`mycelium_discovery*`). Read-only resolvers,
  no authentication of their own.

## Threat model

Adversary capabilities assumed:

1. Off-path passive attacker on the UDP transport. Reads packets but
   cannot inject without spoofing source IP.
2. On-path active attacker. Can drop, reorder, inject, and replay
   packets. Cannot break QUIC TLS 1.3 with `public_key`-validated
   ephemeral keys.
3. Compromise of one peer's Ed25519 private key. Should not allow
   impersonation of other peers, only the compromised one. The
   trust-store entry pinning that key becomes the rollback unit.

Adversary capabilities NOT in the threat model:

- Local-machine attacker with read access to `data/keys/node.key`.
  At that point the node is compromised by definition.
- Side-channel attacks against `crypto:sign/4` or QUIC libcrypto.

## Identity binding

A peer presents two credentials:

- The QUIC TLS cert (`data/quic/node.crt`). Self-signed; identity is
  the public key, not a CN.
- The Ed25519 identity public key (`data/keys/node.pub`).

The auth handshake binds the two: after the TLS handshake, the peer
proves possession of the Ed25519 private key whose public key is
pinned (or TOFU-accepted) for that peer's node atom. A peer that
swaps its TLS cert but not its Ed25519 key is rejected.

Audit questions:

1. Does `mycelium_dist_auth_callback` close all paths to an
   `accepted` outcome that bypass the Ed25519 step?
2. Can a peer in `tofu` trust mode silently re-pin to a new key
   after disconnect/reconnect? (Expected: no, the trust store
   refuses to overwrite without operator action.)
3. Is the nonce in the `CHALLENGE` message bound to the QUIC
   connection's TLS exporter or just the auth stream? Trade-off
   between protocol simplicity and channel binding.

## Replay window

`auth_timestamp_window` (default 30s) caps how stale a `CHALLENGE`
timestamp may be. The handshake rejects messages outside the window.

Audit questions:

1. Is the clock used `erlang:system_time/1` (wall clock) or
   monotonic? Wall-clock drift on the peer can cause spurious
   rejects.
2. Does the responder's timestamp check use a constant-time
   comparison? (Not security-critical here, but worth confirming.)

## Cookie semantics

The Erlang dist cookie (`mycelium.dist_cookie`, default `mycelium`)
still gates `gen_server` calls between nodes. With Ed25519 auth
enabled it is redundant for transport-level security, but it is the
sole barrier when `auth_enabled = false`.

Audit questions:

1. Should `auth_enabled = false` carry a logger warning at boot? The
   defaults currently allow this. The release blocker is whether the
   default should change.
2. Is there any path where the cookie is sent before TLS? (Expected:
   no, all dist traffic rides over the QUIC stream.)

## Known limitations to call out in the report

- Self-signed certs; no cert pinning beyond the Ed25519 layer.
- No peer cert revocation. Rotating identity is the only revocation
  primitive, and it relies on operator action to re-pin.
- No forward secrecy beyond what QUIC's ephemeral key exchange
  provides. Long-term Ed25519 keys are used for identity, not key
  exchange.
- `mycelium_dist_auth` reads keys off disk per attempt rather than
  caching in memory. Trade-off against rotation simplicity.

## Materials for the audit

- This document.
- `docs/authentication.md` (operator-facing summary).
- `docs/internals.md` (architecture, including which gen_servers own
  which protocols).
- `docs/external-relay.md` (the adapter seam, in case the auditor
  wants to reason about middleboxes).
- Recent CT and EUnit logs (`rebar3 check`).
- Property tests under `test/*_prop_tests.erl`.

## Logistics

Recommended deliverables:

- A summary of findings categorised by severity.
- For each finding, the affected file/line range and a proposed
  remediation if obvious.
- A go/no-go on shipping the first public 0.x.

Once the report lands:

- Track findings as items in `docs/features.md` next to the relevant
  feature row.
- File CHANGELOG entries under the release that contains the
  remediation.
- Delete this preparation document.
