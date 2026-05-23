# Authentication

Mycelium authenticates every dist connection with Ed25519
mutual signatures. The authentication runs after the QUIC TLS
handshake and before Erlang's dist handshake; two peers that
have not proved possession of the right private key never
reach the cookie exchange.

This page is the **concept**: what the protocol does and why
the shape is the way it is. For operational tasks
(provisioning, rotation, cookie-only peers), see
[configure authentication](../how-to/configure-authentication.md).

## Why a second identity layer

The QUIC TLS handshake gives us an encrypted transport. The
certificate is self-signed: there is no authority mycelium
expects you to trust, and a fresh node generates its own TLS
material on first boot. So the TLS layer establishes a secure
channel, but it does not say *who* is on the other side.

The Ed25519 layer adds that. Each node has a long-lived
Ed25519 keypair stored on disk. The public key is the node's
identity; the private key is what the node uses to prove that
identity.

Across cert rotation, across reboots, across TLS material
regeneration, the Ed25519 keypair persists. Peers trust each
other by pinning each other's public keys under the node atom.

The cluster also retains the standard Erlang dist cookie. With
Ed25519 enabled (the default), the cookie is
defense-in-depth; with Ed25519 disabled, it is the only
authentication that remains.

## The handshake

When two nodes connect, immediately after the QUIC TLS
handshake finishes, each side opens one unidirectional QUIC
stream toward the other. The four streams (two per side, one
for sending, one for receiving) carry the auth protocol:

```
Node A (client)                          Node B (server)
      |                                        |
      |--HELLO(node_A, pubkey_A)-------------->|
      |<-HELLO(node_B, pubkey_B)---------------|
      |                                        |
      |--CHALLENGE(nonce_A, wall_ts_A)-------->|
      |<-CHALLENGE(nonce_B, wall_ts_B)---------|
      |                                        |
      |--RESPONSE(sig over nonce_B,ts_B,pub_A)>|
      |<-RESPONSE(sig over nonce_A,ts_A,pub_B)-|
      |                                        |
      |--OK----------------------------------->|
      |<-OK------------------------------------|
                    |
                    v
           Erlang dist handshake
```

Two design points are worth knowing in advance.

**The signed message is bound to the responder's identity and to
the TLS channel.** A signature is taken over
`<<nonce, timestamp, responder_pubkey, channel_binding>>`, where
`channel_binding` is the SHA-256 of the server's QUIC TLS
certificate. The responder pubkey means a signature from peer X
cannot be replayed against peer Y. The channel binding means a
signature cannot be relayed across a different TLS connection:
the QUIC certs are self-signed and unvalidated, so an active
on-path attacker could otherwise terminate two TLS legs and relay
the handshake frames verbatim. The client derives the binding
from the server cert it observed (`quic:peercert/1`); the server
from its own listener cert. Under a relay these differ, so
verification fails. A stolen Ed25519 key alone, or a relayed
pinned key, does not let an attacker sit in the middle.

**The window check uses monotonic time.** The wall timestamp
goes on the wire so peers can sanity-check each other's clocks,
but the responder's own duration check uses
`erlang:monotonic_time/1`. An NTP step during the handshake
will not cause a spurious failure.

## Trust modes

The interesting decision happens on the server side after
HELLO: "have I seen this peer before, and does the key it just
presented match the one I have on file?"

| Scenario                    | TOFU mode                             | Strict mode                |
|-----------------------------|---------------------------------------|----------------------------|
| No pin recorded             | Accept, pin the presented key         | Reject (`untrusted_key`)   |
| Pin matches presented key   | Accept                                | Accept                     |
| Pin differs from presented  | Reject (`key_mismatch`)               | Reject (`key_mismatch`)    |

The "pin differs" case is rejected **in both modes**. TOFU does
not silently re-pin, no matter what mode the peer is in. Once
a peer is recorded, you must remove the pin explicitly before
a different key for the same node atom can be accepted.

The trade-off:

- **TOFU** is zero-configuration. Joining a node is one
  command, the first contact pins both sides. Operational
  simplicity. The first contact is theoretically vulnerable to
  an active attacker who outraces the legitimate peer; in
  controlled environments that window is acceptable.
- **Strict** never auto-pins. Every peer must already have its
  public key on disk under
  `data/keys/trusted/<node-atom>.pub`. No first-contact
  window. The price is that you must provision keys before
  nodes can connect.

The same TOFU model powers SSH's `known_hosts`.

## What lives on disk

```
data/keys/
├── node.pub                      # 32 bytes, the node's Ed25519 public key
├── node.key                      # 32 bytes, the private key (chmod 0600)
└── trusted/
    ├── node1@host1.pub           # 32 bytes, raw public key
    ├── node2@host2.pub
    └── ...
```

The file format is the raw 32-byte public key. No PEM headers,
no base64. This matches the on-the-wire shape; the cluster
does not have a separate "import" step beyond placing the
file.

A few invariants the runtime preserves:

- The private key file is created with mode 0600. The helper
  that writes secret material (`mycelium_file:write_secure/2`)
  chmods the temporary file *before* any plaintext bytes are
  written, so a co-tenant cannot race the write.
- Writes go through a temp file and rename, so a crash
  mid-write never leaves a half-written pin that the next boot
  would silently drop.
- The keypair is consistency-checked on load: the public key
  on disk must derive from the private key on disk. If they
  disagree (because a previous rotation crashed mid-flight),
  the load returns `{error, keypair_mismatch}` and the node
  refuses to start until the operator decides which side is
  correct.

For provisioning, rotation, and the cookie-only escape hatch,
see [configure authentication](../how-to/configure-authentication.md).

## Cookie-only peers

A small whitelist of node-atom patterns can bypass the Ed25519
handshake on the strength of the Erlang dist cookie alone:

```erlang
{mycelium, [
    {cookie_only_nodes, ['probe@*', 'monitor@trusted.example']}
]}.
```

This is meant for one specific case: short-lived probes (an
`erl_call`-style helper, a monitoring agent) that cannot carry
an Ed25519 keypair and that you trust on cookie grounds.

The check is **symmetric**. If the cluster runs without this
whitelist on the *client* side, the client refuses an
unsolicited `AUTH_OK` from a server even if the server thinks
the client is in its own cookie_only list. Both ends must list
the peer for the short-circuit to apply.

## Configuration

```erlang
{mycelium, [
    %% Master switch. Defaults to true. Setting this to false in
    %% production removes the Ed25519 layer; the dist cookie
    %% becomes the only authentication. Do not do this.
    {auth_enabled, true},

    %% Trust mode: tofu | strict.
    {auth_trust_mode, tofu},

    %% Directory holding the local keypair and the trust store.
    {auth_key_dir, "data/keys"},

    %% Handshake budget. The full Ed25519 round trip must
    %% complete within this many milliseconds; otherwise the
    %% connection is closed.
    {auth_handshake_timeout, 10000},

    %% Acceptable skew (per direction) between the local clock
    %% and the peer's wall-clock timestamps in the CHALLENGE.
    %% The local handshake duration check uses monotonic time;
    %% this controls the cross-host sanity check.
    {auth_timestamp_window, 30000},

    %% Short-circuit whitelist. Each entry is a node-atom
    %% pattern with optional `*` wildcards.
    {cookie_only_nodes, []}
]}.
```

## API

The relevant entry points. The public ones from `mycelium.erl`
are minimal because most authentication is automatic; the
inspection and provisioning lives in `mycelium_dist_keys` and
`mycelium_dist_auth`.

```erlang
%% Inspect or pin keys.
mycelium_dist_keys:store_key(Node, PubKey) -> ok.
mycelium_dist_keys:store_key_if_new(Node, PubKey) -> ok.
mycelium_dist_keys:lookup_pin(Node) -> not_pinned | {pinned, PubKey}.
mycelium_dist_keys:delete_key(Node) -> ok.
mycelium_dist_keys:list_trusted() -> [#peer_key{}].
mycelium_dist_keys:set_trust_mode(tofu | strict) -> ok.
mycelium_dist_keys:get_trust_mode() -> tofu | strict.
mycelium_dist_keys:fingerprint(PubKey) -> binary().  %% SHA-256

%% Identity.
mycelium_dist_auth:ensure_keypair() -> ok.
mycelium_dist_auth:get_public_key() -> {ok, binary()}.
mycelium_dist_auth:is_cookie_only_allowed(Node) -> boolean().

%% Rotation.
mycelium_rotate:rotate_identity() -> {ok, Info}.
mycelium_rotate:rotate_cert() -> {ok, Info}.
```

## Related

- [Configure authentication](../how-to/configure-authentication.md)
  is the operational guide (TOFU vs strict, provisioning,
  rotation, cookie-only).
- [Dist channel](dist-channel.md) explains where the Ed25519
  handshake fits in the larger boot flow.
- [Run in production](../how-to/run-in-production.md) covers
  ports, secrets, and the operational checklist that includes
  authentication.
