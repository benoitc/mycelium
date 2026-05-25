# Manage node keys

Every mycelium node has two on-disk credentials: an **Ed25519 identity**
keypair (the node's identity, used to authenticate peers) and a **QUIC TLS
certificate** (secures the transport). This guide is the task-first version:
create them, read a node's fingerprint, and share keys so peers trust each
other. For the protocol and trust-mode background, see
[Configure authentication](configure-authentication.md).

## Where keys live

```
data/
├── quic/
│   ├── node.crt          # TLS certificate (self-signed, auto-generated)
│   └── node.key          # TLS private key (chmod 0600)
└── keys/
    ├── node.pub          # Ed25519 public key (raw 32 bytes) - the identity
    ├── node.key          # Ed25519 private key (raw 32 bytes, chmod 0600)
    └── trusted/
        └── <node>.pub    # one pinned public key per trusted peer
```

The Ed25519 public key file is the raw 32 bytes, no PEM, no base64. The
directory is the `auth_key_dir` env (default `data/keys`); the cert dir is
`quic_cert_dir` (default `data/quic`).

## Create a node's keys

You normally do nothing: both credentials are generated on first boot if
missing. To create them ahead of time without starting a cluster, boot the
application once and stop:

```bash
erl -sname tmp -eval 'application:ensure_all_started(mycelium), init:stop().'
```

This writes `data/keys/node.{pub,key}` and `data/quic/node.{crt,key}`. To
pre-generate just the TLS material (for example to bake it into an image),
use the helper script:

```bash
_build/default/lib/mycelium/priv/bin/mycelium_gen_cert.sh --out-dir data/quic
```

Flags: `--out-dir`, `--cn`, `--days`, `--key-bits`, `--force` (idempotent
unless `--force`).

Persist `data/keys/` across restarts so a node keeps its identity. If you
lose it, the node generates a new identity and peers must re-trust it.

## Read a node's fingerprint

Share the fingerprint (a SHA-256 of the public key) to verify a node out of
band, the same way you would compare an SSH host key:

```erlang
{ok, Pub} = mycelium_dist_auth:get_public_key().
mycelium_dist_keys:fingerprint(Pub).        %% <<...32 bytes...>>
```

## Share keys with peers

How you share depends on the trust mode (`auth_trust_mode`).

### TOFU (default): nothing to do

In `tofu` mode the first handshake of each pair pins both sides
automatically. Just start the nodes. A pin never silently changes
afterwards: if a node later presents a different key for the same node
name, the connection is rejected (`key_mismatch`).

### Strict: distribute public keys ahead of time

In `strict` mode a peer is accepted only if its public key is already in
`data/keys/trusted/<node>.pub`. Provision before nodes meet.

1. Collect each node's public key, naming the file after its node atom:

   ```bash
   scp node1:data/keys/node.pub  /tmp/keys/node1@host1.pub
   scp node2:data/keys/node.pub  /tmp/keys/node2@host2.pub
   ```

2. Copy every public key into every node's trust store:

   ```bash
   for n in node1 node2; do scp /tmp/keys/*.pub $n:data/keys/trusted/; done
   ```

3. Set strict mode on each node:

   ```erlang
   {mycelium, [{auth_trust_mode, strict}]}
   ```

Adding a node later means provisioning *its* key on every existing node and
*theirs* on the new node.

### Share a key at runtime (no restart)

From an automation tool or a running node, pin a peer's key directly; the
file is written atomically and picked up without a restart:

```erlang
ok = mycelium_dist_keys:store_key('peer@host', PeerPubKey).   %% PeerPubKey :: <<_:256>>
```

## Verify what is trusted

```erlang
mycelium_dist_keys:list_trusted().              %% all pins
mycelium_dist_keys:lookup_pin('peer@host').     %% not_pinned | {pinned, <<32 bytes>>}
mycelium_dist_keys:get_trust_mode().            %% tofu | strict
```

To replace a pin (after a peer rotates its identity, below), delete the old
one first; TOFU never re-pins over a different key:

```erlang
ok = mycelium_dist_keys:delete_key('peer@host').
```

## Rotate keys

- **Identity** (Ed25519): `mycelium_rotate:rotate_identity()` takes effect on
  the next handshake, no restart. TOFU peers re-pin; strict peers need the
  new public key provisioned first.
- **Certificate** (TLS): `mycelium_rotate:rotate_cert()` needs a node
  restart to load the new cert; the identity is unaffected.

Both move the old material to a timestamped backup dir for rollback. See the
rotation runbooks in [Configure authentication](configure-authentication.md#key-rotation).

## See also

- [Configure authentication](configure-authentication.md) - trust modes, the
  handshake, cookie-only peers, the full key API and config reference.
- [Create an application](../tutorials/create-an-application.md) - where keys
  fit when you build an app from scratch.
