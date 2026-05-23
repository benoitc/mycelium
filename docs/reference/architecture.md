# Internals

This document describes how mycelium works underneath the API. It
is meant for two readers: Erlang developers who have followed the
[getting started](../overview/getting-started.md) guide and the
[practice handbook](../tutorials/distributed-chat.md), and contributors planning a change
to the protocols.

We move from the layers closest to the application code down to
the dist carrier and the credentials, then describe the few
in-tree services that keep the whole picture together.

## Layered view

```
+----------------------------------------------------------+
|                        Application                       |
|   mycelium:register_service/2  whereis_service  Pid ! Msg|
+------------------------+---------------------------------+
                         |
+------------------------+---------------------------------+
|                       mycelium.erl                       |
|   (public API: thin wrappers over the layers below)      |
+----+--------+--------------+---------------+-------------+
     |        |              |               |
     v        v              v               v
+--------+ +--------+ +---------------+ +-----------+
|HyParView| |Registry| |   Plumtree    | |  Streams  |
|Membership|(OR-Map)| |Gossip broadcast| |  multiplex|
+--------+ +--------+ +---------------+ +-----------+
                         |
                         v
                +------------------+
                |  mycelium_dist   |
                |  (proto_dist     |
                |   shim + cert    |
                |   + defaults)    |
                +--------+---------+
                         |
                +--------+---------+
                |    quic_dist     |   <-- upstream
                |   (auth callback,|
                |   discovery, port|
                |   pinning, etc.) |
                +--------+---------+
                         |
                +--------+---------+
                |   erlang_quic    |   <-- transport
                +------------------+
```

The application only ever calls into `mycelium.erl`. Every other
module in the diagram is internal; you are free to read its code,
but you should not depend on its names from outside.

## Supervision tree

A running mycelium node has the following supervision shape:

```
mycelium_sup  (one_for_one)
|
+- mycelium_hlc           hybrid logical clock for CRDT timestamps
|
+- mycelium_dist_keys     trust store (ETS) for Ed25519 pins
|
+- mycelium_hyparview_sup (rest_for_one)
|  +- mycelium_hyparview          HyParView state machine
|  +- mycelium_hyparview_events   subscriber bus for peer_up/peer_down
|  +- mycelium_hyparview_shuffle  periodic view exchange timer
|  +- mycelium_hyparview_cleanup  passive view aging timer
|
+- mycelium_plumtree_sup
|  +- mycelium_plumtree           epidemic broadcast tree
|
+- mycelium_registry_sup  (rest_for_one)
|  +- mycelium_registry           local registry + CRDT
|  +- mycelium_registry_replica   replication driver (mycelium_replica)
|
+- mycelium_leader               cluster-wide singleton election
+- mycelium_leader_replica       election replication (mycelium_replica)
|
+- mycelium_shard                sharded placement (HRW ring)
+- mycelium_members_replica      lease-based live-node set (mycelium_replica)
+- mycelium_reminder             durable reminders (owner-fires-once)
+- mycelium_reminder_replica     reminder store (mycelium_replica)
|
+- mycelium_proxy_sup     (simple_one_for_one)
|  +- mycelium_service_proxy * N  per-name remote proxies
|
+- mycelium_router               service overlay routing cache
+- mycelium_streams              tagged user-stream demultiplexer
+- mycelium_bridge               dist connection bookkeeping
+- mycelium_dist_gc              idle dist channel reaper
```

The vertical structure is not random. `mycelium_hlc` and
`mycelium_dist_keys` start first because every other subsystem
depends on monotonic timestamps and on the trust store being
available. The HyParView subtree is restart-as-a-block
(`rest_for_one`): if the state machine crashes, the event bus,
shuffle timer, and cleanup timer all restart together, which keeps
the cluster's view of the node coherent.

## HyParView: how membership stays bounded

HyParView is a partial-view membership protocol. Each node holds
two sets:

- The **active view**, a small bounded set (default 5) of peers
  this node currently exchanges gossip with. Active links are
  symmetric: if A has B in its active view, B has A in its active
  view.
- The **passive view**, a larger bounded cache (default 30) of
  known peers that are not currently active. Members of the
  passive view are warm spares used when an active link drops.

The key property is that the active view does **not** need to grow
with the cluster. A node with thousands of peers in its passive
view still keeps only five active links; messages reach the rest
of the cluster by being forwarded along the active links of other
nodes.

Read this graph from node A. The active view is the maintenance
topology. The passive view is a reserve of known peers. Neither is
the full set of nodes your Erlang code may eventually talk to.

![HyParView active view: node A keeps a small set of active gossip peers and a passive cache of known peers.](diagrams/active-view.png)

### Joining the cluster

When a new node joins, it sends a `JOIN` message to one *contact
node* it knows about (either explicitly through `mycelium:join/1`
or implicitly through the `contact_nodes` config key):

```
NewNode ----JOIN----> ContactNode
                            |
                            +--- adds NewNode to its active view
                            |
                            +--- sends FORWARD_JOIN(TTL=ARWL) to a
                                 random active peer (and so on, until
                                 TTL=0)
```

Forwarding has two purposes. First, it spreads knowledge of the
new node across the cluster without flooding. Second, when TTL
reaches zero, the receiving node adds the new node to its own
passive view, which becomes a warm spare for future use.

The TTL (`arwl`, default 6) controls how widely the new node
propagates. A smaller value means a more local join; a larger
value spreads farther but costs more messages.

### Failure handling

A node failure is detected through the dist channel:
`net_kernel` reports a `nodedown` event, which mycelium translates
into a HyParView failure. The protocol then:

1. Moves the failed peer from active to a transient "failed"
   state with an exponential backoff timer.
2. After `max_fail_count` consecutive failures (default 5), moves
   the peer to the passive view.
3. Picks a replacement from the passive view and sends a
   `NEIGHBOR` request. If the candidate accepts, the active view
   is back to its target size.

Backoff prevents reconnection storms during network partitions:
if half the cluster becomes unreachable at once, each surviving
node retries its neighbours on staggered timers rather than all
at once.

### Shuffle: keeping passive views fresh

Periodically (every `shuffle_period` ms, default 10s), each node
picks a random active peer and exchanges a small sample of its
known peers. This is how new members reach corners of the cluster
that did not see the original `FORWARD_JOIN`, and how the passive
view stays large enough that there is always a spare to replace a
failed active peer.

The exchange is bounded: a fixed-size random sample, never the
full view.

## Plumtree: broadcasting changes efficiently

Plumtree (Push-Lazy-Push Multicast Tree) is the protocol that
moves registry updates and other gossip across the cluster. The
input is "broadcast this message"; the output is that every peer
eventually sees the message exactly once, even under churn.

The idea: each node classifies its active-view peers into two
sets.

- **Eager peers** receive the full message body.
- **Lazy peers** receive only the message identifier (an `IHAVE`
  announcement); they fetch the body only if they have not seen
  it.

The first time a message goes out, all peers are eager. When a
duplicate arrives (because some other path got there first), the
recipient sends a `PRUNE`, demoting the sender to lazy. The tree
self-organises into a spanning structure where each message
flows through one path; the lazy backups cover the case where a
node drops mid-flight.

If a node receives an `IHAVE` for a message it never sees, it
sends `GRAFT` to the lazy peer, promoting it back to eager so the
message is re-pushed. This is the self-healing path.

The two interesting consequences:

- Broadcast cost is O(n) messages, not O(n log n) or worse.
- A single peer failure costs at most one `GRAFT`/`PRUNE`
  exchange, not a full re-flood.

## The service registry: an OR-Map CRDT

The service registry is the part of mycelium that requires the
most thought to use correctly. We use a CRDT because we want
registration to work without coordination: any node can register
a service at any time, and all nodes converge to the same view
without locking.

The data structure is an **Observed-Remove Map** (OR-Map). A few
properties worth stating explicitly:

- **Add and remove commute.** Two concurrent adds of the same
  name produce two entries; if a third node later removes the
  name, only the additions visible to that third node are
  removed.
- **Tombstones are bounded.** Each remove carries the set of
  dots it observed; once every node has applied the remove, the
  dots can be discarded.
- **Causal merging.** Two replicas merge by union of dots plus
  the rule that a tombstoned dot stays tombstoned.

Each add gets a unique **dot**, which is a `{node, hlc_timestamp}`
pair. The hybrid logical clock guarantees that two adds from the
same node are ordered, and that two adds from different nodes can
be compared causally.

```erlang
-type dot()    :: {node(), mycelium_hlc:timestamp()}.
-type or_map() :: #{
    Key => {
        dots   :: sets:set(dot()),
        values :: [{dot(), Value}]
    }
}.
```

Operationally: when you call `register_service/2`, the registry
adds an entry with a fresh dot, and its `mycelium_replica`
instance broadcasts the delta over Plumtree. When the broadcast reaches
peer B, B merges the delta into its local OR-Map; from then on
B's `whereis_service/1` can find the new registration.

## Sharded placement and the live-node set

`mycelium_shard` answers "which node should own this key" with
rendezvous (HRW) hashing over a replicated live-node set. The set is
NOT the bounded HyParView active view and is not driven by `peer_down`
(which is active-view churn, not cluster death). Instead each node
gossips a periodic heartbeat carrying its wall-clock time through its
own `mycelium_replica` instance (`mycelium_members_replica`); a node is
in the ring while its lease is fresh (`Now - heartbeat =< member_ttl_ms`),
and heartbeats too far in the future are rejected so a fast clock cannot
pin a dead node. The set converges without tombstones: a stale entry in
a full-sync is already expired by its timestamp.

The live member list is published to a read-concurrency ETS table, so
`place/1`, `owners/2`, `is_owner/1`, and `partition/1` are lock-free
local reads. When the live set changes, the shard diffs the partitions
this node owns and emits `{mycelium_shard, {acquired | released, P}}`
to subscribers, which is how consumers hand off partitioned state.

## Durable reminders

`mycelium_reminder` layers fire-at-most-once timers on placement. A
reminder `Key => {FireAt, Payload, Version}` lives in its own
`mycelium_replica` instance (`mycelium_reminder_replica`), so every node
holds it and it survives the node that armed it. The owner of a reminder
is `mycelium_shard:place(Key)`; only the owner arms a local
`erlang:send_after` and fires. The timer is a versioned hint: on timeout
the owner re-checks that the reminder still exists, still names the same
version, is still owned here, and is actually due. Firing is
tombstone-first (gossip the removal, then deliver
`{mycelium_reminder, Key, Payload, Fence}` locally), and ownership
events plus a periodic `reminder_scan_ms` sweep re-arm a survivor's
inherited reminders. The result is exactly-once in steady state and
best-effort under churn or a crash at the fire instant; `Fence` (the
packed version) lets a handler dedup.

## Hybrid logical clocks

Standard wall-clock timestamps can move backwards if NTP corrects
a drifting clock, and they cannot order events from different
nodes consistently. A pure logical clock (a Lamport clock) orders
events but loses the connection to physical time. Mycelium uses
**hybrid logical clocks** (HLC), which combine the two.

The shape is `{wall_ms, logical}`:

- `wall_ms` is the current wall time in milliseconds.
- `logical` is a counter that breaks ties when two events share a
  wall-time.

The clock has two operations:

- `mycelium_hlc:now/0` produces the next local timestamp, ensuring
  monotonic progress relative to the previous local timestamp.
- `mycelium_hlc:update/1` accepts a timestamp from a peer and
  advances the local clock to be greater than both the local
  reading and the peer's timestamp.

HLC timestamps serve two roles in mycelium:

- They are the `dot` component in OR-Map adds. This is where
  causality across nodes lives.
- They are exposed to applications that need cluster-wide
  ordering without coordinating; the `mycelium_hlc` module is
  public.

## Authentication: Ed25519 over a QUIC stream pair

Authentication runs *between* the QUIC TLS handshake and the
Erlang dist handshake. The flow is a mutual challenge-response on
a pair of unidirectional QUIC streams:

```
Node A (client)                               Node B (server)
      |                                             |
      | open uni stream 2                           |
      |-- HELLO(node_A, pubkey_A) ----------------->|
      |                                             |
      |                                  open uni stream 3
      |<------------------------ HELLO(node_B, pubkey_B)
      |                                             |
      |-- CHALLENGE(nonce_A, wall_ts_A) ----------->|
      |<------------ CHALLENGE(nonce_B, wall_ts_B)  |
      |                                             |
      |-- RESPONSE(sign(nonce_B, ts_B, pubkey_A)) ->|
      |<----------- RESPONSE(sign(nonce_A, ts_A, pubkey_B))
      |                                             |
      |-- OK -------------------------------------->|
      |<----------------------------------------- OK
      v                                             v
            Erlang dist handshake (cookie)
```

The signed message includes the responder's own public key. That
binds the signature to the identity the responder claims, so a
peer cannot relay a signature to impersonate someone else.

Trust modes operate on what happens when the presented public key
is not the one already pinned for that node atom:

| Mode    | No pin yet      | Pin matches | Pin differs |
|---------|-----------------|-------------|-------------|
| `tofu`  | accept and pin  | accept      | reject      |
| `strict`| reject          | accept      | reject      |

The trust store lives on disk under `data/keys/trusted/`,
one file per peer (`<node-atom>.pub`). Writes are atomic
(write-then-rename) and use 0600 permissions; see
[authentication.md](../how-to/configure-authentication.md) for the full lifecycle.

## The dist carrier: `mycelium_dist` + `quic_dist`

`mycelium_dist` is the proto_dist module Erlang loads when you
boot with `-proto_dist mycelium`. It is intentionally a thin
shim: the bulk of the QUIC transport is upstream
[`quic_dist`](https://github.com/benoitc/erlang_quic).

What `mycelium_dist:listen/1` does, in order:

1. **Ensures TLS material.** If `data/quic/node.crt` and
   `data/quic/node.key` exist, it uses them; otherwise it
   generates a self-signed pair via `mycelium_quic_cert`.
2. **Projects defaults into the `quic.dist` app env.** Sets
   `auth_callback => {mycelium_dist_auth_callback, authenticate}`,
   `discovery_module => mycelium_discovery`, and the cert/key
   paths. User-supplied values under `{quic, [{dist, [...]}]}` in
   `sys.config` always win; this step only fills gaps.
3. **Validates the projected config.** If `auth_enabled` is
   `true` but the projected `auth_callback` is `undefined`
   (because the user explicitly nulled it), boot fails loudly
   rather than silently shipping an unauthenticated cluster.
4. **Delegates to `quic_dist:listen/1`.**

For outgoing connections, the `setup/5` callback runs the
auth-callback in the same process that initiated the QUIC connect,
so we can pass the dialed node atom to the callback through the
process dictionary. This is needed so the client side of the
handshake can check `cookie_only_nodes` for the *target* (the node
we asked to connect to) rather than the peer's self-reported
identity.

### Discovery

The default `discovery_module` is `mycelium_discovery`, a
*composing* dispatcher: it asks each backend in a chain until one
returns a hit, then caches the result. The default backend chain
is:

1. **Static**. Reads `{quic, [{dist, [{nodes, [...]}]}]}` from
   sys.config. The shape is `{NodeAtom, {Host, Port}}`. Useful in
   docker-compose, in tests, and any environment with a fixed
   topology.
2. **File**. Reads `data/discovery/<node>.endpoint` files. Useful
   on a single host where every node writes its own endpoint to a
   shared directory.
3. **DNS**. Resolves `<host>` portions of node atoms via DNS.
   Useful in environments with proper DNS plumbing.

You can replace the chain entirely by setting
`mycelium.discovery_backends` in sys.config.

## The idle dist-channel GC

`Pid ! Msg` to any cluster node works through OTP's demand-driven
auto-connect. That can open a dist channel that the application
then never uses again, which would accumulate over time. The dist
GC reaps such channels.

The flow below is the reason the GC exists. A service lookup may
return a pid on a node outside the local active view. Sending to that
pid opens an authenticated QUIC dist channel. If the application does
not keep using it, Mycelium closes it later.

![Sending to a pid outside the active view opens an authenticated QUIC dist channel on demand.](diagrams/message-passing.png)

The predicate is conservative. A channel is eligible for reaping
when **all** of the following hold:

- The peer is not in the local HyParView active view.
- `quic_dist:list_streams/1` returns the empty list (no live user
  streams are riding the dist channel).
- The channel is older than `dist_gc_min_age_ms` (default 5
  minutes).

A reaped channel is closed cleanly. If the application sends to
the same peer later, a new dist channel opens on demand.

This GC is unconditionally on. The decoupled-from-active-view
design relies on its presence; see
[features.md](../features.md) for the stability tier.

## Service overlay routing and proxies

`whereis_service/1` resolves a service name in three steps:

1. Look up locally.
2. Look up in the local cache (populated by gossip).
3. If neither, ask through the overlay.

The overlay step uses `mycelium_router`: it sends a route request
to a random active peer with a TTL, which forwards in turn until
a peer finds the service or the TTL runs out. The path is cached
on success so subsequent lookups are direct.

When the resolved service is on a remote node, `whereis_service/2`
optionally hands you a **service proxy**: a local pid that
forwards `gen_server` calls and casts to the remote service over
the dist channel. The proxy is what makes
`{via, mycelium, Name}` registrations transparent: a caller can
`gen_server:call({via, mycelium, my_service}, request)` and the
proxy handles the remote dispatch.

Proxies are reference-counted and reaped when the remote service
goes down.

## Tagged-stream multiplex

The dist channel between two peers is multiplexed: the Erlang
dist control stream is one QUIC bidirectional stream, but the
application can open additional streams alongside it. Mycelium
exposes that as `mycelium_streams`, a single demultiplexer per
node.

Wire format: every mycelium-managed user stream starts with

```
<<TagLen:8, Tag:TagLen/binary, Payload/binary>>
```

The demuxer reads the first `1 + TagLen` bytes, looks up the
registered acceptor for the tag, and hands the stream to that
acceptor. From then on the acceptor owns the stream and uses
`quic_dist:send/2` / `close_stream/1` directly; the demuxer is
off the data path.

The cap on parked-but-not-yet-dispatched streams is small
(currently 64); a peer that opens many streams and drips bytes
without ever completing the tag preamble has its excess streams
reset.

## Connection migration

A single QUIC connection can rebind to a new local UDP 4-tuple
(NIC change, IP change, default-route change) without losing
keys, streams, or ordering. `mycelium:migrate_peer/1,2` exposes
that primitive as a synchronous call.

The decision of **when** to migrate is the application's. Mycelium
provides the trigger and the path statistics
(`mycelium_path_stats:srtt/1`); a watchdog can poll, evaluate, and
call.

The motivating cases are: a mobile node moves between Wi-Fi and
cellular; a server's outbound IP changes because of a CGNAT
shuffle; a peer is being routed through a different relay. See
[migration.md](../how-to/migrate-connections.md) for the recipe.

## Observability

Every metric mycelium emits goes through `mycelium_metrics`. The
catalog is in [observability.md](../how-to/observe-cluster.md); the design
note for this document is: emit sites are wrapped in a
`try`/`catch`, so a misconfigured exporter cannot crash protocol
code.

## Reading the source

If you want to follow a code path end-to-end, the natural seams
are:

- A `mycelium:join/1` call. Start in `mycelium_hyparview`'s
  `handle_call({join, ...})` and follow the `mycelium_bridge`
  request, the QUIC connect, the auth callback, the dist
  handshake, the `peer_up` event.
- A `register_service/2` call. Start in `mycelium_registry`'s
  `handle_call({register, ...})` and follow the OR-Map add, the
  `mycelium_replica` broadcast, the `mycelium_plumtree`
  fanout, and the merge on a remote node.
- A `whereis_service/1` call. Start in `mycelium.erl`, see how
  the local lookup is tried first, then the cache, then the
  overlay route request.

The test suites that exercise each path are named after the
module under test (`test/mycelium_hyparview_SUITE.erl`,
`test/mycelium_registry_SUITE.erl`, `test/mycelium_router_SUITE.erl`,
etc.). [testing.md](../how-to/run-tests.md) lists them and explains the
docker-only suite for the full transport behaviour.
