# Getting started

This guide takes a working Erlang/OTP environment and walks you to a
two-node mycelium cluster, with a real handshake and a real service
lookup. We do not skip the underlying ideas: each step explains what
the system does and why.

The audience is an Erlang developer comfortable with `gen_server`,
releases, and the standard distribution. You do not need prior
knowledge of HyParView, QUIC, or CRDTs; we will introduce the parts
that matter as they appear.

## What you are about to build

A mycelium cluster is, on the surface, a normal Erlang cluster:
`Pid ! Msg`, `rpc:call/4`, `gen_server:call/2`, and `global` all work
as you expect. Under that surface, three pieces replace the default
Erlang behaviour:

- **The dist carrier**. Instead of TCP, mycelium runs the distribution
  channel over a single QUIC connection per peer. That gives us
  encryption by default, multiplexed streams on top of the same
  connection, and connection migration when the local network
  changes.
- **The membership protocol**. Instead of a full mesh where every
  node opens a TCP connection to every other node, each node keeps
  a small, bounded set of *gossip* peers (the HyParView "active
  view", typically five). The cluster as a whole remains fully
  addressable through OTP's demand-driven auto-connect.
- **A service registry**. Processes can be registered under a name
  and discovered from any node in the cluster, without you running a
  separate service-discovery service.

Read the graph from the node in the centre. The active view is the
small set of peers used for gossip. The passive view is a cache of
known peers. They are not connected now, but they are useful when an
active peer drops or when the topology needs to refresh.

![HyParView active view: a node connects to a small set of gossip peers, with additional known peers held in a passive cache.](diagrams/active-view.png)

For an Erlang application, this graph is not `nodes()`. It is only
the maintenance topology. Application traffic can still reach a pid
on another cluster member.

The rest of this guide is the smallest path from a fresh checkout to
two nodes exchanging a message.

## Prerequisites

- Erlang/OTP 27 or later.
- `rebar3`.
- A single UDP port available on each node (the default is
  `0`, meaning "let the OS assign one"; in production you pin it).

We assume no EPMD daemon is running. Mycelium does not use it: peers
discover each other through the discovery chain described later.

## One-time setup: where credentials live

Each mycelium node carries two kinds of credentials on disk. They are
generated the first time the node boots; you do not have to create
them manually.

```
data/
├── quic/
│   ├── node.crt    (TLS certificate; auto-generated, self-signed)
│   └── node.key    (TLS private key; chmod 0600)
└── keys/
    ├── node.pub    (Ed25519 public key, 32 bytes)
    ├── node.key    (Ed25519 private key, 32 bytes; chmod 0600)
    └── trusted/
        └── ...     (one .pub per peer this node has pinned)
```

The two layers serve different purposes:

- **The TLS material** secures the *transport*. QUIC needs a cert to
  start a listener. Mycelium uses self-signed certs by default; the
  certificate's identity is the Ed25519 public key recorded below,
  not the CN field.
- **The Ed25519 keypair** is the node's *identity*. After the QUIC
  TLS handshake completes, the two peers run a short
  challenge-response over a pair of QUIC streams to prove they hold
  the private key matching the public key they presented. The
  cluster uses this layer, not the TLS layer, to decide whether to
  trust the peer.

Both materials are regenerated on a fresh boot if missing. You can
also pre-generate the TLS material with the helper script:

```bash
_build/default/lib/mycelium/priv/bin/mycelium_gen_cert.sh
```

(`--out-dir`, `--cn`, `--days`, `--key-bits`, `--force` are the
flags; the script is idempotent unless you pass `--force`.)

## Adding mycelium to a project

In `rebar.config`:

```erlang
{deps, [
    {mycelium, "0.1.0"}
]}.
```

Fetch and compile:

```bash
rebar3 get-deps
rebar3 compile
```

That is the full dependency setup. Mycelium pulls in the upstream
QUIC implementation, `hlc` for hybrid logical clocks, and `instrument`
for metrics.

## Configuring the node

Mycelium is opinionated about defaults. A minimal `config/sys.config`
that lets you boot a node looks like this:

```erlang
[
    {mycelium, [
        %% HyParView membership parameters
        {active_size, 5},          %% Maximum concurrent gossip peers
        {passive_size, 30},        %% Known-but-disconnected peers

        %% Network
        {listen_port, 9100},       %% 0 lets the OS choose

        %% Authentication
        {auth_enabled, true},      %% Default; flip to false only for dev
        {auth_trust_mode, tofu}    %% tofu | strict
    ]}
].
```

The two membership parameters deserve a word. The *active view* is
the small set of peers a node currently exchanges gossip with. The
protocol guarantees that the whole cluster is reachable from any
node by repeatedly forwarding through this view, so the active view
does not need to grow with the cluster: five peers per node is
enough for thousands of nodes. The *passive view* is a cache of
known peers that are not currently in the active view; they are
warm spares used when an active peer drops.

Trust mode controls the authentication layer:

- `tofu` ("trust on first use") is the default. The first time a
  node meets a peer, it records the peer's Ed25519 public key. From
  then on, that peer is rejected if it presents a different key.
- `strict` requires every peer's key to be pinned in advance. Useful
  in environments where you do not want any first-contact window.

See [authentication.md](../how-to/configure-authentication.md) for the operational side
of trust modes.

## Boot arguments

Two BEAM-level boot flags switch Erlang's distribution layer to
mycelium:

```bash
-proto_dist mycelium
-epmd_module mycelium_epmd
-start_epmd false
```

The first selects mycelium as the dist module. The second tells
`net_kernel` to use mycelium's discovery shim instead of the stock
EPMD daemon. The third disables the daemon entirely; mycelium does
not need it.

These three lines are the entire dist configuration. Everything
else, certificate paths, the auth callback, the discovery chain, is
projected by mycelium into the underlying `quic_dist` app
environment when the listener starts.

## Starting one node

The fastest path is a `rebar3 shell` with the boot args injected:

```bash
ERL_AFLAGS="-proto_dist mycelium -epmd_module mycelium_epmd -start_epmd false" \
rebar3 shell --config config/sys.config --sname node1
```

Inside the shell:

```erlang
1> mycelium:active_view().
[]
```

You have a running mycelium node with no peers yet. The TLS material
and the Ed25519 keypair are in `data/quic/` and `data/keys/`.

To inspect the identity:

```erlang
2> {ok, PubKey} = mycelium_dist_auth:get_public_key().
{ok, <<...32 bytes...>>}
3> mycelium_dist_keys:fingerprint(PubKey).
<<...32 bytes SHA-256...>>
```

The fingerprint is what you log and share when verifying a node out
of band.

## Forming a two-node cluster

Open a second terminal and start a second node:

```bash
ERL_AFLAGS="-proto_dist mycelium -epmd_module mycelium_epmd -start_epmd false" \
rebar3 shell --config config/sys.config --sname node2
```

On `node2`, ask to join `node1`:

```erlang
1> mycelium:join('node1@yourhost').
ok
2> mycelium:active_view().
['node1@yourhost']
```

On `node1`, you will see the symmetric view:

```erlang
4> mycelium:active_view().
['node2@yourhost']
```

What happened, in order:

1. `node2`'s `join/1` produced a `JOIN` message to `node1`.
2. The QUIC layer opened a UDP-backed connection between the two
   nodes. The TLS handshake completed using the self-signed certs.
3. The Ed25519 challenge-response ran on a pair of unidirectional
   QUIC streams. Because both nodes are in `tofu` mode and neither
   had seen the other before, both pinned the other's public key
   under its node atom.
4. The standard Erlang dist handshake then ran on top of the
   authenticated channel.
5. HyParView added each side to the other's active view.

From this point on, anything you can do with stock Erlang
distribution works: `rpc:call/4`, `Pid ! Msg`, `global`, links,
monitors.

## Seeds and discovery

The manual `join/1` above is the teaching path. In a real deployment a
node joins the cluster from configuration, and you never type a node name.
This section explains the two pieces that make that work: what a *seed* is,
and how a node turns a seed's name into an address.

### What a seed is

A seed is just an existing cluster member that a new node contacts to get
in. Nothing about a seed's own configuration is special; what matters is
that other nodes know how to reach it. The very first node you start has
no one to join (it *is* the seed); every later node joins through one or
more seeds. Seeds are not masters: once a node is in the overlay it is an
equal peer, and any member can seed the next joiner.

Because membership is gossiped, you need only a few seeds. Two or three
stable, well-known addresses are enough for a large cluster, since a
joiner needs just one of them to answer.

### How a node resolves a seed's address

Mycelium runs no EPMD, so a node name like `seed@10.0.0.1` must be turned
into a UDP `{address, port}` some other way. That is the *discovery chain*:
a list of backends tried in order, first hit wins.

```erlang
{mycelium, [
    {discovery_backends, [
        mycelium_discovery_static,   %% explicit {Node, {Addr, Port}} table
        mycelium_discovery_file,     %% shared dir of <node>.endpoint files
        mycelium_discovery_dns       %% resolve the host part via DNS
    ]}
]}
```

- **static** reads an explicit table from the `quic` app's `dist` env. Use
  it when seed addresses are fixed and known up front:

  ```erlang
  {quic, [
      {dist, [
          {nodes, [
              {'seed1@10.0.0.1', {"10.0.0.1", 9100}},
              {'seed2@10.0.0.2', {"10.0.0.2", 9100}}
          ]}
      ]}
  ]}
  ```

- **file** uses a directory every node can read. Each node writes its own
  `<node>.endpoint` file there at boot (`discovery_dir`, default
  `data/discovery`), so peers sharing the filesystem (one host, or a
  shared/NFS volume) find each other with no static table. This is what
  the local multi-node examples use.

- **dns** takes the host part of the node name
  (`seed@db.svc.cluster.local`), resolves it through DNS, and pairs it
  with the listen port. Handy on Kubernetes or anywhere the name already
  resolves to an address.

A seed must be reachable through whatever chain its joiners use. The
simplest setups are one static entry per seed (cloud VMs with fixed IPs)
or a shared `discovery_dir` (a single host or a shared volume).

### Auto-joining seeds with `contact_nodes`

List the seeds in `contact_nodes` and mycelium joins them at boot, so you
never call `mycelium:join/1`:

```erlang
{mycelium, [
    {listen_port, 9100},
    {contact_nodes, ['seed1@10.0.0.1', 'seed2@10.0.0.2']}
]}
```

While its active view is empty, the node asks each contact to let it in,
retrying every `contact_retry_ms` (default 5000) until it is in the
overlay. A seed that comes up after its joiners, or a node that briefly
loses every peer, recovers on its own. The seeds must be resolvable
through the discovery chain.

Ship the *same* `contact_nodes` list to every node, seeds included (a node
skips its own name), so the configuration is uniform across the fleet. A
single-seed cluster leaves the seed's list effectively empty and points
everyone else at it.

### Starting a node in production

In a release the three dist flags go in `vm.args` and the mycelium
configuration in `sys.config`:

```
## vm.args
-name app@10.0.0.5
-setcookie <your-secret>
-proto_dist mycelium
-epmd_module mycelium_epmd
-start_epmd false
```

```erlang
%% sys.config
[
    {mycelium, [
        {listen_port, 9100},
        {contact_nodes, ['seed1@10.0.0.1', 'seed2@10.0.0.2']},
        {dist_cookie, <<"your-secret">>},
        {auth_trust_mode, tofu}
    ]},
    {quic, [
        {dist, [{nodes, [
            {'seed1@10.0.0.1', {"10.0.0.1", 9100}},
            {'seed2@10.0.0.2', {"10.0.0.2", 9100}}
        ]}]}
    ]}
].
```

The node generates its TLS and Ed25519 material on first boot (see
[where credentials live](#one-time-setup-where-credentials-live)), resolves
the seeds through discovery, and joins the overlay, all without a scripted
join step. See [run in production](../how-to/run-in-production.md) for
ports, secrets, sizing, and shutdown.

## A first service lookup

Service registration is the part of mycelium that you reach for
most often when building a real application. A service is a process
registered under a name; that name is replicated across the cluster
so any peer can find the process.

On `node1`:

```erlang
5> mycelium:register_service(my_worker, #{role => worker}).
ok
```

The service is registered for the process that calls
`register_service/2`. In a real application this call normally lives
in the service process itself, often in `init/1`. The metadata map is
free-form; you can store anything that fits naturally in a Map.

On `node2`, a moment later (replication is asynchronous, typically
under a second):

```erlang
3> mycelium:lookup(my_worker).
{ok, [{service_entry, my_worker, <0.123.0>, 'node1@yourhost', #{role => worker}}]}

4> {ok, _Node, FoundPid} = mycelium:whereis_service(my_worker).
{ok, 'node1@yourhost', <0.123.0>}

5> FoundPid ! hello.
hello
```

Three things to notice:

- `lookup/1` returns all instances registered under the name. There
  may be more than one if multiple nodes register the same name.
- `whereis_service/1` returns a single instance and prefers a local
  one when there is a choice. The shape is `{ok, Pid}` for a local
  service and `{ok, Node, Pid}` for a remote one. It is the function
  you reach for from application code.
- The pid we got back is the *real* pid on `node1`. The send-bang
  uses standard Erlang distribution, opened on demand. No mycelium
  primitive is on the data path once you hold the pid.

The flow of a cross-node send, when the target is not in the local
active view, is the part to keep in mind:

![Sending a message to a pid on a node that is not in the local active view: OTP opens a QUIC dist channel on demand, runs Ed25519 auth, then delivers the message.](diagrams/message-passing.png)

Mycelium helps you find the pid. OTP sends to it. If no dist channel
exists yet, the QUIC channel is opened and authenticated before the
message is delivered.

### Subscribing to membership and service events

If your application needs to react to cluster changes:

```erlang
6> mycelium:subscribe().
ok
%% receive {mycelium_event, {peer_up, Node}} | {mycelium_event, {peer_down, Node, Reason}}

7> mycelium:subscribe_services().
ok
%% receive {mycelium_service_event, {service_registered, Name, Node}}
%%       | {mycelium_service_event, {service_unregistered, Name, Node}}
%%       | {mycelium_service_event, {service_down, Name, Node, Reason}}
```

These two subscription bus are independent and idempotent: subscribing
twice from the same pid is a no-op. Both deliver standard Erlang
messages, so you can route them through your existing handler.

## Tearing down

Either call `mycelium:leave/0` for a graceful exit (peers move you
to their passive view immediately), or stop the application:

```erlang
8> mycelium:leave().
ok
```

In production, the recommended shutdown order is
`mycelium:leave/0`, then `application:stop(mycelium)`, then
`init:stop/0`. The reasoning is in [deployment.md](../how-to/run-in-production.md).

## A quick look at what just happened

Stepping back from the commands you typed:

- Two BEAM nodes ran with `-proto_dist mycelium`. That replaced the
  default TCP dist with a QUIC carrier, automatically projected the
  certificate paths and the auth callback, and disabled EPMD.
- Each node generated its identity material on first boot. No
  manual provisioning.
- A join over QUIC was authenticated end to end: TLS at the
  transport layer, Ed25519 at the identity layer, the dist cookie
  at the Erlang layer.
- Membership was managed by HyParView with active view size 5; even
  with thousands of nodes, each node would keep only five active
  peers.
- The service registry is a CRDT (an Observed-Remove Map). That is
  why we said "a moment later": registrations are gossiped through
  the cluster and merge without coordination.

## Where to go next

- [tutorial.md](../tutorials/distributed-chat.md) is the practice handbook: build a small
  Erlang application using the primitives introduced here.
- [internals.md](../reference/architecture.md) describes the protocols in more
  depth: HyParView's failure handling, Plumtree's gossip tree, the
  OR-Map merge, and how the QUIC carrier is wired.
- [authentication.md](../how-to/configure-authentication.md) covers strict mode, key
  rotation, and the trust store on disk.
- [deployment.md](../how-to/run-in-production.md) is the operational reference: ports,
  permissions, sizing, shutdown.
- [troubleshooting.md](../how-to/troubleshoot.md) is the table to skim when
  the cluster does not do what you expect.
