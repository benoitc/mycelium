# Quickstart: a minimal app on Mycelium

The smallest useful OTP application built on
[Mycelium](https://github.com/benoitc/mycelium): a worker that registers
itself in the cluster-wide service registry, and a tiny API that discovers
and calls a worker on any node.

It is the runnable companion to the
[Create an application](../../docs/tutorials/create-an-application.md)
tutorial.

## Layout

```
quickstart/
├── rebar.config            # mycelium dep + a release
├── config/
│   ├── sys.config          # mycelium env (ports, auth, discovery)
│   └── vm.args             # the three dist flags + node name + cookie
├── scripts/run-local.sh    # run an isolated node locally (for the 2-node demo)
└── src/
    ├── quickstart.app.src   # lists mycelium in `applications`
    ├── quickstart_app.erl   # application behaviour
    ├── quickstart_sup.erl   # supervisor
    ├── quickstart_worker.erl# gen_server that registers a service
    └── quickstart.erl       # discover-and-call API
```

## Build

```bash
rebar3 compile
```

The dependency is mycelium (which pulls in `quic`, `hlc`, `instrument`).
`scripts/run-local.sh` links this repo as a `_checkouts/mycelium` so it
builds against your local checkout; for a standalone project use the git or
hex dependency in `rebar.config`.

## Try it on one node

```bash
ERL_AFLAGS="-proto_dist mycelium -epmd_module mycelium_epmd -start_epmd false" \
rebar3 shell --config config/sys.config --sname q1
```

```erlang
1> quickstart:work(hello).
{worked_on, q1@yourhost, hello}
2> mycelium:lookup(quickstart_worker).
{ok, [{service_entry, quickstart_worker, <0.123.0>, q1@yourhost, #{node => q1@yourhost}}]}
3> quickstart:who().            %% node name + Ed25519 fingerprint to share
{q1@yourhost, <<...32 bytes...>>}
```

The node generated its TLS cert and Ed25519 identity under `data/` on first
boot; nothing to provision.

## Two nodes on one host

Each node gets its own keys and cert (`data/node<N>/`) but shares
`data/discovery` so they find each other:

```bash
# Terminal 1 (seed)
./scripts/run-local.sh 1

# Terminal 2 (joins node1 automatically)
./scripts/run-local.sh 2
```

In node2's shell:

```erlang
1> quickstart:peers().
['node1@yourhost']
2> quickstart:work(hi).                          %% handled locally on node2
{worked_on, node2@yourhost, hi}
3> quickstart:work_on('node1@yourhost', hi).     %% discovered + routed to node1
{worked_on, node1@yourhost, hi}
```

`work/1` prefers the local worker; `work_on/2` looks a specific node's worker
up by name and calls it over standard distribution.

## Keys

Every node has an Ed25519 identity (its `data/.../keys/node.pub`). In the
default `tofu` trust mode the first handshake pins peers automatically. For
`strict` mode you create and share public keys ahead of time. See
[Manage node keys](../../docs/how-to/manage-node-keys.md).

## Production

For a real deployment, build the release (`rebar3 as prod release`), pin
`listen_port`, set a real `dist_cookie`, list seeds in `contact_nodes`, and
persist `data/`. See
[Run in production](../../docs/how-to/run-in-production.md).
