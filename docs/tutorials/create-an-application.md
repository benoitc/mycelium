# Create an application

This tutorial builds a minimal OTP application on mycelium from scratch: a
worker that registers itself in the cluster-wide service registry, and a
small API that discovers and calls a worker on any node. It is the setup
companion to [Distributed chat](distributed-chat.md), which builds something
larger on the same primitives.

The full source is under
[`examples/quickstart`](../../examples/quickstart/README.md); this page
explains each piece. If you have not read [Getting started](../overview/getting-started.md),
skim it first: it covers the boot flags and the credential layout in more
depth.

## What you get

A normal OTP app where `Pid ! Msg`, `gen_server:call`, `global`, links and
monitors all work as usual, but distribution rides mycelium's QUIC carrier
and HyParView membership, and you get a built-in service registry.

## Prerequisites

- Erlang/OTP 27 or later, and `rebar3`.
- No EPMD: mycelium does not use it.
- One UDP port per node (the default `9100` in this example).

## Step 1: scaffold the app

```bash
rebar3 new app myapp
cd myapp
```

## Step 2: add the dependency

In `rebar.config`:

```erlang
{deps, [
    {mycelium, "0.1.0"}
]}.
```

```bash
rebar3 get-deps && rebar3 compile
```

Mycelium pulls in the QUIC transport, `hlc`, and `instrument`.

## Step 3: declare mycelium as a runtime dependency

List it in `src/myapp.app.src` so the release boots it before your
supervisor starts (your services register against a running mycelium):

```erlang
{applications, [kernel, stdlib, mycelium]}
```

## Step 4: configure the node

`config/sys.config` (mycelium projects the underlying `quic_dist` wiring
itself; you only set mycelium env):

```erlang
[
 {mycelium, [
    {active_size, 5}, {passive_size, 30},   %% HyParView views
    {listen_port, 9100},                    %% pin in prod; 0 = OS-assigned
    {contact_nodes, []},                    %% seeds to auto-join at boot
    {dist_cookie, quickstart},              %% set as the node cookie at boot
    {auth_enabled, true},                   %% Ed25519 mutual auth (default)
    {auth_trust_mode, tofu}                 %% tofu | strict
 ]}
].
```

`config/vm.args` (the three flags switch Erlang's distribution to mycelium):

```
-name myapp@127.0.0.1
-setcookie quickstart
-proto_dist mycelium
-epmd_module mycelium_epmd
-start_epmd false
```

## Step 5: write a worker that registers a service

`register_service/2` registers the calling process, so a `gen_server` that
calls it in `init/1` publishes itself under a cluster-wide name:

```erlang
-module(myapp_worker).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    ok = mycelium:register_service(myapp_worker, #{node => node()}),
    ok = mycelium:register_service({worker, node()}, #{}),
    {ok, #{}}.

handle_call({work, X}, _From, State) ->
    {reply, {worked_on, node(), X}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) ->
    mycelium:unregister_service(myapp_worker),
    mycelium:unregister_service({worker, node()}),
    ok.
```

It registers two names: a generic `myapp_worker` (local-preferred by
`whereis_service/1`) and a per-node `{worker, node()}` so a specific node can
be targeted. The registry monitors the pid, so the entries also disappear
automatically if the worker dies.

The discover-and-call API. `whereis_service/1` returns `{ok, Pid}` for a
local service and `{ok, Node, Pid}` for a remote one; handle both, then send
over standard distribution:

```erlang
-module(myapp).
-export([work/1, work_on/2]).

work(X)          -> call(myapp_worker, {work, X}).
work_on(Node, X) -> call({worker, Node}, {work, X}).

call(Name, Msg) ->
    case mycelium:whereis_service(Name) of
        {ok, Pid}        -> gen_server:call(Pid, Msg);
        {ok, _Node, Pid} -> gen_server:call(Pid, Msg);
        {error, not_found} -> {error, no_worker}
    end.
```

## Step 6: supervise it

Add the worker to your supervisor's children (`one_for_one`), and start the
supervisor from your `application` callback. (See `quickstart_sup.erl` and
`quickstart_app.erl` in the example.)

## Step 7: run one node

```bash
ERL_AFLAGS="-proto_dist mycelium -epmd_module mycelium_epmd -start_epmd false" \
rebar3 shell --config config/sys.config --sname q1
```

```erlang
1> myapp:work(hello).
{worked_on, q1@yourhost, hello}
2> mycelium:lookup(myapp_worker).
{ok, [{service_entry, myapp_worker, <0.123.0>, q1@yourhost, #{node => q1@yourhost}}]}
```

The node generated its TLS cert and Ed25519 identity under `data/` on first
boot. `lookup/1` returns `#service_entry{}` records (`{service_entry, name,
pid, node, meta}`).

## Step 8: two nodes

The example ships `scripts/run-local.sh`, which gives each node its own keys
and cert under `data/node<N>/` while sharing `data/discovery` so they find
each other on one host:

```bash
./scripts/run-local.sh 1     # seed
./scripts/run-local.sh 2     # joins node1
```

In node2's shell:

```erlang
1> myapp:work_on('node1@yourhost', hi).
{worked_on, node1@yourhost, hi}
```

The reply is tagged by `node1`: node2 discovered node1's worker through the
registry and called the real pid over standard distribution. Across hosts you
would instead list seeds in `contact_nodes` and let nodes auto-join at boot
(see [Getting started](../overview/getting-started.md#seeds-and-discovery)).

## Step 9: keys

Each node has an Ed25519 identity. In `tofu` mode the first handshake pins
peers automatically, so the two-node demo needs no key setup. For `strict`
mode, or to verify a node out of band, see
[Manage node keys](../how-to/manage-node-keys.md): create keys, read a
fingerprint, and share public keys with peers.

## Step 10: production

For a real deployment, build a release (`rebar3 as prod release`), pin
`listen_port`, set a real `dist_cookie`, list seeds in `contact_nodes`, and
persist `data/`. See [Run in production](../how-to/run-in-production.md) for
ports, secrets, sizing, and the graceful shutdown order.

## Where to go next

- [Distributed chat](distributed-chat.md) builds a fuller app on the same
  primitives, including service events.
- [Share replicated state](../how-to/share-replicated-state.md),
  [Schedule durable jobs](../how-to/schedule-durable-jobs.md), and the
  leader-election and sharded-placement concepts add the other building
  blocks.
