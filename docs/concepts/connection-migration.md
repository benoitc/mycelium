# Connection migration

A QUIC connection has a property no other Erlang dist carrier
offers: it can rebind to a new local UDP 4-tuple without losing
keys, ordering, or streams. RFC 9000 §9 specifies the
mechanism; mycelium exposes it as a one-shot trigger via
`mycelium:migrate_peer/1,2`.

This page explains what migration solves and when it makes
sense. For the watchdog recipe and the operational details, see
[migrate connections](../how-to/migrate-connections.md).

## What migration solves

Three motivating cases:

- **A laptop running mycelium moves between networks.** Wi-Fi
  to wired, one Wi-Fi network to another, Wi-Fi to cellular.
  The local IP changes; the peer expects packets on the old
  4-tuple.
- **A server's outbound IP changes.** A CGNAT shuffle, a
  routing-table change, a NIC swap. The peer holds a
  connection that is now silently dead from one side.
- **A user-space tunnel reconnects.** A WireGuard daemon, a
  MASQUE proxy, an SSH `ProxyCommand` reconnects and exposes
  a new local socket.

In each case the alternatives without migration are
unsatisfactory:

- Closing and re-establishing the dist channel triggers
  HyParView churn (one `peer_down`, then a `peer_up`).
- Any in-flight Erlang dist data is flushed.
- With strict trust mode, the re-handshake re-runs the
  Ed25519 round trip.

Migration moves the session to the new path in milliseconds,
with no observable interruption at the Erlang layer. Open
streams ride through. The dist controller continues sending on
the new path.

## What migration is *not*

Migration is **not** an automatic feature. Mycelium does not
poll the local network state and decide when the path has
changed. Two reasons:

- "The network has changed enough to warrant a migration" is
  environment-specific. A laptop wants different signals than
  a server in a CGNAT.
- A one-size-fits-all heuristic would be wrong for half of the
  cases that matter.

So mycelium provides the trigger; you provide the policy.

## The primitive

```erlang
ok                              = mycelium:migrate_peer(Node).
ok                              = mycelium:migrate_peer(Node, #{timeout => 5000}).

%% Errors:
{error, not_connected}          %% no current dist channel to Node
{error, no_conn}                %% controller alive but QUIC conn gone
{error, peer_disable_migration} %% peer set the transport-param flag
{error, timeout}                %% path validation didn't complete in time
```

The call is synchronous. It blocks until the new path is
validated by the QUIC layer (the `PATH_CHALLENGE` /
`PATH_RESPONSE` exchange) or until the timeout fires. The
default timeout is 5000 ms.

On success the dist channel and any open user streams ride
through; the application does not see an interruption.

## Error semantics

A short note on each error.

- `{error, not_connected}` and `{error, no_conn}` mean there
  is nothing to migrate. The dist channel does not exist or
  has already gone away. Wait for `peer_up` and try again.
- `{error, peer_disable_migration}` is **terminal for the
  connection**. The peer set the QUIC
  `disable_active_migration` transport parameter at handshake
  time; no migration on this connection will ever succeed.
  Cache this fact and skip the peer until HyParView reports
  `peer_down`/`peer_up`. Retrying on the same connection wastes
  time.
- `{error, timeout}` means the new path did not validate
  within the budget. Either the new route is genuinely broken
  (in which case HyParView will notice the failure and demote
  the peer at its own cadence), or it is simply slow.
  Retrying with a larger timeout is fine; the old path
  remains usable while the new path is being validated.

## What rides through

When the migration completes, the following state is preserved:

- The QUIC connection's TLS keys.
- The Erlang dist control stream and its in-flight messages.
- Every application stream
  ([mycelium_streams](streams.md)) opened on this connection.
- The dist controller process and its registration in
  `net_kernel`.

What changes:

- The local UDP 4-tuple (source IP and port).
- The QUIC connection's path stats (RTT, congestion state)
  reset, since the new path may have different characteristics.

`erlang:nodes/0` continues to report the peer. `Pid ! Msg` to
processes on the peer continues to work, with no observable
interruption.

## Interaction with relays

A common use case: rebind a connection from one external relay
to another. See [route through a relay](../how-to/route-through-relay.md).

The flow:

1. Establish a new socket adapter pointing at the new relay.
2. Call `mycelium:migrate_peer(Node, #{timeout => 5000})`.
3. After migration succeeds, the dist controller continues on
   the new path.

This lets you swap relays without dropping any in-flight dist
traffic.

## Path statistics

The watchdog recipe in
[migrate connections](../how-to/migrate-connections.md) reads
the QUIC path stats via `mycelium_path_stats`:

```erlang
%% Smoothed RTT in microseconds.
mycelium_path_stats:srtt(Node) -> {ok, integer()} | {error, term()}.

%% Wider snapshot.
mycelium_path_stats:summary(Node) ->
    {ok, #{srtt := integer(),
           latest_rtt := integer(),
           cwnd := integer(),
           in_flight := integer(),
           congested := boolean()}}
  | {error, term()}.
```

These are read-only views over the underlying `quic:get_path_stats/1`
state. They are useful for triggers and for diagnostic logging;
they do not initiate any action.

## Stability tier

`migrate_peer/1,2` is marked `beta` in
[features.md](../features.md). The opts map may grow new
keys across minor bumps; the existing `timeout` key is stable.

The error returns are stable. Code that pattern-matches
`{error, peer_disable_migration}` will continue to work.

## API

```erlang
mycelium:migrate_peer(Node) -> ok | {error, term()}.
mycelium:migrate_peer(Node, Opts) -> ok | {error, term()}.

%% Path statistics for diagnostics or triggers.
mycelium_path_stats:srtt(Node) -> {ok, integer()} | {error, term()}.
mycelium_path_stats:summary(Node) -> {ok, map()} | {error, term()}.
mycelium_path_stats:connection(Node) -> {ok, pid()} | {error, term()}.
```

## Related

- [Migrate connections](../how-to/migrate-connections.md) is
  the operational recipe with a watchdog example.
- [Dist channel](dist-channel.md) is the QUIC connection that
  carries the dist traffic and that migration rebinds.
- [Streams](streams.md) explains what rides through a
  migration alongside the dist control stream.
- [Route through a relay](../how-to/route-through-relay.md) is
  how migration is used to swap external relay paths.
