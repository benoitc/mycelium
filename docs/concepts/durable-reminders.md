# Durable reminders

A durable reminder answers one question: **how do I run something at a
future time, somewhere in the cluster, even if the node that scheduled
it dies first?** `erlang:send_after/3` cannot: the timer lives in one
process on one node and disappears with it. A reminder is replicated, so
it survives the node that armed it, and it fires from whichever node
owns its key when the time comes.

This is built directly on [sharded placement](sharded-placement.md): the
owner of a reminder is `mycelium:place(Key)`. When that node leaves, the
key reassigns to a survivor, and the survivor takes over the unfired
reminder.

## The store

A reminder is an entry `Key => {FireAt, Payload, Version}` in a
replicated OR-Map (the same gossip substrate the registry and leader
election use). `FireAt` is absolute wall-clock time in milliseconds.
`Version` is a hybrid-logical-clock stamp minted once when the reminder
is set; it names this exact reminder version and is identical on every
node. Because the store is replicated, every node holds the reminder, so
any of them can fire it after the original owner is gone.

## Who fires it

Only the **owner** arms a local timer and fires:

```
owner(Key) = mycelium:place(Key)
```

Other nodes hold the reminder in their store but do nothing with it. On
churn, when a node gains the key's partition (an `{acquired, P}`
ownership event from the shard), it arms the reminders it now owns; when
it loses the partition, it disarms them. That hand-off is how a survivor
picks up a dead owner's work.

## Timers are hints

The local `erlang:send_after` is only a hint. When it fires, the owner
re-reads the live entry and proceeds **only if** all of these still
hold:

- the reminder still exists,
- its stored version still equals the one the timer carried,
- this node is still the owner, and
- the fire time has actually been reached.

That makes a stale timer harmless. A reminder that was re-set (new
version), cancelled, handed to another node, or whose timer raced a
cancellation simply does not fire on the stale hint. A periodic safety
sweep (`reminder_scan_ms`) re-arms anything the owner holds but missed,
and re-arms far-future reminders as their fire time nears.

## Firing order

Firing is **tombstone-first**: the owner writes and gossips the removal
*before* delivering the payload to local subscribers. Committing "fired"
first biases the system toward not firing twice rather than toward
firing twice.

Delivery is a message to subscribers on the firing node:

```erlang
{mycelium_reminder, Key, Payload, Fence}
```

`Fence` is the packed version stamp (a positive, comparable integer). It
is the same on every node and names the exact reminder version, so a
handler can use it to deduplicate.

## What it guarantees

There is no consensus here, the same as everywhere else in mycelium, so
the guarantee is stated honestly:

- **Steady state (membership converged): fires exactly once.** One owner,
  one timer, tombstone-first.
- **During churn: best-effort.** Before the live-node set converges, two
  nodes can both believe they own the key and both fire before the
  tombstone propagates. No crash is needed for this window to exist.
- **Crash at the fire instant: best-effort.** A crash *between* writing
  the tombstone and delivering drops the fire (zero deliveries). A crash
  *before* the tombstone gossips can let the new owner fire again.

If you need stronger than best-effort under churn, make your handler
idempotent and dedup on `Fence`. That turns the contract into
at-least-once with an idempotent sink, which is the usual way to run
jobs safely on an AP system.

Fire time is wall-clock ("cluster-time"). Nodes do not share a clock, so
a reminder can fire slightly early or late by the same skew bound that
governs membership. It is right for "around 09:00", not for hard
real-time deadlines.

## Durability model

"Durable" means **replicated AND persisted to disk**. A reminder survives
the death of the node that armed it because every node holds a replica, so a
survivor takes over and fires it. It also survives a **full-cluster restart**:
each node writes its reminder store to disk (a write-ahead log plus periodic
snapshots under `reminder_data_dir`, default `data/reminders`) and recovers it
on boot, after which the cluster re-converges. A `remind`/`cancel` is flushed
to disk before the call returns, so an acknowledged reminder survives a crash
of its node as long as that node's disk does. On recovery the local HLC is
advanced past every persisted timestamp, and fire/cancel tombstones are
persisted too, so a fired reminder is never re-fired after a restart.

The reminder store also reconverges after a partition heals via periodic
anti-entropy (a background full-sync pull every `replica_anti_entropy_ms`,
default 30s), so a reminder reaches every node even if some link survived the
split without a fresh connection event.

**The payload must be restart-safe data**: a self-contained value, not a pid,
port, ref, or fun. This already holds without persistence, because a reminder
is delivered on whichever node *owns* the key at fire time, not where it was
set, so a live local reference is already meaningless cross-node. Persistence
extends that to "after a restart": a pid/ref/fun reloaded from disk points at
something that no longer exists. Pass an id or descriptor the handler resolves
locally.

Give each node its own `reminder_data_dir`. A write made on a non-owning node
and not yet snapshotted there can be lost if that node dies abruptly, but the
node that set it flushed it, so no acknowledged reminder is lost cluster-wide.

Fire and cancel both leave a tombstone in the replicated store. A
periodic sweep drops tombstones older than `reminder_tombstone_ttl_ms`
(default one hour) so the store stays bounded. The horizon must comfortably
exceed gossip propagation plus the membership lease, so a delayed add can
never out-live the tombstone that cancelled it; the only way to defeat
that is a partition longer than the horizon replaying an add older than a
dropped tombstone, which would spuriously re-create the reminder.

## Setting and cancelling

```erlang
%% Fire at an absolute wall-clock instant (ms).
mycelium:remind(Key, FireAtMs, Payload).

%% Fire DelayMs from now (converted to an absolute target so all
%% nodes agree).
mycelium:remind_after(Key, DelayMs, Payload).

%% Cancel cluster-wide.
mycelium:cancel_reminder(Key).
```

Re-setting an existing `Key` replaces it with a fresh version, which
invalidates any timer already armed for the old one.

## Receiving

Subscribe on every node where the handler could run, since you do not
know in advance which node will own the key at fire time. The reminder
fires on the current owner and is delivered to that node's subscribers,
so the work runs there:

```erlang
init(_) ->
    ok = mycelium:subscribe_reminders(),
    {ok, #{}}.

handle_info({mycelium_reminder, Key, Payload, Fence}, S) ->
    case already_done(Fence) of
        true  -> {noreply, S};                 %% idempotent dedup
        false -> {noreply, run(Key, Payload, Fence, S)}
    end.
```

## Configuration

| Key                          | Default | Meaning                                            |
|------------------------------|---------|----------------------------------------------------|
| `reminder_scan_ms`           | 1000    | Safety sweep that re-arms owned reminders missed, and re-arms far-future ones as they near. |
| `reminder_tombstone_ttl_ms`  | 3600000 | Drop fire/cancel tombstones older than this. Must exceed gossip propagation plus `member_ttl_ms`. |
| `reminder_data_dir`          | `data/reminders` | Per-node directory for the on-disk store (WAL + snapshot). |

Reminders also depend on the placement settings (`ring_size` and the
lease timings); see [sharded placement](sharded-placement.md).

## Related

- [Sharded placement](sharded-placement.md) decides the owner and emits
  the ownership events that drive hand-off.
- [Hybrid logical clocks](hybrid-logical-clocks.md) mint the version
  stamp behind `Fence`.
- [Schedule durable jobs](../how-to/schedule-durable-jobs.md) is the
  worked recipe.
