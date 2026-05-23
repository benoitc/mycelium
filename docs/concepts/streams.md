# Streams

A QUIC connection multiplexes streams natively. Mycelium uses
one bidirectional stream for the Erlang dist control channel
(opened by upstream `quic_dist`), one pair of unidirectional
streams for the [Ed25519 authentication](authentication.md)
handshake, and *application-level* streams managed by
`mycelium_streams`.

This page is about that third category: the tagged user-stream
multiplex. It is the right tool when message passing is not
the right shape and you want a stream of bytes between two
nodes.

## When to reach for streams

`Pid ! Msg` is great for sending one message. It is poor for:

- A large blob that would block the dist control stream while
  it copies through.
- A long-running byte stream (a log feed, a snapshot upload, a
  video frame stream).
- An application protocol with its own framing where you do not
  want the overhead of Erlang term encoding.

Streams give you a separate QUIC stream over the same
connection. The dist control stream stays responsive; the
application stream has its own flow control; ownership is
explicit.

## The wire shape

Every mycelium-managed user stream starts with a short preamble:

```
<<TagLen:8, Tag:TagLen/binary, Payload/binary>>
```

The tag is a short binary identifier you choose for your
protocol (`<<"acme.snapshots">>`, `<<"chat:transcripts">>`).
The demultiplexer reads the first `1 + TagLen` bytes, looks up
the registered acceptor for the tag, and hands the stream to
that acceptor process. From then on the acceptor owns the
stream; mycelium is off the data path.

Reserved tags:

- `<<"mycelium:", _/binary>>` — reserved for future internals.

## Registering an acceptor

To receive streams under a tag, register an acceptor pid:

```erlang
mycelium_streams:register_acceptor(<<"my.protocol">>, self()).
```

The acceptor pid receives one
`{mstream, StreamRef, opened, FromNode}` message per inbound
stream, then native `{quic_dist_stream, StreamRef, _}` events
for the duration of the stream.

To unregister:

```erlang
mycelium_streams:unregister_acceptor(<<"my.protocol">>).
```

Only one acceptor per tag per node. Attempting to register a
second acceptor for an in-use tag returns `{error, conflict}`.

The registry monitors the acceptor pid: if it crashes, the
registration is automatically removed and inbound streams under
that tag are refused with a reset.

## Opening a stream

To open a stream to a peer:

```erlang
{ok, StreamRef} = mycelium_streams:open(<<"my.protocol">>, 'peer@host').
```

The call:

1. Opens a fresh QUIC stream over the existing dist channel
   (opening a dist channel on demand if there is none yet).
2. Sends the preamble (`<<TagLen:8, Tag:TagLen/binary>>`).
3. Returns the `StreamRef`. From this point on, you own the
   stream and use the upstream `quic_dist` API:

```erlang
quic_dist:send(StreamRef, <<"hello">>).
quic_dist:send(StreamRef, <<"more">>).
quic_dist:close_stream(StreamRef).
```

## Receiving data

After the acceptor receives `{mstream, StreamRef, opened, FromNode}`,
it receives the standard QUIC stream events:

```erlang
receive
    {quic_dist_stream, SR, {data, Bytes, _Fin}} ->
        process(Bytes);
    {quic_dist_stream, SR, closed} ->
        cleanup()
end.
```

The `Fin` flag indicates the sender called `close_stream/1`.

## Ownership transfer

By default, the registered acceptor owns the stream when it
opens. If you want a different process to own it (for example,
the acceptor is a dispatcher that hands the stream to a
per-connection gen_server), use the controlling-process
mechanism:

```erlang
quic_dist:controlling_process(StreamRef, NewOwnerPid).
```

After this, `NewOwnerPid` receives all subsequent
`{quic_dist_stream, _, _}` events.

## Back-pressure

QUIC streams have their own flow control. A slow consumer
causes the sender's `quic_dist:send/2` to block (or return
`{error, would_block}` depending on the upstream's mode). The
dist control stream is unaffected; only the slow stream itself
slows down.

This is the natural shape for "do not let one slow consumer
take down the cluster": the slow stream backs up; everything
else flows.

## The pending cap

The demultiplexer parks each inbound stream's tag preamble
buffer until the tag has been fully received. To prevent a
hostile peer from opening many streams and dripping bytes
without completing the preamble, mycelium caps the number of
in-flight pending streams at 64. Excess streams are reset; a
metric (`mycelium.streams.preamble_dropped`) tracks the rate.

In a healthy cluster the metric should be zero.

## Worked example

A simple "dump a transcript" protocol between two nodes:

```erlang
%% On the receiving node, register an acceptor.
DumpReceiver = spawn(fun receive_loop/0),
mycelium_streams:register_acceptor(<<"chat:dump">>, DumpReceiver).

receive_loop() ->
    receive
        {mstream, SR, opened, _FromNode} ->
            transcript_loop(SR, []);
        _ ->
            receive_loop()
    end.

transcript_loop(SR, Acc) ->
    receive
        {quic_dist_stream, SR, {data, Chunk, _Fin}} ->
            transcript_loop(SR, [Chunk | Acc]);
        {quic_dist_stream, SR, closed} ->
            Transcript = iolist_to_binary(lists:reverse(Acc)),
            store_transcript(Transcript),
            receive_loop()  %% Back to waiting for the next stream.
    end.
```

```erlang
%% On the sending node, open and send.
{ok, SR} = mycelium_streams:open(<<"chat:dump">>, 'peer@host'),
ok = quic_dist:send(SR, transcript_chunk_1()),
ok = quic_dist:send(SR, transcript_chunk_2()),
ok = quic_dist:close_stream(SR).
```

The two nodes never exchange a control message at the Erlang
dist layer; the entire interaction is on the application
stream.

## When not to use streams

Streams are powerful but more complex than a `gen_server:call`:

- For small messages, `gen_server:call` over the dist channel
  is faster to write and reason about.
- For request/response, an RPC-shaped pattern with the service
  registry is usually the right shape.
- For broadcasting to many peers, use the [service registry
  events](service-registry.md) or build on top of
  [gossip broadcast](gossip-broadcast.md).

Streams shine when:

- One direction carries a lot of bytes.
- The protocol is already designed as a byte stream.
- You want the dist control stream to stay responsive while
  the bulk transfer runs in the background.

## API

```erlang
mycelium_streams:register_acceptor(Tag, Pid) -> ok | {error, conflict}.
mycelium_streams:unregister_acceptor(Tag) -> ok.
mycelium_streams:open(Tag, Node) -> {ok, StreamRef} | {error, term()}.
mycelium_streams:list_acceptors() -> [{Tag, Pid}].
```

The streams subsystem is marked `beta` in
[features.md](../features.md). Wire-protocol changes are
flagged in the CHANGELOG.

## Related

- [Dist channel](dist-channel.md) explains the QUIC connection
  the streams ride on.
- [Connection migration](connection-migration.md) covers what
  happens to in-flight streams when the underlying connection
  moves to a new network path (they ride through).
- [Distributed chat tutorial](../tutorials/distributed-chat.md)
  introduces streams in the "when message-passing is not the
  right shape" section.
