%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Property tests for mycelium_streams.
%%%
%%% Locks in the multiplex contract under random fragmentation: for
%%% every tag, payload, and chunking of `<<Preamble/binary,
%%% Payload/binary>>', the registered handler receives exactly one
%%% `opened' followed by `{data, _, _}' events whose concatenated
%%% payload equals the original.
%%%
%%% Uses meck to stub quic_dist; runs entirely in the test VM.

-module(mycelium_streams_prop_tests).

-include_lib("eunit/include/eunit.hrl").
%% eunit and proper both define ?LET; proper's is the one we use here.
-undef('LET').
-include_lib("proper/include/proper.hrl").

-define(NUMTESTS, 100).

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    meck:new(quic_dist, [non_strict]),
    meck:expect(quic_dist, accept_streams, fun(_) -> ok end),
    meck:expect(quic_dist, controlling_process, fun(_, _) -> ok end),
    meck:expect(quic_dist, reset_stream, fun(_, _) -> ok end),
    meck:expect(quic_dist, send, fun(_, _) -> ok end),
    meck:expect(quic_dist, close_stream, fun(_) -> ok end),
    {ok, _} = mycelium_streams:start_link(),
    ok.

teardown(_) ->
    catch gen_server:stop(mycelium_streams),
    catch meck:unload(quic_dist),
    ok.

prop_test_() ->
    {setup, fun setup/0, fun teardown/1, [
        {timeout, 60, ?_assert(run(prop_dispatch_fragments_preserve_payload()))}
    ]}.

run(Prop) ->
    proper:quickcheck(Prop, [{numtests, ?NUMTESTS}, {to_file, user}]).

%%====================================================================
%% Generators
%%====================================================================

%% Tag is 1..32 random non-zero bytes.
tag() ->
    ?LET(
        N,
        choose(1, 32),
        ?LET(
            Bs,
            vector(N, choose(1, 255)),
            list_to_binary(Bs)
        )
    ).

payload() ->
    ?LET(
        N,
        choose(0, 200),
        ?LET(
            Bs,
            vector(N, choose(0, 255)),
            list_to_binary(Bs)
        )
    ).

%% Split a binary into 1..6 chunks at random offsets. Returns the
%% list of chunks; concatenated they reproduce the input exactly.
chunking(<<>>) ->
    [<<>>];
chunking(Bin) ->
    ?LET(
        NChunks,
        choose(1, 6),
        split_n(Bin, NChunks)
    ).

split_n(Bin, 1) ->
    [Bin];
split_n(Bin, N) ->
    Size = byte_size(Bin),
    ?LET(
        Cut,
        choose(1, max(1, Size)),
        begin
            Cut1 = min(Cut, Size),
            <<H:Cut1/binary, T/binary>> = Bin,
            [H | split_n(T, N - 1)]
        end
    ).

%%====================================================================
%% Property
%%====================================================================

prop_dispatch_fragments_preserve_payload() ->
    ?FORALL(
        {Tag, Payload},
        {tag(), payload()},
        ?FORALL(
            Chunks,
            chunking(<<(byte_size(Tag)):8, Tag/binary, Payload/binary>>),
            check_dispatch(Tag, Payload, Chunks)
        )
    ).

%%====================================================================
%% Driver
%%====================================================================

check_dispatch(Tag, Payload, Chunks) ->
    %% Each property iteration must start from a clean slate; reset the
    %% acceptor registry and the demuxer's pending-buffer map.
    Self = self(),
    drain(),
    ok = mycelium_streams:unregister_acceptor(Tag),
    ok = mycelium_streams:register_acceptor(Tag, Self),
    SR = {quic_dist_stream, 'peer@h', erlang:unique_integer([positive, monotonic])},
    Demuxer = whereis(mycelium_streams),
    Total = length(Chunks),
    %% In production the dist controller is the single serial router for a
    %% stream: it delivers every pre-handoff chunk to the demuxer before
    %% the synchronous controlling_process/2 call returns, so the demuxer's
    %% post-dispatch drain (`drain_to', `after 0') deterministically sees
    %% them all. This test delivers chunks straight to the demuxer with no
    %% controller in between, so we reproduce that ordering by suspending
    %% the demuxer while we enqueue, then resuming: all chunks are in its
    %% mailbox before it processes the preamble-completing one. Without
    %% this, the demuxer could dispatch and drain before later chunks
    %% arrived, then mis-read them as a new preamble.
    sys:suspend(Demuxer),
    lists:foldl(
        fun(Chunk, I) ->
            Fin = (I =:= Total),
            Demuxer ! {quic_dist_stream, SR, {data, Chunk, Fin}},
            I + 1
        end,
        1,
        Chunks
    ),
    sys:resume(Demuxer),
    Opened = wait_opened(SR),
    Got = collect_data(SR, <<>>),
    ok = mycelium_streams:unregister_acceptor(Tag),
    Opened andalso (Got =:= Payload).

wait_opened(SR) ->
    receive
        {mstream, SR, opened, _} -> true
    after 1000 ->
        false
    end.

collect_data(SR, Acc) ->
    receive
        {quic_dist_stream, SR, {data, Bin, true}} ->
            <<Acc/binary, Bin/binary>>;
        {quic_dist_stream, SR, {data, Bin, false}} ->
            collect_data(SR, <<Acc/binary, Bin/binary>>)
    after 1000 ->
        %% Whatever we have so far; the property will compare.
        Acc
    end.

drain() ->
    receive
        _ -> drain()
    after 0 -> ok
    end.
