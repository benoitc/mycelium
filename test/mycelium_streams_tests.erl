%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_streams.
%%%
%%% Pure in-VM tests; quic_dist is mecked.

-module(mycelium_streams_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    meck:new(quic_dist, [non_strict]),
    meck:expect(quic_dist, accept_streams, fun(_Node) -> ok end),
    meck:expect(quic_dist, controlling_process, fun(_SR, _Pid) -> ok end),
    meck:expect(quic_dist, reset_stream, fun(_SR, _Code) -> ok end),
    meck:expect(quic_dist, send, fun(_SR, _Data) -> ok end),
    meck:expect(quic_dist, close_stream, fun(_SR) -> ok end),
    meck:expect(quic_dist, open_stream, fun(Node) ->
        {ok, {quic_dist_stream, Node, rand:uniform(10000)}}
    end),
    {ok, _Pid} = mycelium_streams:start_link(),
    ok.

teardown(_) ->
    catch gen_server:stop(mycelium_streams),
    meck:unload(quic_dist),
    ok.

with_streams(Test) ->
    {setup, fun setup/0, fun teardown/1, Test}.

%%====================================================================
%% Tests
%%====================================================================

register_acceptor_test_() ->
    with_streams(fun() ->
        ok = mycelium_streams:register_acceptor(<<"my:tag">>, self()),
        [{<<"my:tag">>, Self}] = mycelium_streams:list_acceptors(),
        ?assertEqual(self(), Self)
    end).

register_acceptor_conflict_test_() ->
    with_streams(fun() ->
        Other = spawn(fun() ->
            receive
                _ -> ok
            end
        end),
        ok = mycelium_streams:register_acceptor(<<"a">>, self()),
        ?assertEqual(
            {error, conflict},
            mycelium_streams:register_acceptor(<<"a">>, Other)
        ),
        exit(Other, kill)
    end).

register_acceptor_idempotent_test_() ->
    with_streams(fun() ->
        ok = mycelium_streams:register_acceptor(<<"a">>, self()),
        ok = mycelium_streams:register_acceptor(<<"a">>, self()),
        [_] = mycelium_streams:list_acceptors()
    end).

unregister_acceptor_test_() ->
    with_streams(fun() ->
        ok = mycelium_streams:register_acceptor(<<"a">>, self()),
        ok = mycelium_streams:unregister_acceptor(<<"a">>),
        [] = mycelium_streams:list_acceptors()
    end).

handler_down_drops_registration_test_() ->
    with_streams(fun() ->
        Other = spawn(fun() -> ok end),
        Ref = monitor(process, Other),
        receive
            {'DOWN', Ref, process, Other, _} -> ok
        after 1000 -> ?assert(false)
        end,
        ok = mycelium_streams:register_acceptor(<<"x">>, Other),
        %% Give the gen_server a chance to process the DOWN.
        timer:sleep(50),
        ?assertEqual([], mycelium_streams:list_acceptors())
    end).

open_writes_tag_preamble_test_() ->
    with_streams(fun() ->
        meck:reset(quic_dist),
        meck:expect(
            quic_dist,
            open_stream,
            fun(_) -> {ok, {quic_dist_stream, 'n@h', 1}} end
        ),
        meck:expect(quic_dist, send, fun(_SR, _Data) -> ok end),
        meck:expect(quic_dist, close_stream, fun(_) -> ok end),
        {ok, SR} = mycelium_streams:open(<<"foo">>, 'n@h'),
        ?assertMatch({quic_dist_stream, 'n@h', 1}, SR),
        %% First send must be the tag preamble: <<3, "foo">>.
        Hist = meck:history(quic_dist),
        Sends = [Args || {_, {quic_dist, send, Args}, ok} <- Hist],
        ?assert(
            lists:any(
                fun
                    ([_, <<3, "foo">>]) -> true;
                    (_) -> false
                end,
                Sends
            )
        )
    end).

dispatch_known_tag_handoff_test_() ->
    with_streams(fun() ->
        ok = mycelium_streams:register_acceptor(<<"my:tag">>, self()),
        SR = {quic_dist_stream, 'peer@h', 7},
        Preamble = <<6, "my:tag">>,
        Payload = <<"first chunk">>,
        Demuxer = whereis(mycelium_streams),
        Demuxer ! {quic_dist_stream, SR, {data, <<Preamble/binary, Payload/binary>>, false}},
        receive
            {mstream, SR, opened, 'peer@h'} -> ok
        after 1000 ->
            ?assert(false)
        end,
        receive
            {quic_dist_stream, SR, {data, <<"first chunk">>, false}} -> ok
        after 1000 ->
            ?assert(false)
        end
    end).

dispatch_unknown_tag_resets_test_() ->
    with_streams(fun() ->
        meck:reset(quic_dist),
        meck:expect(quic_dist, reset_stream, fun(_, _) -> ok end),
        SR = {quic_dist_stream, 'peer@h', 1},
        Preamble = <<6, "no:one">>,
        Demuxer = whereis(mycelium_streams),
        Demuxer ! {quic_dist_stream, SR, {data, Preamble, false}},
        timer:sleep(50),
        Hist = meck:history(quic_dist),
        ResetCalls = [
            Args
         || {_, {quic_dist, reset_stream, Args}, ok} <- Hist
        ],
        ?assert(length(ResetCalls) >= 1)
    end).

partial_preamble_buffers_test_() ->
    with_streams(fun() ->
        ok = mycelium_streams:register_acceptor(<<"my:tag">>, self()),
        SR = {quic_dist_stream, 'peer@h', 9},
        Demuxer = whereis(mycelium_streams),
        %% Send only the TagLen byte first; no full tag yet.
        Demuxer ! {quic_dist_stream, SR, {data, <<6>>, false}},
        receive
            {mstream, SR, opened, _} -> ?assert(false)
        after 100 -> ok
        end,
        %% Now send the rest. Should now dispatch.
        Demuxer ! {quic_dist_stream, SR, {data, <<"my:tag", "ok">>, false}},
        receive
            {mstream, SR, opened, 'peer@h'} -> ok
        after 1000 ->
            ?assert(false)
        end,
        receive
            {quic_dist_stream, SR, {data, <<"ok">>, false}} -> ok
        after 1000 ->
            ?assert(false)
        end
    end).

fin_only_after_tag_preserves_close_test_() ->
    with_streams(fun() ->
        ok = mycelium_streams:register_acceptor(<<"t">>, self()),
        SR = {quic_dist_stream, 'p@h', 3},
        Demuxer = whereis(mycelium_streams),
        %% Tag fits exactly in the first chunk; no app data; Fin=true.
        Demuxer ! {quic_dist_stream, SR, {data, <<1, "t">>, true}},
        receive
            {mstream, SR, opened, 'p@h'} -> ok
        after 1000 -> ?assert(false)
        end,
        %% Even with empty Rest, the Fin must reach the handler.
        receive
            {quic_dist_stream, SR, {data, <<>>, true}} -> ok
        after 1000 -> ?assert(false)
        end
    end).

%% A peer that opens many streams and never completes any tag
%% preamble must not grow `pending' without bound. After 64 incomplete
%% preambles are parked, the next new stream is refused.
pending_stream_cap_refuses_new_streams_test_() ->
    with_streams(fun() ->
        Self = self(),
        meck:expect(
            quic_dist,
            close_stream,
            fun(SR0) ->
                Self ! {close, SR0},
                ok
            end
        ),
        Demuxer = whereis(mycelium_streams),
        %% Park 64 incomplete preambles (single-byte data, no Tag).
        %% Each delivers TagLen=255 (or similar) without enough bytes
        %% to complete, so each enters `pending'.
        [
            Demuxer ! {quic_dist_stream, {quic_dist_stream, 'p@h', I}, {data, <<255>>, false}}
         || I <- lists:seq(1, 64)
        ],
        _ = sys:get_state(Demuxer),
        %% The 65th new stream should be refused; no `mstream' event
        %% reaches us and the demuxer calls quic_dist:close_stream.
        Excess = {quic_dist_stream, 'p@h', 999},
        Demuxer ! {quic_dist_stream, Excess, {data, <<255>>, false}},
        receive
            {close, Excess} -> ok
        after 1000 ->
            ?assert(false)
        end
    end).
