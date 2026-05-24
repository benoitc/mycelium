%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_path_stats.
%%%
%%% The success path (sys:get_state on a live dist_controller +
%%% quic:get_path_stats on the conn pid) is exercised by
%%% mycelium_circuit_multinode_SUITE; here we just verify the error
%%% paths through the wrapper.

-module(mycelium_path_stats_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    meck:new(quic_dist, [non_strict]),
    ok.

teardown(_) ->
    meck:unload(quic_dist),
    ok.

with(Test) -> {setup, fun setup/0, fun teardown/1, Test}.

summary_propagates_not_connected_test_() ->
    with(fun() ->
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {error, not_connected} end
        ),
        ?assertEqual(
            {error, not_connected},
            mycelium_path_stats:summary('peer@h')
        )
    end).

summary_propagates_not_quic_test_() ->
    with(fun() ->
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {error, not_quic_connection} end
        ),
        ?assertEqual(
            {error, not_quic_connection},
            mycelium_path_stats:summary('peer@h')
        )
    end).

srtt_propagates_not_connected_test_() ->
    with(fun() ->
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {error, not_connected} end
        ),
        ?assertEqual(
            {error, not_connected},
            mycelium_path_stats:srtt('peer@h')
        )
    end).

extract_conn_failure_returns_no_conn_test_() ->
    with(fun() ->
        %% Return a stale/dead pid; extract_conn should fail cleanly.
        Dead = spawn(fun() -> ok end),
        timer:sleep(20),
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {ok, Dead} end
        ),
        ?assertEqual(
            {error, no_conn},
            mycelium_path_stats:summary('peer@h')
        )
    end).

connection_propagates_not_connected_test_() ->
    with(fun() ->
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {error, not_connected} end
        ),
        ?assertEqual(
            {error, not_connected},
            mycelium_path_stats:connection('peer@h')
        )
    end).

connection_returns_no_conn_on_dead_controller_test_() ->
    with(fun() ->
        Dead = spawn(fun() -> ok end),
        timer:sleep(20),
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {ok, Dead} end
        ),
        ?assertEqual(
            {error, no_conn},
            mycelium_path_stats:connection('peer@h')
        )
    end).
