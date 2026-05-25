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

%%====================================================================
%% Success and scan/fallback paths
%%
%% extract_conn/1 reads the controller's gen_statem state via
%% sys:get_state. We stand in a real gen_server (mycelium_test_server)
%% and drive its state with sys:replace_state so the accessor sees a
%% controlled tuple, then meck quic:get_path_stats for the conn pid.
%%====================================================================

stats() ->
    #{
        srtt => 4321,
        latest_rtt => 4000,
        min_rtt => 3000,
        rtt_var => 200,
        cwnd => 14600,
        bytes_in_flight => 0,
        in_recovery => false,
        congested => false
    }.

%% A live process standing in for the QUIC connection pid.
live_pid() ->
    spawn(fun() ->
        receive
            stop -> ok
        end
    end).

%% A fake dist controller whose state tuple we set directly.
fake_controller(StateTuple) ->
    {ok, Ctrl} = mycelium_test_server:start_link(),
    _ = sys:replace_state(Ctrl, fun(_) -> StateTuple end),
    Ctrl.

%% Fast path: position 2 of the controller state is the live conn pid.
summary_fast_path_returns_stats_test() ->
    meck:new(quic_dist, [non_strict]),
    meck:new(quic, [non_strict]),
    Conn = live_pid(),
    Ctrl = fake_controller({state, Conn, extra}),
    try
        Stats = stats(),
        meck:expect(quic_dist, get_controller, fun(_) -> {ok, Ctrl} end),
        meck:expect(quic, get_path_stats, fun
            (P) when P =:= Conn -> {ok, Stats};
            (_) -> {error, wrong_pid}
        end),
        ?assertEqual({ok, Stats}, mycelium_path_stats:summary('peer@h')),
        ?assertEqual({ok, 4321}, mycelium_path_stats:srtt('peer@h')),
        ?assertEqual({ok, Conn}, mycelium_path_stats:connection('peer@h'))
    after
        gen_server:stop(Ctrl),
        Conn ! stop,
        meck:unload(quic),
        meck:unload(quic_dist)
    end.

%% Scan path: position 2 is not a pid, so the accessor scans the rest of
%% the tuple for a pid that answers quic:get_path_stats/1.
summary_scan_path_finds_conn_test() ->
    meck:new(quic_dist, [non_strict]),
    meck:new(quic, [non_strict]),
    Conn = live_pid(),
    Ctrl = fake_controller({state, not_a_pid, Conn}),
    try
        Stats = stats(),
        meck:expect(quic_dist, get_controller, fun(_) -> {ok, Ctrl} end),
        meck:expect(quic, get_path_stats, fun
            (P) when P =:= Conn -> {ok, Stats};
            (_) -> {error, wrong_pid}
        end),
        ?assertEqual({ok, Stats}, mycelium_path_stats:summary('peer@h')),
        ?assertEqual({ok, Conn}, mycelium_path_stats:connection('peer@h'))
    after
        gen_server:stop(Ctrl),
        Conn ! stop,
        meck:unload(quic),
        meck:unload(quic_dist)
    end.

%% Scan exhausts: the only live pid in the state does not answer
%% get_path_stats, so the accessor gives up with {error, no_conn}.
summary_scan_exhausted_returns_no_conn_test() ->
    meck:new(quic_dist, [non_strict]),
    meck:new(quic, [non_strict]),
    Conn = live_pid(),
    Ctrl = fake_controller({state, not_a_pid, Conn}),
    try
        meck:expect(quic_dist, get_controller, fun(_) -> {ok, Ctrl} end),
        meck:expect(quic, get_path_stats, fun(_) -> {error, closed} end),
        ?assertEqual({error, no_conn}, mycelium_path_stats:summary('peer@h'))
    after
        gen_server:stop(Ctrl),
        Conn ! stop,
        meck:unload(quic),
        meck:unload(quic_dist)
    end.
