%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium:migrate_peer/1,2.
%%%
%%% The actual RFC 9000 §9 path-validation handshake belongs to
%%% upstream `quic_connection'; here we only verify the wrapper
%%% threads errors through and forwards options to `quic:migrate/2'.
%%%
%%% Stubs: `quic_dist:get_controller/1' (meck) and `quic:migrate/2'
%%% (meck). The dist-controller pid is a real gen_server whose state
%%% places the "connection" pid at element 2 (matching the upstream
%%% record shape that `mycelium_path_stats:extract_conn/1' relies on).

-module(mycelium_migrate_peer_tests).

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

%% gen_server callbacks for the fake dist controller.
-export([init/1, handle_call/3, handle_cast/2]).

setup() ->
    meck:new(quic_dist, [non_strict]),
    meck:new(quic, [non_strict]),
    ok.

teardown(_) ->
    meck:unload(quic),
    meck:unload(quic_dist),
    ok.

with(Test) -> {setup, fun setup/0, fun teardown/1, Test}.

migrate_peer_propagates_not_connected_test_() ->
    with(fun() ->
        meck:expect(
            quic_dist,
            get_controller,
            fun(_) -> {error, not_connected} end
        ),
        ?assertEqual(
            {error, not_connected},
            mycelium:migrate_peer('peer@h')
        )
    end).

migrate_peer_propagates_no_conn_on_dead_controller_test_() ->
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
            mycelium:migrate_peer('peer@h')
        )
    end).

migrate_peer_calls_quic_migrate_with_opts_test_() ->
    with(fun() ->
        Self = self(),
        Conn = spawn_link(fun() -> conn_loop() end),
        {ok, Ctrl} = gen_server:start_link(?MODULE, Conn, []),
        meck:expect(quic_dist, get_controller, fun(_) -> {ok, Ctrl} end),
        meck:expect(
            quic,
            migrate,
            fun(C, Opts) ->
                Self ! {migrate_called, C, Opts},
                ok
            end
        ),
        ?assertEqual(
            ok,
            mycelium:migrate_peer(
                'peer@h',
                #{timeout => 1500}
            )
        ),
        receive
            {migrate_called, GotConn, Opts} ->
                ?assertEqual(Conn, GotConn),
                ?assertEqual(#{timeout => 1500}, Opts)
        after 200 ->
            erlang:error(migrate_not_called)
        end,
        unlink(Ctrl),
        exit(Ctrl, kill),
        unlink(Conn),
        exit(Conn, kill)
    end).

migrate_peer_propagates_peer_disable_migration_test_() ->
    with(fun() ->
        Conn = spawn_link(fun() -> conn_loop() end),
        {ok, Ctrl} = gen_server:start_link(?MODULE, Conn, []),
        meck:expect(quic_dist, get_controller, fun(_) -> {ok, Ctrl} end),
        meck:expect(
            quic,
            migrate,
            fun(_, _) -> {error, peer_disable_migration} end
        ),
        ?assertEqual(
            {error, peer_disable_migration},
            mycelium:migrate_peer('peer@h')
        ),
        unlink(Ctrl),
        exit(Ctrl, kill),
        unlink(Conn),
        exit(Conn, kill)
    end).

%%====================================================================
%% Fake dist controller — state is `{state, Conn, undefined}`, so
%% sys:get_state returns a tuple whose element 2 is Conn (matching
%% the upstream `quic_dist_controller` record layout).
%%====================================================================

init(Conn) ->
    {ok, {state, Conn, undefined}}.

handle_call(_, _, S) -> {reply, ok, S}.
handle_cast(_, S) -> {noreply, S}.

conn_loop() ->
    receive
        _ -> conn_loop()
    end.
