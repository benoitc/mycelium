%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_sync_bench).

%% Benchmark module for registry sync performance
%% Run with: rebar3 as test shell, then mycelium_sync_bench:run().

-export([run/0, run/1]).
-export([bench_broadcast/1, bench_registration/1, bench_events/1]).

-define(DEFAULT_ITERATIONS, 1000).

%%====================================================================
%% API
%%====================================================================

run() ->
    run(#{iterations => ?DEFAULT_ITERATIONS}).

run(Opts) ->
    io:format("~n=== Mycelium Sync Benchmark ===~n~n"),

    %% Ensure application is started
    {ok, _} = application:ensure_all_started(mycelium),

    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS),

    Results = [
        {registration, bench_registration(Iterations)},
        {events, bench_events(Iterations)},
        {broadcast, bench_broadcast(Iterations)}
    ],

    io:format("~n=== Summary ===~n"),
    lists:foreach(
        fun({Name, {Total, PerOp}}) ->
            io:format(
                "~-20s: ~8.2f ms total, ~8.2f us/op~n",
                [Name, Total / 1000, PerOp]
            )
        end,
        Results
    ),

    application:stop(mycelium),
    Results.

%%====================================================================
%% Benchmarks
%%====================================================================

%% Benchmark service registration/unregistration
bench_registration(Iterations) ->
    io:format("Benchmarking registration (~p iterations)...~n", [Iterations]),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(
            fun(I) ->
                Name = list_to_atom("bench_svc_" ++ integer_to_list(I)),
                ok = mycelium:register_service(Name),
                ok = mycelium:unregister_service(Name)
            end,
            lists:seq(1, Iterations)
        )
    end),

    %% 2 ops per iteration
    PerOp = Time / (Iterations * 2),
    io:format("  Registration: ~.2f ms total, ~.2f us/op~n", [Time / 1000, PerOp]),
    {Time, PerOp}.

%% Benchmark event delivery
bench_events(Iterations) ->
    io:format("Benchmarking event delivery (~p iterations)...~n", [Iterations]),

    %% Subscribe
    ok = mycelium:subscribe_services(),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(
            fun(I) ->
                Name = list_to_atom("event_svc_" ++ integer_to_list(I)),
                ok = mycelium:register_service(Name),
                receive
                    {mycelium_service_event, {service_registered, Name, _}} -> ok
                after 1000 ->
                    error({timeout, Name})
                end,
                ok = mycelium:unregister_service(Name),
                receive
                    {mycelium_service_event, {service_unregistered, Name, _}} -> ok
                after 1000 ->
                    error({timeout_unreg, Name})
                end
            end,
            lists:seq(1, Iterations)
        )
    end),

    ok = mycelium:unsubscribe_services(self()),

    PerOp = Time / (Iterations * 2),
    io:format("  Events: ~.2f ms total, ~.2f us/op~n", [Time / 1000, PerOp]),
    {Time, PerOp}.

%% Benchmark broadcast (simulated peer sync)
bench_broadcast(Iterations) ->
    io:format("Benchmarking broadcast (~p iterations)...~n", [Iterations]),

    %% Register some services to broadcast
    lists:foreach(
        fun(I) ->
            Name = list_to_atom("broadcast_svc_" ++ integer_to_list(I)),
            mycelium:register_service(Name)
        end,
        lists:seq(1, 10)
    ),

    {Time, _} = timer:tc(fun() ->
        lists:foreach(
            fun(I) ->
                Name = list_to_atom("broadcast_test_" ++ integer_to_list(I)),
                %% This triggers broadcast_update internally
                ok = mycelium:register_service(Name),
                ok = mycelium:unregister_service(Name)
            end,
            lists:seq(1, Iterations)
        )
    end),

    %% Cleanup
    lists:foreach(
        fun(I) ->
            Name = list_to_atom("broadcast_svc_" ++ integer_to_list(I)),
            mycelium:unregister_service(Name)
        end,
        lists:seq(1, 10)
    ),

    PerOp = Time / (Iterations * 2),
    io:format("  Broadcast: ~.2f ms total, ~.2f us/op~n", [Time / 1000, PerOp]),
    {Time, PerOp}.
