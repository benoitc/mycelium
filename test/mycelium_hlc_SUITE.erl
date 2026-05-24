%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_hlc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    test_now_returns_timestamp/1,
    test_now_advances/1,
    test_update_advances_clock/1,
    test_compare_equal/1,
    test_compare_less_than/1,
    test_compare_greater_than/1,
    test_binary_roundtrip/1,
    test_wall_time_accessor/1,
    test_logical_accessor/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, hlc}].

groups() ->
    [
        {hlc, [sequence], [
            test_now_returns_timestamp,
            test_now_advances,
            test_update_advances_clock,
            test_compare_equal,
            test_compare_less_than,
            test_compare_greater_than,
            test_binary_roundtrip,
            test_wall_time_accessor,
            test_logical_accessor
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, _} = application:ensure_all_started(mycelium),
    Config.

end_per_testcase(_TestCase, _Config) ->
    application:stop(mycelium),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_now_returns_timestamp(_Config) ->
    #timestamp{wall_time = Wall, logical = Logical} = mycelium_hlc:now(),
    ?assert(is_integer(Wall)),
    ?assert(is_integer(Logical)),
    ?assert(Wall > 0),
    ok.

test_now_advances(_Config) ->
    T1 = mycelium_hlc:now(),
    T2 = mycelium_hlc:now(),
    %% Each call should advance
    ?assertNotEqual(T1, T2),
    %% T2 should be greater
    ?assertEqual(gt, mycelium_hlc:compare(T2, T1)),
    ok.

test_update_advances_clock(_Config) ->
    T1 = mycelium_hlc:now(),
    %% Create a "future" timestamp
    #timestamp{wall_time = Wall, logical = Logical} = T1,
    FutureTs = #timestamp{wall_time = Wall + 10000, logical = Logical + 100},
    %% Update with the future timestamp
    T2 = mycelium_hlc:update(FutureTs),
    %% The updated clock should be at least as recent as the future timestamp
    ?assertNotEqual(lt, mycelium_hlc:compare(T2, FutureTs)),
    ok.

test_compare_equal(_Config) ->
    T1 = #timestamp{wall_time = 1000, logical = 5},
    T2 = #timestamp{wall_time = 1000, logical = 5},
    ?assertEqual(eq, mycelium_hlc:compare(T1, T2)),
    ok.

test_compare_less_than(_Config) ->
    T1 = #timestamp{wall_time = 1000, logical = 5},
    T2 = #timestamp{wall_time = 1001, logical = 0},
    ?assertEqual(lt, mycelium_hlc:compare(T1, T2)),
    %% Also test logical comparison when wall times equal
    T3 = #timestamp{wall_time = 1000, logical = 5},
    T4 = #timestamp{wall_time = 1000, logical = 6},
    ?assertEqual(lt, mycelium_hlc:compare(T3, T4)),
    ok.

test_compare_greater_than(_Config) ->
    T1 = #timestamp{wall_time = 2000, logical = 0},
    T2 = #timestamp{wall_time = 1000, logical = 100},
    ?assertEqual(gt, mycelium_hlc:compare(T1, T2)),
    ok.

test_binary_roundtrip(_Config) ->
    Original = mycelium_hlc:now(),
    Binary = mycelium_hlc:to_binary(Original),
    %% 8 bytes wall + 4 bytes logical
    ?assertEqual(12, byte_size(Binary)),
    Recovered = mycelium_hlc:from_binary(Binary),
    ?assertEqual(Original, Recovered),
    ok.

test_wall_time_accessor(_Config) ->
    TS = #timestamp{wall_time = 12345, logical = 67},
    ?assertEqual(12345, mycelium_hlc:wall_time(TS)),
    ok.

test_logical_accessor(_Config) ->
    TS = #timestamp{wall_time = 12345, logical = 67},
    ?assertEqual(67, mycelium_hlc:logical(TS)),
    ok.
