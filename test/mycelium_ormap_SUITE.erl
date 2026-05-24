%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_ormap_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    test_new_is_empty/1,
    test_add_and_get/1,
    test_add_overwrites/1,
    test_remove/1,
    test_keys/1,
    test_to_list/1,
    test_merge_disjoint/1,
    test_merge_overlapping/1,
    test_merge_idempotent/1,
    test_merge_commutative/1,
    test_merge_associative/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, ormap}].

groups() ->
    [
        {ormap, [sequence], [
            test_new_is_empty,
            test_add_and_get,
            test_add_overwrites,
            test_remove,
            test_keys,
            test_to_list,
            test_merge_disjoint,
            test_merge_overlapping,
            test_merge_idempotent,
            test_merge_commutative,
            test_merge_associative
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

test_new_is_empty(_Config) ->
    Map = mycelium_ormap:new(),
    ?assert(mycelium_ormap:is_empty(Map)),
    ?assertEqual([], mycelium_ormap:keys(Map)),
    ok.

test_add_and_get(_Config) ->
    Map0 = mycelium_ormap:new(),
    Map1 = mycelium_ormap:add(foo, bar, Map0),
    ?assertEqual({ok, bar}, mycelium_ormap:get(foo, Map1)),
    ?assertEqual(not_found, mycelium_ormap:get(baz, Map1)),
    ok.

test_add_overwrites(_Config) ->
    Map0 = mycelium_ormap:new(),
    Map1 = mycelium_ormap:add(key, value1, Map0),
    Map2 = mycelium_ormap:add(key, value2, Map1),
    %% Latest add wins
    ?assertEqual({ok, value2}, mycelium_ormap:get(key, Map2)),
    ok.

test_remove(_Config) ->
    Map0 = mycelium_ormap:new(),
    Map1 = mycelium_ormap:add(key, value, Map0),
    Map2 = mycelium_ormap:remove(key, Map1),
    ?assertEqual(not_found, mycelium_ormap:get(key, Map2)),
    ?assert(mycelium_ormap:is_empty(Map2)),
    ok.

test_keys(_Config) ->
    Map0 = mycelium_ormap:new(),
    Map1 = mycelium_ormap:add(a, 1, Map0),
    Map2 = mycelium_ormap:add(b, 2, Map1),
    Map3 = mycelium_ormap:add(c, 3, Map2),
    Keys = mycelium_ormap:keys(Map3),
    ?assertEqual(3, length(Keys)),
    ?assert(lists:member(a, Keys)),
    ?assert(lists:member(b, Keys)),
    ?assert(lists:member(c, Keys)),
    ok.

test_to_list(_Config) ->
    Map0 = mycelium_ormap:new(),
    Map1 = mycelium_ormap:add(x, 10, Map0),
    Map2 = mycelium_ormap:add(y, 20, Map1),
    List = mycelium_ormap:to_list(Map2),
    ?assertEqual(2, length(List)),
    ?assert(lists:member({x, 10}, List)),
    ?assert(lists:member({y, 20}, List)),
    ok.

test_merge_disjoint(_Config) ->
    %% Two maps with different keys
    Map1 = mycelium_ormap:add(a, 1, mycelium_ormap:new()),
    Map2 = mycelium_ormap:add(b, 2, mycelium_ormap:new()),
    Merged = mycelium_ormap:merge(Map1, Map2),
    ?assertEqual({ok, 1}, mycelium_ormap:get(a, Merged)),
    ?assertEqual({ok, 2}, mycelium_ormap:get(b, Merged)),
    ok.

test_merge_overlapping(_Config) ->
    %% Two maps with same key, latest HLC wins
    Map1 = mycelium_ormap:add(key, old_value, mycelium_ormap:new()),
    %% Ensure different HLC
    timer:sleep(1),
    Map2 = mycelium_ormap:add(key, new_value, mycelium_ormap:new()),
    %% Map2 was created later, so new_value should win
    Merged = mycelium_ormap:merge(Map1, Map2),
    ?assertEqual({ok, new_value}, mycelium_ormap:get(key, Merged)),
    ok.

test_merge_idempotent(_Config) ->
    %% merge(A, A) == A
    Map = mycelium_ormap:add(key, value, mycelium_ormap:new()),
    Merged = mycelium_ormap:merge(Map, Map),
    ?assertEqual(mycelium_ormap:to_list(Map), mycelium_ormap:to_list(Merged)),
    ok.

test_merge_commutative(_Config) ->
    %% merge(A, B) == merge(B, A) (in terms of values)
    Map1 = mycelium_ormap:add(a, 1, mycelium_ormap:new()),
    Map2 = mycelium_ormap:add(b, 2, mycelium_ormap:new()),
    MergedAB = mycelium_ormap:merge(Map1, Map2),
    MergedBA = mycelium_ormap:merge(Map2, Map1),
    %% Same keys and values
    ?assertEqual(
        lists:sort(mycelium_ormap:to_list(MergedAB)),
        lists:sort(mycelium_ormap:to_list(MergedBA))
    ),
    ok.

test_merge_associative(_Config) ->
    %% merge(merge(A, B), C) == merge(A, merge(B, C))
    Map1 = mycelium_ormap:add(a, 1, mycelium_ormap:new()),
    Map2 = mycelium_ormap:add(b, 2, mycelium_ormap:new()),
    Map3 = mycelium_ormap:add(c, 3, mycelium_ormap:new()),
    Merged1 = mycelium_ormap:merge(mycelium_ormap:merge(Map1, Map2), Map3),
    Merged2 = mycelium_ormap:merge(Map1, mycelium_ormap:merge(Map2, Map3)),
    ?assertEqual(
        lists:sort(mycelium_ormap:to_list(Merged1)),
        lists:sort(mycelium_ormap:to_list(Merged2))
    ),
    ok.
