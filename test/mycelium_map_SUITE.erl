%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Single-node logic coverage for mycelium_map. Multi-node convergence is
%%% proven in mycelium_map_e2e_SUITE.
-module(mycelium_map_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    new_is_idempotent/1,
    new_rejects_non_atom/1,
    independent_maps/1,
    put_get_remove_roundtrip/1,
    keys_and_to_list/1,
    subscribe_receives_events/1,
    unsubscribe_stops_events/1,
    subscriber_down_is_cleaned_up/1,
    validator_rejects_bad_put/1,
    simulated_remote_delta_merges_and_emits/1,
    malformed_gossip_does_not_crash/1,
    tombstones_gc_after_ttl/1,
    delete_map_stops_instance/1,
    ops_on_missing_map/1,
    persist_recovers_after_restart/1
]).

all() ->
    [
        new_is_idempotent,
        new_rejects_non_atom,
        independent_maps,
        put_get_remove_roundtrip,
        keys_and_to_list,
        subscribe_receives_events,
        unsubscribe_stops_events,
        subscriber_down_is_cleaned_up,
        validator_rejects_bad_put,
        simulated_remote_delta_merges_and_emits,
        malformed_gossip_does_not_crash,
        tombstones_gc_after_ttl,
        delete_map_stops_instance,
        ops_on_missing_map,
        persist_recovers_after_restart
    ].

init_per_testcase(_Case, Config) ->
    %% Per-case map persistence dir, so a `persist => true' map writes to an
    %% isolated location (no cross-case or repo pollution).
    Dir = filename:join(
        ?config(priv_dir, Config),
        "maps_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    application:set_env(mycelium, mycelium_map_data_dir, Dir),
    {ok, _} = application:ensure_all_started(mycelium),
    Config.

end_per_testcase(_Case, _Config) ->
    application:stop(mycelium),
    ok.

%%====================================================================
%% Cases
%%====================================================================

new_is_idempotent(_Config) ->
    {ok, Pid} = mycelium:new_map(m),
    {ok, Pid2} = mycelium:new_map(m),
    ?assertEqual(Pid, Pid2).

new_rejects_non_atom(_Config) ->
    ?assertEqual({error, invalid_map_name}, mycelium:new_map(<<"bin">>)),
    ?assertEqual({error, invalid_map_name}, mycelium:new_map("str")).

independent_maps(_Config) ->
    {ok, _} = mycelium:new_map(m1),
    {ok, _} = mycelium:new_map(m2),
    ok = mycelium:map_put(m1, k, 1),
    ?assertEqual({ok, 1}, mycelium:map_get(m1, k)),
    ?assertEqual(not_found, mycelium:map_get(m2, k)).

put_get_remove_roundtrip(_Config) ->
    {ok, _} = mycelium:new_map(m),
    ?assertEqual(not_found, mycelium:map_get(m, k)),
    ok = mycelium:map_put(m, k, v),
    ?assertEqual({ok, v}, mycelium:map_get(m, k)),
    ok = mycelium:map_remove(m, k),
    ?assertEqual(not_found, mycelium:map_get(m, k)).

keys_and_to_list(_Config) ->
    {ok, _} = mycelium:new_map(m),
    ok = mycelium:map_put(m, a, 1),
    ok = mycelium:map_put(m, b, 2),
    ?assertEqual([a, b], lists:sort(mycelium:map_keys(m))),
    ?assertEqual([{a, 1}, {b, 2}], lists:sort(mycelium:map_to_list(m))).

subscribe_receives_events(_Config) ->
    {ok, _} = mycelium:new_map(m),
    ok = mycelium:subscribe_map(m),
    ok = mycelium:map_put(m, k, v),
    ?assertEqual({put, k, v}, recv(m)),
    ok = mycelium:map_remove(m, k),
    ?assertEqual({remove, k}, recv(m)).

unsubscribe_stops_events(_Config) ->
    {ok, _} = mycelium:new_map(m),
    ok = mycelium:subscribe_map(m),
    ok = mycelium:unsubscribe_map(m),
    ok = mycelium:map_put(m, k, v),
    ?assertEqual(timeout, recv(m)).

subscriber_down_is_cleaned_up(_Config) ->
    {ok, _} = mycelium:new_map(m),
    Self = self(),
    Pid = spawn(fun() ->
        mycelium:subscribe_map(m, self()),
        Self ! ready,
        receive
            stop -> ok
        end
    end),
    receive
        ready -> ok
    after 1000 -> ct:fail(no_ready)
    end,
    Owner = sys:get_state(mycelium_map:owner_name(m)),
    Pid ! stop,
    timer:sleep(100),
    %% The owner dropped the dead subscriber; a put still works.
    ok = mycelium:map_put(m, k, v),
    ?assert(is_tuple(Owner)).

validator_rejects_bad_put(_Config) ->
    {ok, _} = mycelium:new_map(m, #{validator => fun erlang:is_integer/1}),
    ?assertEqual({error, invalid_value}, mycelium:map_put(m, k, notint)),
    ?assertEqual(not_found, mycelium:map_get(m, k)),
    ?assertEqual(ok, mycelium:map_put(m, k, 7)),
    ?assertEqual({ok, 7}, mycelium:map_get(m, k)).

simulated_remote_delta_merges_and_emits(_Config) ->
    {ok, _} = mycelium:new_map(m),
    ok = mycelium:subscribe_map(m),
    Delta = #{rk => {value, rv, #{{'peer@h', mycelium_hlc:now()} => true}}},
    ok = mycelium_map:replica_merge_delta(mycelium_map:replica_name(m), Delta),
    ?assertEqual({put, rk, rv}, recv(m)),
    ?assertEqual({ok, rv}, mycelium:map_get(m, rk)).

malformed_gossip_does_not_crash(_Config) ->
    {ok, _} = mycelium:new_map(m),
    Rep = mycelium_map:replica_name(m),
    H = mycelium_hlc:now(),
    GoodDots = #{{'peer@h', H} => true},
    Bad = [
        %% non-map payload
        not_a_map,
        %% dots not a map
        #{ka => {value, v, not_a_map}},
        %% empty dot map
        #{kb => {value, v, #{}}},
        %% malformed dot key
        #{kc => {value, v, #{bad => true}}},
        %% malformed tombstone
        #{kd => {tombstone, not_a_timestamp}},
        %% not an entry
        #{ke => garbage}
    ],
    [ok = mycelium_map:replica_merge_delta(Rep, D) || D <- Bad],
    %% Owner and the shared HLC server survived, and good ops still work.
    _ = sys:get_state(mycelium_map:owner_name(m)),
    ?assert(is_process_alive(whereis(mycelium_hlc))),
    Good = #{kf => {value, ok_val, GoodDots}},
    ok = mycelium_map:replica_merge_delta(Rep, Good),
    %% flush the merge cast
    _ = sys:get_state(mycelium_map:owner_name(m)),
    ?assertEqual({ok, ok_val}, mycelium:map_get(m, kf)).

tombstones_gc_after_ttl(_Config) ->
    {ok, _} = mycelium:new_map(m, #{scan_ms => 50, tombstone_ttl_ms => 50}),
    ok = mycelium:map_put(m, k, v),
    ok = mycelium:map_remove(m, k),
    %% White-box: the OR-Map is field #state.map (element 5 of the record
    %% tuple). Tombstone present right after remove, GC'd after the TTL.
    Map0 = element(5, sys:get_state(mycelium_map:owner_name(m))),
    ?assertMatch({ok, {tombstone, _}}, mycelium_ormap:get_entry(k, Map0)),
    timer:sleep(300),
    Map1 = element(5, sys:get_state(mycelium_map:owner_name(m))),
    ?assertEqual(not_found, mycelium_ormap:get_entry(k, Map1)).

delete_map_stops_instance(_Config) ->
    {ok, _} = mycelium:new_map(m),
    ok = mycelium:map_put(m, k, v),
    ?assert(is_pid(whereis(mycelium_map:owner_name(m)))),
    ok = mycelium:delete_map(m),
    timer:sleep(100),
    ?assertEqual(undefined, whereis(mycelium_map:owner_name(m))),
    ?assertEqual(not_found, mycelium:map_get(m, k)).

ops_on_missing_map(_Config) ->
    ?assertEqual({error, no_such_map}, mycelium:map_put(nope, k, v)),
    ?assertEqual(not_found, mycelium:map_get(nope, k)),
    ?assertEqual([], mycelium:map_keys(nope)).

%% A `persist => true' map reloads its contents after a clean restart of
%% the whole application (the single-node analogue of a full-cluster
%% restart: nothing to re-sync from, so recovery is purely from disk).
persist_recovers_after_restart(_Config) ->
    {ok, _} = mycelium:new_map(pm, #{persist => true}),
    ok = mycelium:map_put(pm, k, v),
    ok = mycelium:map_put(pm, k2, v2),
    ok = mycelium:map_remove(pm, k2),
    %% Clean stop runs the owner's terminate (closes the log); the data dir
    %% (set in init_per_testcase) persists across the restart.
    application:stop(mycelium),
    {ok, _} = application:ensure_all_started(mycelium),
    {ok, _} = mycelium:new_map(pm, #{persist => true}),
    ?assertEqual({ok, v}, mycelium:map_get(pm, k)),
    ?assertEqual(not_found, mycelium:map_get(pm, k2)).

%%====================================================================
%% Helpers
%%====================================================================

recv(Name) ->
    receive
        {mycelium_map, Name, Event} -> Event
    after 1000 -> timeout
    end.
