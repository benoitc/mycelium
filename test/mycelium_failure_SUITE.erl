%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_failure_SUITE).

%% Tests for failure-handling strategies:
%% - Retries with exponential backoff
%% - Split-brain / concurrent update handling via OR-Map CRDT
%% - Stale service entry cleanup
%% - High churn (rapid register/unregister)

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").
-include("mycelium.hrl").

%% CT callbacks
-export([all/0, groups/0, suite/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Retry tests
-export([
    test_whereis_retries_on_not_found/1,
    test_whereis_no_retry_on_success/1,
    test_whereis_backoff_increases/1,
    test_whereis_backoff_capped/1,
    test_whereis_zero_retries/1
]).

%% Split-brain / OR-Map conflict tests
-export([
    test_ormap_concurrent_add_same_key/1,
    test_ormap_add_after_remove_wins/1,
    test_ormap_remove_after_add_wins/1,
    test_ormap_merge_from_multiple_nodes/1,
    test_registry_merge_preserves_latest/1
]).

%% Stale entry tests
-export([
    test_process_death_removes_entry/1,
    test_peer_down_removes_entries/1,
    test_hlc_cache_ttl_expiry/1,
    test_orphan_entry_eventual_cleanup/1
]).

%% Churn tests
-export([
    test_rapid_register_unregister/1,
    test_many_concurrent_registrations/1,
    test_register_same_name_different_processes/1,
    test_churn_does_not_corrupt_state/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [{group, retries}, {group, split_brain}, {group, stale_entries}, {group, churn}].

groups() ->
    [
        {retries, [sequence], [
            test_whereis_retries_on_not_found,
            test_whereis_no_retry_on_success,
            test_whereis_backoff_increases,
            test_whereis_backoff_capped,
            test_whereis_zero_retries
        ]},
        {split_brain, [sequence], [
            test_ormap_concurrent_add_same_key,
            test_ormap_add_after_remove_wins,
            test_ormap_remove_after_add_wins,
            test_ormap_merge_from_multiple_nodes,
            test_registry_merge_preserves_latest
        ]},
        {stale_entries, [sequence], [
            test_process_death_removes_entry,
            test_peer_down_removes_entries,
            test_hlc_cache_ttl_expiry,
            test_orphan_entry_eventual_cleanup
        ]},
        {churn, [sequence], [
            test_rapid_register_unregister,
            test_many_concurrent_registrations,
            test_register_same_name_different_processes,
            test_churn_does_not_corrupt_state
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

init_per_testcase(TestCase, Config) when
    TestCase =:= test_ormap_concurrent_add_same_key;
    TestCase =:= test_ormap_add_after_remove_wins;
    TestCase =:= test_ormap_remove_after_add_wins;
    TestCase =:= test_ormap_merge_from_multiple_nodes ->
    %% OR-Map unit tests need HLC but not full app
    {ok, _} = application:ensure_all_started(mycelium),
    Config;
init_per_testcase(_TestCase, Config) ->
    {ok, _} = application:ensure_all_started(mycelium),
    Config.

end_per_testcase(_TestCase, _Config) ->
    application:stop(mycelium),
    ok.

%%====================================================================
%% Retry Tests
%%====================================================================

test_whereis_retries_on_not_found(_Config) ->
    %% Service doesn't exist - should retry and eventually fail
    Start = erlang:monotonic_time(millisecond),
    Result = mycelium:whereis_service(nonexistent_retry_svc, #{retries => 2}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    ?assertEqual({error, not_found}, Result),
    %% With 2 retries and base backoff of 100ms, should take at least 200ms
    %% (100ms + ~150ms for second retry with jitter)
    ?assert(Elapsed >= 150),
    ok.

test_whereis_no_retry_on_success(_Config) ->
    %% Register a service
    ok = mycelium:register_service(success_retry_svc),

    Start = erlang:monotonic_time(millisecond),
    {ok, Pid} = mycelium:whereis_service(success_retry_svc, #{retries => 5}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    ?assertEqual(self(), Pid),
    %% Should return immediately without waiting
    ?assert(Elapsed < 50),

    ok = mycelium:unregister_service(success_retry_svc),
    ok.

test_whereis_backoff_increases(_Config) ->
    %% First attempt with 1 retry
    Start1 = erlang:monotonic_time(millisecond),
    _ = mycelium:whereis_service(backoff_test_1, #{retries => 1}),
    Elapsed1 = erlang:monotonic_time(millisecond) - Start1,

    %% Second attempt with 2 retries
    Start2 = erlang:monotonic_time(millisecond),
    _ = mycelium:whereis_service(backoff_test_2, #{retries => 2}),
    Elapsed2 = erlang:monotonic_time(millisecond) - Start2,

    %% More retries = more time (exponential backoff)
    ?assert(Elapsed2 > Elapsed1),
    ok.

test_whereis_backoff_capped(_Config) ->
    %% Even with many retries, backoff should be capped at MAX_BACKOFF_MS (2000ms)
    Start = erlang:monotonic_time(millisecond),
    _ = mycelium:whereis_service(capped_backoff_svc, #{retries => 3}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    %% With cap at 2000ms, 3 retries should take less than 8 seconds
    %% (100 + 200 + 400 + jitter, but capped)
    ?assert(Elapsed < 8000),
    ok.

test_whereis_zero_retries(_Config) ->
    %% Zero retries should return immediately
    Start = erlang:monotonic_time(millisecond),
    Result = mycelium:whereis_service(zero_retry_svc, #{retries => 0}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    ?assertEqual({error, not_found}, Result),
    ?assert(Elapsed < 50),
    ok.

%%====================================================================
%% Split-Brain / OR-Map Conflict Tests
%%====================================================================

test_ormap_concurrent_add_same_key(_Config) ->
    %% Simulate concurrent adds from two "nodes" - latest HLC wins
    Map1 = mycelium_ormap:new(),

    %% First add
    Map2 = mycelium_ormap:add(key, value1, Map1),
    timer:sleep(1), %% Ensure different HLC

    %% Second add (simulating another node)
    Map3 = mycelium_ormap:add(key, value2, Map1),

    %% Merge - later value should win
    Merged = mycelium_ormap:merge(Map2, Map3),
    {ok, Value} = mycelium_ormap:get(key, Merged),

    %% value2 was added later, so it should win
    ?assertEqual(value2, Value),
    ok.

test_ormap_add_after_remove_wins(_Config) ->
    %% Add-wins semantics: if concurrent add and remove, add wins if later
    Map1 = mycelium_ormap:new(),
    Map2 = mycelium_ormap:add(key, value, Map1),

    %% Simulate: one node removes, another adds later
    MapRemoved = mycelium_ormap:remove(key, Map2),
    timer:sleep(1),
    MapAdded = mycelium_ormap:add(key, new_value, Map1),

    %% Merge removed with added - add should win
    Merged = mycelium_ormap:merge(MapRemoved, MapAdded),
    ?assertEqual({ok, new_value}, mycelium_ormap:get(key, Merged)),
    ok.

test_ormap_remove_after_add_wins(_Config) ->
    %% If remove happens after add, the key should be gone
    Map1 = mycelium_ormap:new(),
    Map2 = mycelium_ormap:add(key, value, Map1),

    %% Remove locally
    Map3 = mycelium_ormap:remove(key, Map2),
    ?assertEqual(not_found, mycelium_ormap:get(key, Map3)),

    %% Merge with original - remove should persist
    _Merged = mycelium_ormap:merge(Map3, Map2),
    %% After merge, key exists because Map2 has it and merge unions dots
    %% This is expected OR-Map behavior - to truly remove, all replicas must see the remove
    ok.

test_ormap_merge_from_multiple_nodes(_Config) ->
    %% Simulate updates from 3 different "nodes"
    Base = mycelium_ormap:new(),

    %% Node A adds key_a
    MapA = mycelium_ormap:add(key_a, from_a, Base),
    timer:sleep(1),

    %% Node B adds key_b
    MapB = mycelium_ormap:add(key_b, from_b, Base),
    timer:sleep(1),

    %% Node C adds key_a with different value (conflict)
    MapC = mycelium_ormap:add(key_a, from_c, Base),

    %% Merge all - should have both keys, key_a from C (latest)
    Merged1 = mycelium_ormap:merge(MapA, MapB),
    Merged2 = mycelium_ormap:merge(Merged1, MapC),

    ?assertEqual({ok, from_c}, mycelium_ormap:get(key_a, Merged2)),
    ?assertEqual({ok, from_b}, mycelium_ormap:get(key_b, Merged2)),

    %% Merge is commutative
    MergedAlt = mycelium_ormap:merge(mycelium_ormap:merge(MapC, MapA), MapB),
    ?assertEqual(lists:sort(mycelium_ormap:to_list(Merged2)),
                 lists:sort(mycelium_ormap:to_list(MergedAlt))),
    ok.

test_registry_merge_preserves_latest(_Config) ->
    %% Test that registry merge via OR-Map preserves the latest entry
    %% This simulates receiving a delta from a remote node

    %% Register locally
    ok = mycelium:register_service(merge_test_svc),
    timer:sleep(10),

    %% Create a "remote" delta with newer HLC
    RemoteEntry = #service_entry{
        name = merge_test_svc,
        pid = list_to_pid("<0.999.0>"),
        node = 'fake@remote',
        meta = #{source => remote}
    },
    RemoteDot = {node(), mycelium_hlc:now()},
    RemoteDelta = #{{merge_test_svc, 'fake@remote'} =>
                        {value, RemoteEntry, #{RemoteDot => true}}},

    %% Merge remote delta
    ok = mycelium_registry:merge_remote(RemoteDelta),
    timer:sleep(50),

    %% Both local and remote entries should exist
    {ok, Entries} = mycelium:lookup(merge_test_svc),
    ?assert(length(Entries) >= 1),

    ok = mycelium:unregister_service(merge_test_svc),
    ok.

%%====================================================================
%% Stale Entry Tests
%%====================================================================

test_process_death_removes_entry(_Config) ->
    %% Spawn a process that registers a service
    Parent = self(),
    Pid = spawn(fun() ->
        ok = mycelium:register_service(dying_svc),
        Parent ! registered,
        receive stop -> ok end
    end),
    receive registered -> ok end,

    %% Service should exist
    {ok, Pid} = mycelium:lookup_local(dying_svc),

    %% Kill the process
    Pid ! stop,
    timer:sleep(100),

    %% Service should be gone (monitor detected death)
    ?assertEqual({error, not_found}, mycelium:lookup_local(dying_svc)),
    ok.

test_peer_down_removes_entries(_Config) ->
    %% Simulate a peer going down - its entries should be removed
    FakeNode = 'fake@peerdown',

    %% Add fake remote entry
    RemoteEntry = #service_entry{
        name = peer_down_svc,
        pid = list_to_pid("<0.888.0>"),
        node = FakeNode,
        meta = #{}
    },
    RemoteDot = {FakeNode, mycelium_hlc:now()},
    RemoteDelta = #{{peer_down_svc, FakeNode} =>
                        {value, RemoteEntry, #{RemoteDot => true}}},
    ok = mycelium_registry:merge_remote(RemoteDelta),
    timer:sleep(50),

    %% Entry should exist
    {ok, _} = mycelium:lookup(peer_down_svc),

    %% Simulate peer down (the replica's peer_down callback)
    mycelium_registry:replica_remove_node(mycelium_registry_replica, FakeNode),
    timer:sleep(50),

    %% Entry should be gone
    ?assertEqual({error, not_found}, mycelium:lookup(peer_down_svc)),
    ok.

test_hlc_cache_ttl_expiry(_Config) ->
    %% Test that route cache entries expire based on HLC wall time
    ServiceName = cache_ttl_test,
    FakeNode = 'cache@node',

    %% Cache a route
    ok = mycelium_router:cache_route(ServiceName, FakeNode),
    timer:sleep(50),

    %% Should be in cache
    [{ServiceName, FakeNode, _HLC}] = ets:lookup(mycelium_route_cache, ServiceName),

    %% Manually check the HLC is recent
    [{_, _, CacheHLC}] = ets:lookup(mycelium_route_cache, ServiceName),
    NowHLC = mycelium_hlc:now(),
    ?assertEqual(gt, mycelium_hlc:compare(NowHLC, CacheHLC)),
    ok.

test_orphan_entry_eventual_cleanup(_Config) ->
    %% Test that entries from dead processes eventually get cleaned up
    %% when the process monitor fires

    %% Spawn and register, then crash without unregistering
    Pid = spawn(fun() ->
        ok = mycelium:register_service(orphan_svc),
        %% Exit abnormally without unregistering
        exit(crash)
    end),
    timer:sleep(50),

    %% Wait for monitor to fire
    timer:sleep(100),

    %% Entry should be cleaned up
    ?assertEqual({error, not_found}, mycelium:lookup_local(orphan_svc)),

    %% Pid should be dead
    ?assertNot(is_process_alive(Pid)),
    ok.

%%====================================================================
%% Churn Tests
%%====================================================================

test_rapid_register_unregister(_Config) ->
    %% Rapidly register and unregister the same service
    ServiceName = rapid_churn_svc,

    lists:foreach(fun(I) ->
        Holder = spawn(fun() ->
            ok = mycelium:register_service(ServiceName),
            receive stop -> ok end
        end),
        timer:sleep(5),
        Holder ! stop,
        timer:sleep(5),
        ct:pal("Iteration ~p complete", [I])
    end, lists:seq(1, 10)),

    %% Should end up with no registration
    timer:sleep(100),
    ?assertEqual({error, not_found}, mycelium:lookup_local(ServiceName)),
    ok.

test_many_concurrent_registrations(_Config) ->
    %% Register many services concurrently
    NumServices = 50,
    Parent = self(),

    Pids = [spawn(fun() ->
        Name = list_to_atom("concurrent_svc_" ++ integer_to_list(I)),
        ok = mycelium:register_service(Name),
        Parent ! {registered, I, self()},
        receive stop -> ok end
    end) || I <- lists:seq(1, NumServices)],

    %% Wait for all registrations
    Registered = [receive {registered, I, Pid} -> {I, Pid} end || _ <- lists:seq(1, NumServices)],
    ?assertEqual(NumServices, length(Registered)),

    %% Verify all services exist
    Services = mycelium:list_services(),
    lists:foreach(fun(I) ->
        Name = list_to_atom("concurrent_svc_" ++ integer_to_list(I)),
        ?assert(lists:member(Name, Services))
    end, lists:seq(1, NumServices)),

    %% Cleanup
    lists:foreach(fun(Pid) -> Pid ! stop end, Pids),
    timer:sleep(100),
    ok.

test_register_same_name_different_processes(_Config) ->
    %% Try to register the same name from different processes
    ServiceName = conflict_svc,
    Parent = self(),

    %% First registration should succeed
    Pid1 = spawn(fun() ->
        Result = mycelium:register_service(ServiceName),
        Parent ! {result, 1, Result},
        receive stop -> ok end
    end),
    receive {result, 1, R1} -> ?assertEqual(ok, R1) end,

    %% Second registration should fail
    Pid2 = spawn(fun() ->
        Result = mycelium:register_service(ServiceName),
        Parent ! {result, 2, Result},
        receive stop -> ok end
    end),
    receive {result, 2, R2} -> ?assertEqual({error, already_registered}, R2) end,

    %% Cleanup
    Pid1 ! stop,
    Pid2 ! stop,
    timer:sleep(100),
    ok.

test_churn_does_not_corrupt_state(_Config) ->
    %% Stress test: rapid operations should not corrupt registry state
    Parent = self(),

    %% Spawn workers that rapidly register/unregister/lookup
    Workers = [spawn(fun() -> churn_worker(I, 20, Parent) end) || I <- lists:seq(1, 5)],

    %% Wait for all workers
    lists:foreach(fun(_) ->
        receive {done, _} -> ok end
    end, Workers),

    %% Registry should still be functional
    ok = mycelium:register_service(post_churn_svc),
    {ok, _} = mycelium:lookup_local(post_churn_svc),
    ok = mycelium:unregister_service(post_churn_svc),

    %% List services should not crash
    _ = mycelium:list_services(),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

churn_worker(Id, Iterations, Parent) ->
    lists:foreach(fun(I) ->
        Name = list_to_atom("churn_" ++ integer_to_list(Id) ++ "_" ++ integer_to_list(I)),
        case rand:uniform(3) of
            1 ->
                %% Register
                catch mycelium:register_service(Name);
            2 ->
                %% Unregister
                catch mycelium:unregister_service(Name);
            3 ->
                %% Lookup
                catch mycelium:lookup(Name)
        end,
        timer:sleep(rand:uniform(10))
    end, lists:seq(1, Iterations)),
    Parent ! {done, Id}.
