%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_router_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").
-include("mycelium.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    test_find_route_direct/1,
    test_find_route_via/1,
    test_find_route_self/1,
    test_cache_route/1,
    test_invalidate_route/1,
    test_invalidate_all/1,
    test_route_cache_expiry/1,
    test_route_request_drops_over_cap/1,
    test_route_request_releases_slot_on_completion/1,
    test_sweep_evicts_expired_entries/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, routing}].

groups() ->
    [
        {routing, [sequence], [
            test_find_route_self,
            test_find_route_direct,
            test_find_route_via,
            test_cache_route,
            test_invalidate_route,
            test_invalidate_all,
            test_route_cache_expiry,
            test_route_request_drops_over_cap,
            test_route_request_releases_slot_on_completion,
            test_sweep_evicts_expired_entries
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

test_find_route_self(_Config) ->
    %% Route to self should be direct
    ?assertEqual({direct, node()}, mycelium_router:find_route(node())),
    ok.

test_find_route_direct(_Config) ->
    %% Simulate having a node in active view by mocking
    %% In real test, we'd need distributed setup
    %% For now, test that no route returns when active view is empty
    ?assertEqual(no_route, mycelium_router:find_route('fake@node')),
    ok.

test_find_route_via(_Config) ->
    %% Test that when we have no direct connection, we try to find a via route
    %% With an empty active view, we should get no_route
    ?assertEqual(no_route, mycelium_router:find_route('remote@node')),
    ok.

test_cache_route(_Config) ->
    %% Cache a route and verify we can retrieve it
    ServiceName = test_service,
    ViaNode = 'via@node',

    %% Cache the route
    ok = mycelium_router:cache_route(ServiceName, ViaNode),
    %% Allow async cast to complete
    timer:sleep(50),

    %% Verify cache entry exists in ETS
    [{ServiceName, ViaNode, _Time}] = ets:lookup(mycelium_route_cache, ServiceName),
    ok.

test_invalidate_route(_Config) ->
    %% Cache a route
    ServiceName = invalidate_test,
    ok = mycelium_router:cache_route(ServiceName, 'some@node'),
    timer:sleep(50),

    %% Verify it exists
    ?assertMatch([{_, _, _}], ets:lookup(mycelium_route_cache, ServiceName)),

    %% Invalidate it
    ok = mycelium_router:invalidate_route(ServiceName),
    timer:sleep(50),

    %% Should be gone
    ?assertEqual([], ets:lookup(mycelium_route_cache, ServiceName)),
    ok.

test_invalidate_all(_Config) ->
    %% Cache multiple routes
    ok = mycelium_router:cache_route(svc_a, 'node_a@host'),
    ok = mycelium_router:cache_route(svc_b, 'node_b@host'),
    ok = mycelium_router:cache_route(svc_c, 'node_c@host'),
    timer:sleep(50),

    %% Should have entries
    ?assert(ets:info(mycelium_route_cache, size) >= 3),

    %% Invalidate all
    ok = mycelium_router:invalidate_all(),
    timer:sleep(50),

    %% Should be empty
    ?assertEqual(0, ets:info(mycelium_route_cache, size)),
    ok.

test_route_cache_expiry(_Config) ->
    %% Test that cache entries have HLC timestamps
    ServiceName = expiry_test,
    BeforeHLC = mycelium_hlc:now(),
    ok = mycelium_router:cache_route(ServiceName, 'some@node'),
    timer:sleep(50),

    %% Verify entry has HLC timestamp
    [{ServiceName, _Node, HLC}] = ets:lookup(mycelium_route_cache, ServiceName),
    ?assertMatch(#timestamp{}, HLC),
    %% HLC should be >= the time before we cached
    ?assertNotEqual(lt, mycelium_hlc:compare(HLC, BeforeHLC)),
    ok.

%% When in_flight has saturated max_in_flight, the next route_request
%% is refused with `{error, overloaded}' instead of spawning another
%% helper. Uses sys:replace_state/2 to saturate the counter without
%% needing a busy real handler.
test_route_request_drops_over_cap(_Config) ->
    sys:replace_state(mycelium_router, fun({state, _N, _Max}) ->
        {state, 1, 1}
    end),
    Self = self(),
    Ref = make_ref(),
    mycelium_router !
        {'$mycelium_route', {route_request, undefined, {Self, Ref}}},
    receive
        {Ref, {error, overloaded}} -> ok
    after 1000 ->
        ct:fail("Expected overloaded reply when at cap")
    end,
    %% Reset to defaults so the next case starts clean.
    sys:replace_state(mycelium_router, fun({state, _, Max}) ->
        {state, 0, Max}
    end),
    ok.

%% A completed handler must decrement in_flight so the next request
%% is accepted. Issue a handful of requests; with max=2 we should see
%% all of them reply (since each finishes quickly).
test_route_request_releases_slot_on_completion(_Config) ->
    sys:replace_state(mycelium_router, fun({state, _, _}) ->
        {state, 0, 2}
    end),
    Self = self(),
    Refs = [make_ref() || _ <- lists:seq(1, 5)],
    lists:foreach(
        fun(Ref) ->
            Req = #route_req{
                service_name = nonexistent_svc,
                ttl = 1,
                origin = node(),
                visited = [node()]
            },
            mycelium_router !
                {'$mycelium_route', {route_request, Req, {Self, Ref}}}
        end,
        Refs
    ),
    %% Drain all replies. Some can be `{error, not_found}' (no peers),
    %% some can be `{error, overloaded}'; the test only asserts that
    %% none hang.
    lists:foreach(
        fun(Ref) ->
            receive
                {Ref, _} -> ok
            after 2000 ->
                ct:fail({no_reply, Ref})
            end
        end,
        Refs
    ),
    %% After all handlers exit, in_flight should be back at 0.
    timer:sleep(100),
    {state, NFinal, _} = sys:get_state(mycelium_router),
    ?assertEqual(0, NFinal),
    ok.

%% Periodic sweep evicts cache entries whose HLC wall-time is older
%% than the cache TTL, even if no caller ever re-reads them.
test_sweep_evicts_expired_entries(_Config) ->
    Fresh = fresh_route_test,
    Stale = stale_route_test,
    ok = mycelium_router:cache_route(Fresh, 'fresh@host'),
    %% Manually insert a stale entry. Build an HLC timestamp anchored
    %% one full TTL window in the past.
    NowWall = mycelium_hlc:wall_time(mycelium_hlc:now()),
    StaleHLC = #timestamp{
        wall_time = NowWall - 2 * 1800000,
        logical = 0
    },
    true = ets:insert(
        mycelium_route_cache,
        {Stale, 'stale@host', StaleHLC}
    ),
    %% Trigger the sweep synchronously by sending the handler message
    %% then sync via get_state.
    mycelium_router ! sweep_cache,
    _ = sys:get_state(mycelium_router),
    ?assertEqual([], ets:lookup(mycelium_route_cache, Stale)),
    ?assertMatch(
        [{Fresh, _, _}],
        ets:lookup(mycelium_route_cache, Fresh)
    ),
    ok.
