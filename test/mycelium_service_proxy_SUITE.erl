%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Tests for the per-proxy fan-out cap on overlay-routed casts.

-module(mycelium_service_proxy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    test_forward_cast_drops_over_cap/1,
    test_forward_cast_releases_slot/1,
    test_relay_refuses_ttl_zero/1,
    test_relay_refuses_visited_node/1
]).

all() ->
    [{group, fan_out}, {group, relay}].

groups() ->
    [
        {fan_out, [sequence], [
            test_forward_cast_drops_over_cap,
            test_forward_cast_releases_slot
        ]},
        {relay, [sequence], [
            test_relay_refuses_ttl_zero,
            test_relay_refuses_visited_node
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
    %% Route everything through a fake next hop so forward_cast/4
    %% takes the spawn path.
    ok = meck:new(mycelium_router, [passthrough]),
    ok = meck:expect(
        mycelium_router,
        find_route,
        fun(_) -> {via, 'fake_next@host'} end
    ),
    Config.

end_per_testcase(_TestCase, _Config) ->
    catch meck:unload(mycelium_router),
    application:stop(mycelium),
    ok.

%% With the proxy's in_flight counter saturated, additional overlay
%% casts must not spawn helpers. We check by reading the gen_server
%% state and asserting `in_flight' did not grow.
test_forward_cast_drops_over_cap(_Config) ->
    Name = proxy_drop_svc,
    Target = 'fake_target@host',
    {ok, Proxy} = mycelium_proxy_sup:start_proxy(Name, Target),
    %% Saturate: in_flight = max = 1. Address the #state{} fields by
    %% position (in_flight = 4, max_in_flight = 5) so trailing fields
    %% are preserved.
    sys:replace_state(Proxy, fun(S) -> setelement(4, setelement(5, S, 1), 1) end),
    gen_server:cast(Proxy, some_request),
    %% Let the handler return.
    _ = sys:get_state(Proxy),
    InFlight = element(4, sys:get_state(Proxy)),
    ?assertEqual(1, InFlight),
    mycelium_proxy_sup:stop_proxy(Name),
    ok.

%% Under-cap casts must spawn helpers and decrement the counter when
%% the helper exits. With max=2, several quick casts should drain
%% back to in_flight = 0.
test_forward_cast_releases_slot(_Config) ->
    Name = proxy_release_svc,
    Target = 'fake_target@host',
    {ok, Proxy} = mycelium_proxy_sup:start_proxy(Name, Target),
    sys:replace_state(Proxy, fun(S) -> setelement(4, setelement(5, S, 2), 0) end),
    %% rpc:call to a non-existent node returns {badrpc, nodedown}
    %% almost immediately; the spawned helper exits and triggers DOWN.
    [gen_server:cast(Proxy, {ping, I}) || I <- lists:seq(1, 5)],
    %% Sync and wait for handlers to drain.
    _ = sys:get_state(Proxy),
    timer:sleep(200),
    InFlight = element(4, sys:get_state(Proxy)),
    ?assertEqual(0, InFlight),
    mycelium_proxy_sup:stop_proxy(Name),
    ok.

%% relay/4 with TTL=0 refuses the hop. Closes the rpc-loop hazard
%% where mismatched route caches could relay forever.
test_relay_refuses_ttl_zero(_Config) ->
    Ctx = #{ttl => 0, visited => [node()]},
    ?assertEqual(
        {error, ttl_expired},
        mycelium_service_proxy:relay(
            some_svc, 'fake_target@host', request, Ctx
        )
    ),
    ok.

%% relay/4 refuses to forward to a node already in the visited list.
test_relay_refuses_visited_node(_Config) ->
    %% Re-stub find_route to return a NextHop that's in visited.
    Stuck = 'already_visited@host',
    meck:expect(
        mycelium_router,
        find_route,
        fun(_) -> {via, Stuck} end
    ),
    Ctx = #{ttl => 5, visited => [node(), Stuck]},
    ?assertEqual(
        {error, relay_loop},
        mycelium_service_proxy:relay(
            some_svc, 'fake_target@host', request, Ctx
        )
    ),
    ok.
