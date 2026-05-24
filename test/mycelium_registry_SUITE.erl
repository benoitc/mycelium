%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_registry_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("mycelium.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    test_register_service/1,
    test_register_duplicate/1,
    test_unregister_service/1,
    test_lookup_local/1,
    test_lookup_not_found/1,
    test_list_services/1,
    test_service_monitor/1,
    test_whereis_service_local/1,
    test_whereis_service_retry_not_found/1,
    test_whereis_service_no_retry_on_success/1,
    test_whereis_service_custom_retries/1,
    test_overlay_lookup_returns_ok_node_pid/1,
    test_whereis_service_via_overlay/1,
    %% Via callbacks
    test_register_name/1,
    test_register_name_duplicate/1,
    test_unregister_name/1,
    test_whereis_name/1,
    test_send/1,
    test_send_not_found/1,
    test_via_gen_server/1,
    %% Service proxy event wiring
    test_service_proxy_reaps_on_remote_service_down/1,
    test_service_proxy_ignores_unrelated_service_down/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, registry}].

groups() ->
    [
        {registry, [sequence], [
            test_register_service,
            test_register_duplicate,
            test_unregister_service,
            test_lookup_local,
            test_lookup_not_found,
            test_list_services,
            test_service_monitor,
            test_whereis_service_local,
            test_whereis_service_retry_not_found,
            test_whereis_service_no_retry_on_success,
            test_whereis_service_custom_retries,
            test_overlay_lookup_returns_ok_node_pid,
            test_whereis_service_via_overlay,
            %% Via callbacks
            test_register_name,
            test_register_name_duplicate,
            test_unregister_name,
            test_whereis_name,
            test_send,
            test_send_not_found,
            test_via_gen_server,
            test_service_proxy_reaps_on_remote_service_down,
            test_service_proxy_ignores_unrelated_service_down
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

test_register_service(_Config) ->
    ?assertEqual(ok, mycelium:register_service(test_svc, #{type => worker})),
    ok.

test_register_duplicate(_Config) ->
    ok = mycelium:register_service(dup_svc),
    ?assertEqual({error, already_registered}, mycelium:register_service(dup_svc)),
    ok.

test_unregister_service(_Config) ->
    ok = mycelium:register_service(unreg_svc),
    ok = mycelium:unregister_service(unreg_svc),
    ?assertEqual({error, not_found}, mycelium:lookup_local(unreg_svc)),
    ok.

test_lookup_local(_Config) ->
    ok = mycelium:register_service(local_svc),
    {ok, Pid} = mycelium:lookup_local(local_svc),
    ?assertEqual(self(), Pid),
    ok.

test_lookup_not_found(_Config) ->
    ?assertEqual({error, not_found}, mycelium:lookup(nonexistent)),
    ?assertEqual({error, not_found}, mycelium:lookup_local(nonexistent)),
    ok.

test_list_services(_Config) ->
    ok = mycelium:register_service(svc_a),
    ok = mycelium:register_service(svc_b),
    Services = mycelium:list_services(),
    ?assert(lists:member(svc_a, Services)),
    ?assert(lists:member(svc_b, Services)),
    ok.

test_service_monitor(_Config) ->
    %% Spawn a process that registers a service then dies
    Parent = self(),
    Pid = spawn(fun() ->
        ok = mycelium:register_service(monitored_svc),
        Parent ! registered,
        receive
            stop -> ok
        end
    end),
    receive
        registered -> ok
    end,

    %% Service should exist
    {ok, Pid} = mycelium:lookup_local(monitored_svc),

    %% Kill the process
    Pid ! stop,
    timer:sleep(100),

    %% Service should be gone
    ?assertEqual({error, not_found}, mycelium:lookup_local(monitored_svc)),
    ok.

test_whereis_service_local(_Config) ->
    %% Test whereis_service finds local services
    ok = mycelium:register_service(whereis_local_svc),
    {ok, Pid} = mycelium:whereis_service(whereis_local_svc),
    ?assertEqual(self(), Pid),
    ok = mycelium:unregister_service(whereis_local_svc),
    ok.

test_whereis_service_retry_not_found(_Config) ->
    %% Test that whereis_service retries on not_found
    %% With 0 retries, should return immediately
    Start = erlang:monotonic_time(millisecond),
    ?assertEqual({error, not_found}, mycelium:whereis_service(nonexistent_svc, #{retries => 0})),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    %% Should complete quickly with 0 retries
    ?assert(Elapsed < 50),
    ok.

test_whereis_service_no_retry_on_success(_Config) ->
    %% Test that whereis_service doesn't retry when found
    ok = mycelium:register_service(success_svc),

    Start = erlang:monotonic_time(millisecond),
    {ok, _Pid} = mycelium:whereis_service(success_svc),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    %% Should return immediately without any retry delay
    ?assert(Elapsed < 50),

    ok = mycelium:unregister_service(success_svc),
    ok.

test_whereis_service_custom_retries(_Config) ->
    %% Test that retry count can be customized
    %% With 1 retry and ~100ms base backoff, should take at least 100ms
    Start = erlang:monotonic_time(millisecond),
    ?assertEqual({error, not_found}, mycelium:whereis_service(custom_retry_svc, #{retries => 1})),
    Elapsed = erlang:monotonic_time(millisecond) - Start,

    %% Should have waited for at least one backoff cycle (100ms base + jitter)
    ?assert(Elapsed >= 100),
    %% But not too long
    ?assert(Elapsed < 500),
    ok.

%% The router's find_service/1 returns `{found, Node, Pid}'.
%% overlay_lookup/1 specs `{ok, node(), pid()}'. Lock the normalisation.
test_overlay_lookup_returns_ok_node_pid(_Config) ->
    Fake = self(),
    Node = 'fake@host',
    ok = meck:new(mycelium_router, [passthrough]),
    ok = meck:expect(
        mycelium_router,
        find_service,
        fun(_) -> {found, Node, Fake} end
    ),
    try
        ?assertEqual(
            {ok, Node, Fake},
            mycelium_registry:overlay_lookup(some_svc)
        )
    after
        meck:unload(mycelium_router)
    end,
    ok.

%% A service reachable only through overlay must not crash the
%% retry loop of whereis_service/1,2. Before the fix, the retry
%% case clause matched only {ok,_}, {ok,_,_} and {error,not_found};
%% a `{found,_,_}' from the router fell through and raised
%% case_clause.
test_whereis_service_via_overlay(_Config) ->
    Fake = self(),
    Node = 'fake@host',
    ok = meck:new(mycelium_router, [passthrough]),
    ok = meck:expect(
        mycelium_router,
        find_service,
        fun(_) -> {found, Node, Fake} end
    ),
    try
        ?assertEqual(
            {ok, Node, Fake},
            mycelium:whereis_service(overlay_only_svc)
        )
    after
        meck:unload(mycelium_router)
    end,
    ok.

%%====================================================================
%% Via Callback Tests
%%====================================================================

test_register_name(_Config) ->
    %% Test register_name/2 returns 'yes' on success
    Pid = self(),
    ?assertEqual(yes, mycelium:register_name(via_test_name, Pid)),
    %% Cleanup
    mycelium:unregister_name(via_test_name),
    ok.

test_register_name_duplicate(_Config) ->
    %% Test register_name/2 returns 'no' if already registered
    Pid = self(),
    ?assertEqual(yes, mycelium:register_name(via_dup_name, Pid)),
    ?assertEqual(no, mycelium:register_name(via_dup_name, Pid)),
    %% Cleanup
    mycelium:unregister_name(via_dup_name),
    ok.

test_unregister_name(_Config) ->
    %% Test unregister_name/1 removes registration
    Pid = self(),
    yes = mycelium:register_name(via_unreg_name, Pid),
    ?assertEqual(ok, mycelium:unregister_name(via_unreg_name)),
    %% Should be gone now
    ?assertEqual(undefined, mycelium:whereis_name(via_unreg_name)),
    ok.

test_whereis_name(_Config) ->
    %% Test whereis_name/1 returns pid or undefined
    Pid = self(),
    %% Not registered yet
    ?assertEqual(undefined, mycelium:whereis_name(via_whereis_name)),
    %% Register and verify
    yes = mycelium:register_name(via_whereis_name, Pid),
    ?assertEqual(Pid, mycelium:whereis_name(via_whereis_name)),
    %% Cleanup
    mycelium:unregister_name(via_whereis_name),
    ok.

test_send(_Config) ->
    %% Test send/2 delivers message and returns pid
    Parent = self(),
    Pid = spawn(fun() ->
        yes = mycelium:register_name(via_send_name, self()),
        Parent ! registered,
        receive
            test_msg -> Parent ! {received, self()}
        end
    end),
    receive
        registered -> ok
    end,

    %% Send message and verify return value
    ?assertEqual(Pid, mycelium:send(via_send_name, test_msg)),

    %% Verify message was received
    receive
        {received, Pid} -> ok
    after 1000 ->
        ct:fail(message_not_received)
    end,
    ok.

test_send_not_found(_Config) ->
    %% Test send/2 raises badarg if name not found
    ?assertError(
        {badarg, {via_nonexistent_name, test_msg}},
        mycelium:send(via_nonexistent_name, test_msg)
    ),
    ok.

test_via_gen_server(_Config) ->
    %% Test {via, mycelium, Name} works with gen_server
    %% Start a gen_server using via tuple
    Name = via_gen_server_test,
    {ok, Pid} = gen_server:start({via, mycelium, Name}, mycelium_test_server, [], []),

    %% Verify we can call it by name
    ?assertEqual(pong, gen_server:call({via, mycelium, Name}, ping)),

    %% Verify whereis_name returns the right pid
    ?assertEqual(Pid, mycelium:whereis_name(Name)),

    %% Stop the server
    gen_server:stop({via, mycelium, Name}),

    %% Verify it's unregistered after stopping
    timer:sleep(50),
    ?assertEqual(undefined, mycelium:whereis_name(Name)),
    ok.

%% A service_down event for the proxy's own service must terminate
%% the proxy. The producer at mycelium_registry emits the 4-tuple
%% form on the service event bus; the proxy used to subscribe to the
%% hyparview bus and match a 3-tuple, so dead proxies never reaped.
test_service_proxy_reaps_on_remote_service_down(_Config) ->
    Name = proxy_reap_svc,
    Target = 'fake_target@host',
    {ok, Proxy} = mycelium_proxy_sup:start_proxy(Name, Target),
    Ref = monitor(process, Proxy),
    mycelium_service_events:notify({service_down, Name, Target, killed}),
    receive
        {'DOWN', Ref, process, Proxy, _} -> ok
    after 1000 ->
        ct:fail("Proxy did not terminate on service_down")
    end,
    ok.

%% A service_down event for a different service must NOT terminate
%% the proxy.
test_service_proxy_ignores_unrelated_service_down(_Config) ->
    Name = proxy_keep_svc,
    Other = some_other_svc,
    Target = 'fake_target@host',
    {ok, Proxy} = mycelium_proxy_sup:start_proxy(Name, Target),
    Ref = monitor(process, Proxy),
    mycelium_service_events:notify({service_down, Other, Target, killed}),
    receive
        {'DOWN', Ref, process, Proxy, _} ->
            ct:fail("Proxy terminated on unrelated service_down")
    after 200 ->
        ok
    end,
    true = is_process_alive(Proxy),
    mycelium_proxy_sup:stop_proxy(Name),
    ok.
