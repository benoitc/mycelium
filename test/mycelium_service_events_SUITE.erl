%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_service_events_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("mycelium.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    test_subscribe_receive_events/1,
    test_unsubscribe/1,
    test_filter_by_name/1,
    test_filter_by_pattern/1,
    test_service_down_event/1,
    test_multiple_subscribers/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, service_events}].

groups() ->
    [
        {service_events, [sequence], [
            test_subscribe_receive_events,
            test_unsubscribe,
            test_filter_by_name,
            test_filter_by_pattern,
            test_service_down_event,
            test_multiple_subscribers
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

test_subscribe_receive_events(_Config) ->
    %% Subscribe to all events
    ok = mycelium:subscribe_services(),

    %% Register a service
    ok = mycelium:register_service(event_test_svc),

    %% Should receive registered event
    receive
        {mycelium_service_event, {service_registered, event_test_svc, _Node}} ->
            ok
    after 1000 ->
        ct:fail("Did not receive service_registered event")
    end,

    %% Unregister the service
    ok = mycelium:unregister_service(event_test_svc),

    %% Should receive unregistered event
    receive
        {mycelium_service_event, {service_unregistered, event_test_svc, _Node2}} ->
            ok
    after 1000 ->
        ct:fail("Did not receive service_unregistered event")
    end,

    ok = mycelium:unsubscribe_services(self()),
    ok.

test_unsubscribe(_Config) ->
    ok = mycelium:subscribe_services(),
    ok = mycelium:unsubscribe_services(self()),

    %% Register a service
    ok = mycelium:register_service(unsub_test_svc),

    %% Should NOT receive event after unsubscribe
    receive
        {mycelium_service_event, _} ->
            ct:fail("Received event after unsubscribe")
    after 200 ->
        ok
    end,

    ok = mycelium:unregister_service(unsub_test_svc),
    ok.

test_filter_by_name(_Config) ->
    %% Subscribe only to events for specific service
    ok = mycelium_service_events:subscribe(self(), {name, filtered_svc}),

    %% Register a different service
    ok = mycelium:register_service(other_svc),

    %% Should NOT receive event for other_svc
    receive
        {mycelium_service_event, {service_registered, other_svc, _}} ->
            ct:fail("Received event for filtered service")
    after 200 ->
        ok
    end,

    %% Register the filtered service
    ok = mycelium:register_service(filtered_svc),

    %% Should receive event for filtered_svc
    receive
        {mycelium_service_event, {service_registered, filtered_svc, _Node}} ->
            ok
    after 1000 ->
        ct:fail("Did not receive event for subscribed service")
    end,

    ok = mycelium:unregister_service(other_svc),
    ok = mycelium:unregister_service(filtered_svc),
    ok = mycelium_service_events:unsubscribe(self()),
    ok.

test_filter_by_pattern(_Config) ->
    %% Subscribe to services matching pattern "test_.*"
    ok = mycelium_service_events:subscribe(self(), {pattern, <<"test_.*">>}),

    %% Register a service that doesn't match
    ok = mycelium:register_service(prod_svc),

    %% Should NOT receive event
    receive
        {mycelium_service_event, {service_registered, prod_svc, _}} ->
            ct:fail("Received event for non-matching service")
    after 200 ->
        ok
    end,

    %% Register a service that matches
    ok = mycelium:register_service(test_matching_svc),

    %% Should receive event
    receive
        {mycelium_service_event, {service_registered, test_matching_svc, _Node}} ->
            ok
    after 1000 ->
        ct:fail("Did not receive event for matching service")
    end,

    ok = mycelium:unregister_service(prod_svc),
    ok = mycelium:unregister_service(test_matching_svc),
    ok = mycelium_service_events:unsubscribe(self()),
    ok.

test_service_down_event(_Config) ->
    ok = mycelium:subscribe_services(),

    %% Spawn a process that registers a service then dies
    Parent = self(),
    Pid = spawn(fun() ->
        ok = mycelium:register_service(dying_svc),
        Parent ! registered,
        receive
            stop -> ok
        end
    end),

    receive
        registered -> ok
    end,

    %% Consume the registered event
    receive
        {mycelium_service_event, {service_registered, dying_svc, _}} -> ok
    after 1000 ->
        ct:fail("Did not receive registered event")
    end,

    %% Kill the process
    Pid ! stop,

    %% Should receive service_down event
    receive
        {mycelium_service_event, {service_down, dying_svc, _Node, _Reason}} ->
            ok
    after 1000 ->
        ct:fail("Did not receive service_down event")
    end,

    ok = mycelium:unsubscribe_services(self()),
    ok.

test_multiple_subscribers(_Config) ->
    %% Create multiple subscriber processes
    Parent = self(),
    Sub1 = spawn(fun() -> subscriber_loop(Parent, sub1) end),
    Sub2 = spawn(fun() -> subscriber_loop(Parent, sub2) end),

    ok = mycelium:subscribe_services(Sub1),
    ok = mycelium:subscribe_services(Sub2),

    %% Register a service
    ok = mycelium:register_service(multi_sub_svc),

    %% Both subscribers should receive the event
    receive
        {sub1, received, {service_registered, multi_sub_svc, _}} -> ok
    after 1000 ->
        ct:fail("Sub1 did not receive event")
    end,

    receive
        {sub2, received, {service_registered, multi_sub_svc, _}} -> ok
    after 1000 ->
        ct:fail("Sub2 did not receive event")
    end,

    ok = mycelium:unregister_service(multi_sub_svc),
    Sub1 ! stop,
    Sub2 ! stop,
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

subscriber_loop(Parent, Name) ->
    receive
        {mycelium_service_event, Event} ->
            Parent ! {Name, received, Event},
            subscriber_loop(Parent, Name);
        stop ->
            ok
    end.
