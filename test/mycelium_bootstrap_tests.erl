%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_bootstrap: the seed auto-join worker. Single
%%% node, mycelium:active_view/0 and mycelium:join/1 are mecked so the
%%% join-tick logic is exercised without a real cluster.
-module(mycelium_bootstrap_tests).

-include_lib("eunit/include/eunit.hrl").

set_contacts(Contacts) ->
    application:set_env(mycelium, contact_nodes, Contacts),
    %% Large retry so the init timer does not race the test.
    application:set_env(mycelium, contact_retry_ms, 60000).

clear_contacts() ->
    application:unset_env(mycelium, contact_nodes),
    application:unset_env(mycelium, contact_retry_ms).

%% A seed (no contacts) has nothing to bootstrap from: init returns ignore.
init_ignores_without_contacts_test() ->
    set_contacts([]),
    try
        ?assertEqual(ignore, mycelium_bootstrap:start_link()),
        ?assertEqual(undefined, whereis(mycelium_bootstrap))
    after
        clear_contacts()
    end.

%% With contacts, init starts and arms a timer; contacts() drops our own
%% node from the configured list (state is {state, Contacts, RetryMs}).
init_filters_self_and_starts_test() ->
    set_contacts([node(), 'seed@h']),
    {ok, Pid} = mycelium_bootstrap:start_link(),
    try
        ?assertMatch({state, ['seed@h'], 60000}, sys:get_state(Pid))
    after
        gen_server:stop(Pid),
        clear_contacts()
    end.

%% A join tick with an empty active view requests a join to every contact.
join_contacts_joins_when_isolated_test() ->
    meck:new(mycelium, [passthrough]),
    try
        meck:expect(mycelium, active_view, fun() -> [] end),
        meck:expect(mycelium, join, fun(_) -> ok end),
        set_contacts(['seed1@h', 'seed2@h']),
        {ok, Pid} = mycelium_bootstrap:start_link(),
        Pid ! join_contacts,
        %% Force the info to be processed before asserting.
        _ = sys:get_state(Pid),
        ?assert(meck:called(mycelium, join, ['seed1@h'])),
        ?assert(meck:called(mycelium, join, ['seed2@h'])),
        gen_server:stop(Pid)
    after
        meck:unload(mycelium),
        clear_contacts()
    end.

%% A join tick with a non-empty active view does nothing.
join_contacts_skips_when_in_cluster_test() ->
    meck:new(mycelium, [passthrough]),
    try
        meck:expect(mycelium, active_view, fun() -> ['peer@h'] end),
        meck:expect(mycelium, join, fun(_) -> ok end),
        set_contacts(['seed@h']),
        {ok, Pid} = mycelium_bootstrap:start_link(),
        Pid ! join_contacts,
        _ = sys:get_state(Pid),
        ?assertEqual(0, meck:num_calls(mycelium, join, '_')),
        gen_server:stop(Pid)
    after
        meck:unload(mycelium),
        clear_contacts()
    end.

%% call/cast/unknown-info are inert; stop exercises terminate/2.
callbacks_are_inert_test() ->
    set_contacts(['seed@h']),
    {ok, Pid} = mycelium_bootstrap:start_link(),
    try
        ?assertEqual(ok, gen_server:call(Pid, anything)),
        ?assertEqual(ok, gen_server:cast(Pid, anything)),
        Pid ! some_other_info,
        _ = sys:get_state(Pid)
    after
        gen_server:stop(Pid),
        clear_contacts()
    end.
