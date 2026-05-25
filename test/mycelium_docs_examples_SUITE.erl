%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Executable documentation: each case runs the headline runnable example
%%% from a feature doc and asserts the EXACT result the doc shows, so a
%%% snippet that drifts from the code breaks here. Single node only;
%%% multi-node behaviour is proven in the per-feature e2e suites.
%%%
%%% Doc sources:
%%%   replicated_map_flags  -> docs/how-to/share-replicated-state.md
%%%   durable_reminder      -> docs/how-to/schedule-durable-jobs.md
%%%   leader_singleton      -> docs/concepts/leader-election.md
%%%   service_registry      -> docs/concepts/service-registry.md
-module(mycelium_docs_examples_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("mycelium.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([
    replicated_map_flags/1,
    durable_reminder/1,
    leader_singleton/1,
    service_registry/1
]).

all() ->
    [replicated_map_flags, durable_reminder, leader_singleton, service_registry].

init_per_testcase(_Case, Config) ->
    Base = filename:join(
        ?config(priv_dir, Config),
        "ex_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    application:set_env(mycelium, mycelium_map_data_dir, filename:join(Base, "maps")),
    application:set_env(mycelium, reminder_data_dir, filename:join(Base, "reminders")),
    {ok, _} = application:ensure_all_started(mycelium),
    Config.

end_per_testcase(_Case, _Config) ->
    application:stop(mycelium),
    ok.

%%====================================================================
%% docs/how-to/share-replicated-state.md
%%====================================================================

replicated_map_flags(_Config) ->
    {ok, _} = mycelium:new_map(flags),
    ok = mycelium:map_put(flags, dark_mode, true),
    ?assertEqual({ok, true}, mycelium:map_get(flags, dark_mode)),
    ?assertEqual(not_found, mycelium:map_get(flags, missing)),
    ?assertEqual([dark_mode], mycelium:map_keys(flags)),
    ?assertEqual([{dark_mode, true}], mycelium:map_to_list(flags)),
    %% Validator example: a non-boolean is rejected.
    {ok, _} = mycelium:new_map(vflags, #{validator => fun erlang:is_boolean/1}),
    ?assertEqual({error, invalid_value}, mycelium:map_put(vflags, k, "yes")),
    %% Removal.
    ok = mycelium:map_remove(flags, dark_mode),
    ?assertEqual(not_found, mycelium:map_get(flags, dark_mode)).

%%====================================================================
%% docs/how-to/schedule-durable-jobs.md
%%====================================================================

durable_reminder(_Config) ->
    ok = mycelium:subscribe_reminders(),
    Key = {nightly_rollup, {2026, 5, 25}},
    Payload = #{date => {2026, 5, 25}},
    ok = mycelium:remind_after(Key, 150, Payload),
    receive
        {mycelium_reminder, K, P, Fence} ->
            ?assertEqual(Key, K),
            ?assertEqual(Payload, P),
            ?assert(is_integer(Fence))
    after 5000 ->
        ct:fail(reminder_did_not_fire)
    end,
    %% cancel_reminder/1 stops a not-yet-fired reminder.
    Key2 = {retry, 1},
    ok = mycelium:remind_after(Key2, 60000, #{}),
    ok = mycelium:cancel_reminder(Key2),
    receive
        {mycelium_reminder, Key2, _, _} -> ct:fail(cancelled_reminder_fired)
    after 300 ->
        ok
    end.

%%====================================================================
%% docs/concepts/leader-election.md (public mycelium: facade)
%%====================================================================

leader_singleton(_Config) ->
    %% Sole candidate on a single node wins immediately.
    {ok, {leader, Fence}} = mycelium:lead(report_roller),
    ?assert(is_integer(Fence)),
    ?assertEqual(true, mycelium:is_leader(report_roller)),
    ?assertEqual({ok, Fence}, mycelium:fence(report_roller)),
    ?assertEqual({ok, node(), self()}, mycelium:leader(report_roller)),
    %% Stepping down clears leadership (no `revoked' is sent to the caller).
    ok = mycelium:resign(report_roller),
    ?assertEqual(false, mycelium:is_leader(report_roller)),
    ?assertEqual({error, not_leader}, mycelium:fence(report_roller)),
    ?assertEqual({error, no_leader}, mycelium:leader(report_roller)).

%%====================================================================
%% docs/concepts/service-registry.md
%%====================================================================

service_registry(_Config) ->
    ok = mycelium:register_service(my_worker, #{role => primary}),
    %% Local pid lookups.
    ?assertEqual({ok, self()}, mycelium:lookup_local(my_worker)),
    ?assertEqual({ok, self()}, mycelium:whereis_service(my_worker)),
    %% lookup/1 returns #service_entry{} records (name, pid, node, meta).
    {ok, [Entry]} = mycelium:lookup(my_worker),
    ?assertMatch(
        #service_entry{
            name = my_worker,
            pid = _,
            node = _,
            meta = #{role := primary}
        },
        Entry
    ),
    ?assertEqual(self(), Entry#service_entry.pid),
    ?assertEqual(node(), Entry#service_entry.node).
