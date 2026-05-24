%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Subscriptions must survive a restart of the event SOURCE. Each source
%%% keeps its subscriber list in ephemeral gen_server state, and a source
%%% lives in a different supervision subtree from its subscribers, so a
%%% source crash does not restart them. mycelium_source_monitor makes each
%%% subscriber re-subscribe after the source comes back. These cases kill
%%% a source and assert its subscribers re-appear (and never died).
-module(mycelium_source_monitor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    plumtree_resubscribes_replicas/1,
    hyparview_events_resubscribes_watchers/1,
    shard_resubscribes_reminder/1
]).

%% Index of the `subscribers' field in each source's #state record
%% (white-box; a reorder would fail these tests, which is intended).
-define(PLUMTREE_SUBS_IDX, 6).
-define(HYPARVIEW_EVENTS_SUBS_IDX, 2).
-define(SHARD_SUBS_IDX, 9).

all() ->
    [
        plumtree_resubscribes_replicas,
        hyparview_events_resubscribes_watchers,
        shard_resubscribes_reminder
    ].

init_per_testcase(_Case, Config) ->
    application:set_env(mycelium, member_heartbeat_ms, 100),
    application:set_env(mycelium, member_ttl_ms, 300),
    application:set_env(mycelium, member_skew_ms, 60000),
    application:set_env(mycelium, reminder_scan_ms, 200),
    {ok, _} = application:ensure_all_started(mycelium),
    Config.

end_per_testcase(_Case, _Config) ->
    application:stop(mycelium),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% The four mycelium_replica instances subscribe to plumtree. After
%% plumtree restarts, they re-subscribe (without having died).
plumtree_resubscribes_replicas(_Config) ->
    Old = whereis(mycelium_plumtree),
    OldSubs = subs(mycelium_plumtree, ?PLUMTREE_SUBS_IDX),
    ?assert(length(OldSubs) >= 1),

    exit(Old, kill),
    wait_until(fun() -> restarted(mycelium_plumtree, Old) end, 5000),
    wait_until(
        fun() -> subset(OldSubs, subs(mycelium_plumtree, ?PLUMTREE_SUBS_IDX)) end,
        5000
    ),

    [?assert(is_process_alive(P)) || P <- OldSubs],
    ok.

%% plumtree and a replica both subscribe to hyparview_events from other
%% subtrees, so they survive its restart and must re-subscribe.
hyparview_events_resubscribes_watchers(_Config) ->
    Old = whereis(mycelium_hyparview_events),
    Watchers = [whereis(mycelium_reminder_replica), whereis(mycelium_plumtree)],
    [?assert(is_pid(P)) || P <- Watchers],
    ?assert(subset(Watchers, subs(mycelium_hyparview_events, ?HYPARVIEW_EVENTS_SUBS_IDX))),

    exit(Old, kill),
    wait_until(fun() -> restarted(mycelium_hyparview_events, Old) end, 5000),
    wait_until(
        fun() ->
            subset(Watchers, subs(mycelium_hyparview_events, ?HYPARVIEW_EVENTS_SUBS_IDX))
        end,
        5000
    ),

    [?assert(is_process_alive(P)) || P <- Watchers],
    %% They were not restarted (different subtrees), so their pids hold.
    ?assertEqual(whereis(mycelium_plumtree), lists:last(Watchers)),
    ok.

%% Non-replica subscriber: mycelium_reminder subscribes to shard
%% ownership events and must re-subscribe after shard restarts.
shard_resubscribes_reminder(_Config) ->
    RemPid = whereis(mycelium_reminder),
    Old = whereis(mycelium_shard),
    ?assert(lists:member(RemPid, subs(mycelium_shard, ?SHARD_SUBS_IDX))),

    exit(Old, kill),
    wait_until(fun() -> restarted(mycelium_shard, Old) end, 5000),
    wait_until(
        fun() -> lists:member(RemPid, subs(mycelium_shard, ?SHARD_SUBS_IDX)) end,
        5000
    ),

    %% The reminder never died (it lives in a different subtree).
    ?assertEqual(RemPid, whereis(mycelium_reminder)),
    ?assert(is_process_alive(RemPid)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

subs(Name, Idx) ->
    case whereis(Name) of
        undefined -> [];
        _ -> maps:keys(element(Idx, sys:get_state(Name)))
    end.

restarted(Name, Old) ->
    case whereis(Name) of
        undefined -> false;
        New -> is_pid(New) andalso New =/= Old
    end.

subset(Wanted, Have) ->
    lists:all(fun(P) -> lists:member(P, Have) end, Wanted).

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case catch Fun() of
        true ->
            ok;
        _ ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    ?assert(false, "wait_until timed out");
                false ->
                    timer:sleep(25),
                    wait_loop(Fun, Deadline)
            end
    end.
