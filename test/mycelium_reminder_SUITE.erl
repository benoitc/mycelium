%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Single-node logic coverage for durable reminders. As the sole
%%% member, this node owns every key, so reminders arm and fire locally.
%%% Multi-node durability (a survivor fires after the owner dies) is
%%% proven in mycelium_reminder_e2e_SUITE.
-module(mycelium_reminder_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    fires_as_sole_owner/1,
    remind_after_fires/1,
    delivers_stable_fence/1,
    cancel_prevents_fire/1,
    fires_exactly_once/1,
    reset_replaces_reminder/1,
    stale_add_does_not_resurrect/1,
    malformed_gossip_does_not_crash/1,
    tombstones_gc_after_ttl/1
]).

all() ->
    [
        fires_as_sole_owner,
        remind_after_fires,
        delivers_stable_fence,
        cancel_prevents_fire,
        fires_exactly_once,
        reset_replaces_reminder,
        stale_add_does_not_resurrect,
        malformed_gossip_does_not_crash,
        tombstones_gc_after_ttl
    ].

init_per_testcase(Case, Config) ->
    %% Short timings so the cases run quickly.
    application:set_env(mycelium, member_heartbeat_ms, 100),
    application:set_env(mycelium, member_ttl_ms, 300),
    application:set_env(mycelium, member_skew_ms, 60000),
    application:set_env(mycelium, reminder_scan_ms, 200),
    %% Only the GC case wants a tiny tombstone horizon; the others keep
    %% tombstones around (e.g. the resurrection case relies on it).
    TombTtl = case Case of
        tombstones_gc_after_ttl -> 50;
        _                       -> 3600000
    end,
    application:set_env(mycelium, reminder_tombstone_ttl_ms, TombTtl),
    %% Reminders persist by default; give each case a fresh store dir so a
    %% prior case's reminders are never reloaded.
    Dir = filename:join(?config(priv_dir, Config),
                        "reminders_" ++ integer_to_list(erlang:unique_integer([positive]))),
    application:set_env(mycelium, reminder_data_dir, Dir),
    {ok, _} = application:ensure_all_started(mycelium),
    ok = mycelium:subscribe_reminders(),
    Config.

end_per_testcase(_Case, _Config) ->
    application:stop(mycelium),
    ok.

%%====================================================================
%% Cases
%%====================================================================

fires_as_sole_owner(_Config) ->
    ?assert(mycelium:is_owner(rk)),
    ok = mycelium:remind(rk, now_ms() + 150, hello),
    receive
        {mycelium_reminder, rk, hello, Fence} ->
            ?assert(is_integer(Fence) andalso Fence >= 0)
    after 2000 ->
        ct:fail(reminder_did_not_fire)
    end.

remind_after_fires(_Config) ->
    ok = mycelium:remind_after(rk, 150, world),
    receive
        {mycelium_reminder, rk, world, _Fence} -> ok
    after 2000 ->
        ct:fail(reminder_after_did_not_fire)
    end.

delivers_stable_fence(_Config) ->
    ok = mycelium:remind(rk, now_ms() + 100, payload),
    Fence = receive
        {mycelium_reminder, rk, payload, F} -> F
    after 2000 ->
        ct:fail(no_fire)
    end,
    %% The fence is the packed version HLC: a positive, comparable id.
    ?assert(is_integer(Fence)),
    ?assert(Fence > 0).

cancel_prevents_fire(_Config) ->
    ok = mycelium:remind(rk, now_ms() + 400, nope),
    ok = mycelium:cancel_reminder(rk),
    receive
        {mycelium_reminder, rk, _, _} -> ct:fail(fired_after_cancel)
    after 900 ->
        ok
    end.

fires_exactly_once(_Config) ->
    ok = mycelium:remind(rk, now_ms() + 150, once),
    receive
        {mycelium_reminder, rk, once, _} -> ok
    after 2000 ->
        ct:fail(no_fire)
    end,
    %% No second delivery: the fire tombstones the entry, so neither the
    %% scan nor a re-merge re-arms it.
    receive
        {mycelium_reminder, rk, once, _} -> ct:fail(double_fire)
    after 800 ->
        ok
    end.

reset_replaces_reminder(_Config) ->
    %% Arm far out, then re-set the same key to fire soon with a new
    %% payload. The stale far-future timer must not fire the old payload.
    ok = mycelium:remind(rk, now_ms() + 60000, stale),
    ok = mycelium:remind(rk, now_ms() + 150, fresh),
    receive
        {mycelium_reminder, rk, fresh, _} -> ok;
        {mycelium_reminder, rk, stale, _} -> ct:fail(fired_stale_version)
    after 2000 ->
        ct:fail(no_fire)
    end,
    receive
        {mycelium_reminder, rk, _, _} -> ct:fail(unexpected_second_fire)
    after 600 ->
        ok
    end.

%% A delayed re-gossip of the original add (as mycelium_replica would
%% deliver it) must not resurrect a reminder that already fired: the
%% fire-time tombstone out-ranks the older add by HLC.
stale_add_does_not_resurrect(_Config) ->
    H = mycelium_hlc:now(),
    FireAt = now_ms() + 150,
    Val = {FireAt, boo, H},
    Delta = #{rk => {value, Val, #{{'ghost@127.0.0.1', H} => true}}},
    mycelium_reminder:replica_merge_delta(mycelium_reminder_replica, Delta),
    receive
        {mycelium_reminder, rk, boo, _} -> ok
    after 2000 ->
        ct:fail(no_fire)
    end,
    %% Replay the stale add; the tombstone written at fire time wins.
    mycelium_reminder:replica_merge_delta(mycelium_reminder_replica, Delta),
    receive
        {mycelium_reminder, rk, boo, _} -> ct:fail(resurrected)
    after 800 ->
        ok
    end.

%% Malformed gossip must not crash the reminder server (nor the shared
%% HLC server). Covers the absorb_clock/merge crash paths, not just the
%% leaf payload shape.
malformed_gossip_does_not_crash(_Config) ->
    H = mycelium_hlc:now(),
    GoodDots = #{{'ghost@127.0.0.1', H} => true},
    GoodVal = {now_ms() + 100000, payload, H},
    Bad = [
        #{ka => {value, garbage, GoodDots}},          %% payload not 3-tuple
        #{kb => {value, GoodVal, not_a_map}},          %% dots not a map
        #{kc => {value, GoodVal, #{}}},                %% empty dot map
        #{kd => {value, GoodVal, #{bad_key => true}}}, %% malformed dot key
        #{ke => {tombstone, not_a_timestamp}}          %% malformed tombstone
    ],
    [ mycelium_reminder:replica_merge_delta(mycelium_reminder_replica, D) || D <- Bad ],
    %% Force the casts to be processed, then assert both servers survived.
    _ = sys:get_state(mycelium_reminder),
    ?assert(is_process_alive(whereis(mycelium_reminder))),
    ?assert(is_process_alive(whereis(mycelium_hlc))),
    %% And a well-formed reminder still fires.
    ok = mycelium:remind(good_key, now_ms() + 150, ok_payload),
    receive
        {mycelium_reminder, good_key, ok_payload, _} -> ok
    after 2000 ->
        ct:fail(good_reminder_did_not_fire)
    end.

%% Fire/cancel tombstones are swept once older than the TTL, so the
%% replicated store does not grow without bound.
tombstones_gc_after_ttl(_Config) ->
    ok = mycelium:remind(gk, now_ms() + 100, p),
    receive
        {mycelium_reminder, gk, p, _} -> ok
    after 2000 ->
        ct:fail(no_fire)
    end,
    %% After the fire the key is a tombstone; the sweep (ttl 50ms, scan
    %% 200ms) drops it within a couple of scans.
    wait_until(fun() ->
        mycelium_ormap:get_entry(gk, reminders_map()) =:= not_found
    end, 3000),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

now_ms() ->
    erlang:system_time(millisecond).

%% White-box read of the reminder gen_server's OR-map. The #state record
%% is {state, scan_ms, tombstone_ttl_ms, reminders, timers, subscribers},
%% so the reminders map is element 4.
reminders_map() ->
    element(4, sys:get_state(mycelium_reminder)).

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        _ ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true  -> ?assert(false, "wait_until timed out");
                false -> timer:sleep(25), wait_loop(Fun, Deadline)
            end
    end.
