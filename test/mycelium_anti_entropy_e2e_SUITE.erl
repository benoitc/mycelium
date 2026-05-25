%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E proof that periodic anti-entropy reconverges a value-carrying store
%%% (here a mycelium_map) WITHOUT a fresh peer_up. It is an isolating A/B
%%% test, NOT a partition test: a healed partition would reconverge via
%%% HyParView shuffle/forward-join peer_ups, which would not prove
%%% anti-entropy was the cause.
%%%
%%% Two nodes join (one peer_up). We let the join's one-shot full-sync drain
%%% while both maps are empty, then inject a key into A ONLY via the replica
%%% merge callback (no broadcast, no new peer_up). The ONLY path that can
%%% carry it to B is anti-entropy:
%%%   - off (replica_anti_entropy_ms = 0): B stays behind.
%%%   - on  (~500ms): B converges.
-module(mycelium_anti_entropy_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [antientropy_off_leaves_peer_behind, antientropy_converges].

init_per_suite(Config) ->
    BasePort = 26000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
    [{base_port, BasePort} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    TcDir = filename:join(
        ?config(priv_dir, Config),
        "tc_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    DiscoveryDir = filename:join(TcDir, "discovery"),
    ok = filelib:ensure_dir(filename:join(DiscoveryDir, "dummy")),
    put(?MODULE, []),
    [{tc_dir, TcDir}, {discovery_dir, DiscoveryDir} | Config].

end_per_testcase(_Case, _Config) ->
    Peers =
        case get(?MODULE) of
            undefined -> [];
            L -> L
        end,
    [catch peer:stop(P) || P <- Peers],
    erase(?MODULE),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% With anti-entropy OFF, the divergence persists: nothing re-delivers the
%% injected key to B (no broadcast, no new peer_up, shuffle carries no data).
%% This is also the negative control that establishes the divergence is real:
%% with the interval at 0 no timer is ever armed, so asserting B is empty is
%% stable (the ON case cannot make that assertion without racing its own timer).
antientropy_off_leaves_peer_behind(Config) ->
    {Pa, _NodeA, Pb, _NodeB} = start_pair(Config, 0),
    inject(Pa),
    ?assertEqual(not_found, map_get_on(Pb, m, k)),
    %% Settle far longer than several ~500ms intervals would need; with
    %% anti-entropy off this is a real negative, not an impatient one.
    timer:sleep(4000),
    ?assertEqual(not_found, map_get_on(Pb, m, k)).

%% With anti-entropy ON, B pulls A on a tick and converges. The two cases are
%% identical but for the interval (0 vs 500), so B converging here while it
%% stays behind in the OFF case pins anti-entropy as the cause.
antientropy_converges(Config) ->
    {Pa, _NodeA, Pb, _NodeB} = start_pair(Config, 500),
    inject(Pa),
    wait_until(fun() -> map_get_on(Pb, m, k) =:= {ok, v} end, 15000),
    ok.

%%====================================================================
%% Scenario helpers
%%====================================================================

%% Start A and B with the given anti-entropy interval, host map `m' on both,
%% join them, and let the join's one-shot full-sync fully drain while both
%% maps are still empty.
start_pair(Config, AeMs) ->
    {Pa, NodeA} = start_peer("a", AeMs, Config),
    {Pb, NodeB} = start_peer("b", AeMs, Config),
    {ok, _} = peer:call(Pa, mycelium, new_map, [m]),
    {ok, _} = peer:call(Pb, mycelium, new_map, [m]),
    %% Connect + join so B has A as a replica peer (the anti-entropy target).
    wait_until(fun() -> connect_ok(Pb, NodeA) end, 30000),
    ok = peer:call(Pb, mycelium, join, [NodeA]),
    wait_until(
        fun() ->
            lists:member(NodeA, peer:call(Pb, mycelium, active_view, [])) andalso
                lists:member(NodeB, peer:call(Pa, mycelium, active_view, []))
        end,
        30000
    ),
    %% Drain the one-shot peer_up full-sync + request_sync round-trip WHILE
    %% BOTH MAPS ARE EMPTY. "B lists A" already flipped on peer_up, before
    %% those deferred self-messages ran, so use a real settle: a generous
    %% wait plus flushing both replica mailboxes (sys:get_state forces the
    %% queued do_full_sync/request_sync to be processed).
    Rep = peer:call(Pa, mycelium_map, replica_name, [m]),
    timer:sleep(2000),
    flush_replicas([Pa, Pb], Rep),
    timer:sleep(500),
    flush_replicas([Pa, Pb], Rep),
    {Pa, NodeA, Pb, NodeB}.

%% Inject a well-formed entry into A ONLY (a "learned" delta with a valid dot
%% authored by a third origin node, so the map's crdt_wire validator accepts
%% it) and assert A actually took it. This guards against a false green where a
%% dropped/malformed inject leaves A empty too. The "B does not have it" half of
%% the divergence is asserted by the caller where it is race-free (the OFF case).
inject(Pa) ->
    HLC = peer:call(Pa, mycelium_hlc, now, []),
    Delta = #{k => {value, v, #{{'origin@127.0.0.1', HLC} => true}}},
    Rep = peer:call(Pa, mycelium_map, replica_name, [m]),
    ok = peer:call(Pa, mycelium_map, replica_merge_delta, [Rep, Delta]),
    wait_until(fun() -> map_get_on(Pa, m, k) =:= {ok, v} end, 5000).

map_get_on(Peer, Name, Key) ->
    peer:call(Peer, mycelium, map_get, [Name, Key]).

flush_replicas(Peers, Rep) ->
    [catch peer:call(P, sys, get_state, [Rep]) || P <- Peers],
    ok.

connect_ok(P, N) ->
    peer:call(P, net_kernel, connect_node, [N], 15000) =:= true andalso
        lists:member(N, peer:call(P, erlang, nodes, [])).

%%====================================================================
%% Peer setup (mirrors mycelium_reminder_e2e_SUITE)
%%====================================================================

start_peer(Suffix, AeMs, Config) ->
    TcDir = ?config(tc_dir, Config),
    NodeDir = filename:join(TcDir, Suffix),
    QuicDir = filename:join(NodeDir, "data/quic"),
    KeysDir = filename:join(NodeDir, "data/keys"),
    RemindersDir = filename:join(NodeDir, "data/reminders"),
    [
        ok = filelib:ensure_dir(filename:join(D, "dummy"))
     || D <- [QuicDir, KeysDir, RemindersDir]
    ],
    DiscoveryDir = ?config(discovery_dir, Config),
    Port = next_port(?config(base_port, Config)),
    Name = list_to_atom(
        "myc_" ++ Suffix ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    BaseArgs = [
        "-proto_dist",
        "mycelium",
        "-epmd_module",
        "mycelium_epmd",
        "-start_epmd",
        "false",
        "-mycelium_dist_port",
        integer_to_list(Port),
        "-mycelium_dist_cert_dir",
        QuicDir,
        "-setcookie",
        "mycelium_ct",
        "-mycelium",
        "auth_key_dir",
        quote(KeysDir),
        "-mycelium",
        "discovery_dir",
        quote(DiscoveryDir),
        "-mycelium",
        "reminder_data_dir",
        quote(RemindersDir),
        "-mycelium",
        "active_size",
        "5",
        "-mycelium",
        "replica_anti_entropy_ms",
        integer_to_list(AeMs)
    ],
    PaArgs = lists:flatmap(fun(P) -> ["-pa", P] end, code:get_path()),
    Args = PaArgs ++ BaseArgs,
    {ok, Pid, Node} = peer:start(#{
        name => Name,
        longnames => true,
        host => "127.0.0.1",
        connection => standard_io,
        args => Args
    }),
    {ok, _Started} = peer:call(Pid, application, ensure_all_started, [mycelium]),
    put(?MODULE, [Pid | tracked()]),
    {Pid, Node}.

tracked() ->
    case get(?MODULE) of
        undefined -> [];
        L -> L
    end.

quote(S) ->
    "\"" ++ S ++ "\"".

next_port(BasePort) ->
    Key = {?MODULE, next_port_offset},
    N =
        case get(Key) of
            undefined -> 0;
            X -> X
        end,
    put(Key, N + 1),
    BasePort + N.

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
                    timer:sleep(200),
                    wait_loop(Fun, Deadline)
            end
    end.
