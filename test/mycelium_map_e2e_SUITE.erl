%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E proof for mycelium_map. Spawns real BEAM peers under
%%% `-proto_dist mycelium', forms a cluster, and shows the convergence
%%% guarantees the feature promises: a put on one node appears on the
%%% others, a remove converges, a restarted map recovers its state via
%%% full-sync, and a map started AFTER the cluster has already formed pulls
%%% the existing peer state (the Stage 1b seed-on-start fix). Maps are
%%% node-local, so every node calls `mycelium:new_map/1'. The peer
%%% scaffolding mirrors mycelium_reminder_e2e_SUITE.
-module(mycelium_map_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        put_on_a_converges_on_b,
        remove_converges,
        survives_owner_restart_via_full_sync,
        map_created_after_cluster_formation_syncs_existing_peer_state
    ].

init_per_suite(Config) ->
    BasePort = 24000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
    [{base_port, BasePort} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    SuiteDir = ?config(priv_dir, Config),
    TcDir = filename:join(SuiteDir,
                          "tc_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(TcDir, "dummy")),
    DiscoveryDir = filename:join(TcDir, "discovery"),
    ok = filelib:ensure_dir(filename:join(DiscoveryDir, "dummy")),
    put(?MODULE, []),
    [{tc_dir, TcDir}, {discovery_dir, DiscoveryDir} | Config].

end_per_testcase(_Case, _Config) ->
    Peers = case get(?MODULE) of undefined -> []; L -> L end,
    [catch peer:stop(P) || P <- Peers],
    erase(?MODULE),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% A put on one node converges to the rest.
put_on_a_converges_on_b(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    new_map_everywhere(Peers, m),

    {Pa, _} = hd(Peers),
    ok = peer:call(Pa, mycelium, map_put, [m, k, v1]),
    wait_until(fun() -> converged(Peers, m, k, v1) end, 15000),
    ok.

%% A remove converges: the key disappears on every node.
remove_converges(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    new_map_everywhere(Peers, m),

    {Pa, _} = hd(Peers),
    ok = peer:call(Pa, mycelium, map_put, [m, k, v1]),
    wait_until(fun() -> converged(Peers, m, k, v1) end, 15000),
    ok = peer:call(Pa, mycelium, map_remove, [m, k]),
    wait_until(fun() -> absent(Peers, m, k) end, 15000),
    ok.

%% Crash a node's map owner; rest_for_one restarts owner+replica, and the
%% fresh instance must full-sync the existing key from its peers. Ongoing
%% gossip then resumes to the recovered node.
survives_owner_restart_via_full_sync(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    new_map_everywhere(Peers, m),

    {Pa, _} = hd(Peers),
    ok = peer:call(Pa, mycelium, map_put, [m, k, v1]),
    wait_until(fun() -> converged(Peers, m, k, v1) end, 15000),

    {Pc, _} = lists:last(Peers),
    Owner = peer:call(Pc, mycelium_map, owner_name, [m]),
    OldPid = peer:call(Pc, erlang, whereis, [Owner]),
    ?assert(is_pid(OldPid)),
    true = peer:call(Pc, erlang, exit, [OldPid, kill]),
    wait_until(fun() ->
        case peer:call(Pc, erlang, whereis, [Owner]) of
            P when is_pid(P), P =/= OldPid -> true;
            _ -> false
        end
    end, 15000),

    %% The recovered map re-syncs k=>v1 from its peers...
    wait_until(fun() -> map_get_on(Pc, m, k) =:= {ok, v1} end, 15000),
    %% ...and a later put still propagates to it.
    ok = peer:call(Pa, mycelium, map_put, [m, k2, v2]),
    wait_until(fun() -> map_get_on(Pc, m, k2) =:= {ok, v2} end, 15000),
    ok.

%% Stage 1b: a map started AFTER the cluster has already formed must seed
%% peers from the active view and pull the existing state - it gets no
%% peer_up for already-connected peers.
map_created_after_cluster_formation_syncs_existing_peer_state(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),

    %% Start the map on A and B only, and seed state there.
    [PeerA, PeerB, PeerC] = Peers,
    Hosts = [PeerA, PeerB],
    new_map_everywhere(Hosts, m),
    {Pa, _} = PeerA,
    {Pb, _} = PeerB,
    ok = peer:call(Pa, mycelium, map_put, [m, k1, v1]),
    ok = peer:call(Pb, mycelium, map_put, [m, k2, v2]),
    wait_until(fun() -> converged(Hosts, m, k1, v1)
                            andalso converged(Hosts, m, k2, v2) end, 15000),

    %% Now bring the map up on C, which already has A and B as peers.
    {Pc, _} = PeerC,
    {ok, _} = peer:call(Pc, mycelium, new_map, [m]),
    wait_until(fun() -> map_get_on(Pc, m, k1) =:= {ok, v1}
                            andalso map_get_on(Pc, m, k2) =:= {ok, v2} end, 15000),
    ok.

%%====================================================================
%% Map helpers
%%====================================================================

new_map_everywhere(Peers, Name) ->
    [ {ok, _} = peer:call(P, mycelium, new_map, [Name]) || {P, _N} <- Peers ].

map_get_on(Peer, Name, Key) ->
    peer:call(Peer, mycelium, map_get, [Name, Key]).

converged(Peers, Name, Key, Expected) ->
    lists:all(fun({P, _N}) -> map_get_on(P, Name, Key) =:= {ok, Expected} end, Peers).

absent(Peers, Name, Key) ->
    lists:all(fun({P, _N}) -> map_get_on(P, Name, Key) =:= not_found end, Peers).

%%====================================================================
%% Orchestration
%%====================================================================

start_cluster(Config) ->
    {Pa, NodeA, _, C1} = start_peer("a", Config),
    {Pb, NodeB, _, C2} = start_peer("b", C1),
    {Pc, NodeC, _, _}  = start_peer("c", C2),
    Peers = [{Pa, NodeA}, {Pb, NodeB}, {Pc, NodeC}],
    form_cluster(Peers),
    Peers.

form_cluster(Peers) ->
    Pairs = [{Pi, Nj} || {Pi, Ni} <- Peers, {_Pj, Nj} <- Peers, Ni =/= Nj],
    [ wait_until(fun() -> connect_ok(Pi, Nj) end, 30000) || {Pi, Nj} <- Pairs ],
    [ _ = peer:call(Pi, mycelium, join, [Nj]) || {Pi, Nj} <- Pairs ],
    wait_until(fun() -> fully_connected(Peers) end, 30000).

connect_ok(P, N) ->
    peer:call(P, net_kernel, connect_node, [N], 15000) =:= true
        andalso lists:member(N, peer:call(P, erlang, nodes, [])).

fully_connected(Peers) ->
    Nodes = [N || {_P, N} <- Peers],
    lists:all(fun({P, N}) ->
        AV = peer:call(P, mycelium, active_view, []),
        lists:all(fun(O) -> lists:member(O, AV) end, Nodes -- [N])
    end, Peers).

all_members_are(Peers, Expected) ->
    Want = lists:sort(Expected),
    lists:all(fun({P, _N}) ->
        lists:sort(peer:call(P, mycelium, members, [])) =:= Want
    end, Peers).

%%====================================================================
%% Peer setup (mirrors mycelium_reminder_e2e_SUITE; short lease timings)
%%====================================================================

start_peer(Suffix, Config) ->
    TcDir = ?config(tc_dir, Config),
    NodeDir = filename:join(TcDir, Suffix),
    ok = filelib:ensure_dir(filename:join(NodeDir, "dummy")),
    QuicDir = filename:join(NodeDir, "data/quic"),
    KeysDir = filename:join(NodeDir, "data/keys"),
    ok = filelib:ensure_dir(filename:join(QuicDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(KeysDir, "dummy")),
    DiscoveryDir = ?config(discovery_dir, Config),
    BasePort = ?config(base_port, Config),
    Port = next_port(BasePort),
    Name = list_to_atom("myc_" ++ Suffix ++ "_"
                        ++ integer_to_list(erlang:unique_integer([positive]))),
    BaseArgs = [
        "-proto_dist", "mycelium",
        "-epmd_module", "mycelium_epmd",
        "-start_epmd", "false",
        "-mycelium_dist_port",     integer_to_list(Port),
        "-mycelium_dist_cert_dir", QuicDir,
        "-setcookie", "mycelium_ct",
        "-mycelium", "auth_key_dir",  quote(KeysDir),
        "-mycelium", "discovery_dir", quote(DiscoveryDir),
        "-mycelium", "active_size",   "5",
        "-mycelium", "member_heartbeat_ms", "500",
        "-mycelium", "member_ttl_ms",       "2000",
        "-mycelium", "member_skew_ms",      "60000"
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
    put(?MODULE, [Pid | case get(?MODULE) of undefined -> []; L -> L end]),
    {Pid, Node, NodeDir, Config}.

quote(S) ->
    "\"" ++ S ++ "\"".

next_port(BasePort) ->
    Key = {?MODULE, next_port_offset},
    N = case get(Key) of undefined -> 0; X -> X end,
    put(Key, N + 1),
    BasePort + N.

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case catch Fun() of
        true -> ok;
        _ ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true  -> ?assert(false, "wait_until timed out");
                false -> timer:sleep(200), wait_loop(Fun, Deadline)
            end
    end.
