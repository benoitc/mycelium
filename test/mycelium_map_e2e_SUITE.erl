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
        remote_subscriber_receives_events,
        concurrent_writes_converge,
        survives_owner_restart_via_full_sync,
        map_created_after_cluster_formation_syncs_existing_peer_state,
        presence_map_prunes_departed_node,
        persist_map_survives_full_cluster_restart
    ].

init_per_suite(Config) ->
    BasePort = 24000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
    [{base_port, BasePort} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    SuiteDir = ?config(priv_dir, Config),
    TcDir = filename:join(
        SuiteDir,
        "tc_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    ok = filelib:ensure_dir(filename:join(TcDir, "dummy")),
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

%% A subscriber on one node receives events for writes made on another.
remote_subscriber_receives_events(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    new_map_everywhere(Peers, m),

    {Pa, _} = hd(Peers),
    {Pb, _} = lists:nth(2, Peers),
    ok = peer:call(Pb, ?MODULE, start_map_probe, [m]),

    ok = peer:call(Pa, mycelium, map_put, [m, k, v1]),
    wait_until(fun() -> lists:member({put, k, v1}, events_on(Pb)) end, 15000),
    ok = peer:call(Pa, mycelium, map_remove, [m, k]),
    wait_until(fun() -> lists:member({remove, k}, events_on(Pb)) end, 15000),
    ok.

%% Conflicting writes to the same key on two nodes converge: every node
%% ends on a single last-write-wins value (one of the two written).
concurrent_writes_converge(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    new_map_everywhere(Peers, m),

    {Pa, _} = hd(Peers),
    {Pb, _} = lists:nth(2, Peers),
    ok = peer:call(Pa, mycelium, map_put, [m, k, from_a]),
    ok = peer:call(Pb, mycelium, map_put, [m, k, from_b]),
    wait_until(fun() -> all_agree(Peers, m, k) end, 15000),
    {ok, V} = map_get_on(Pa, m, k),
    ?assert(lists:member(V, [from_a, from_b])),
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
    wait_until(
        fun() ->
            case peer:call(Pc, erlang, whereis, [Owner]) of
                P when is_pid(P), P =/= OldPid -> true;
                _ -> false
            end
        end,
        15000
    ),

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
    wait_until(
        fun() ->
            converged(Hosts, m, k1, v1) andalso
                converged(Hosts, m, k2, v2)
        end,
        15000
    ),

    %% Now bring the map up on C, which already has A and B as peers.
    {Pc, _} = PeerC,
    {ok, _} = peer:call(Pc, mycelium, new_map, [m]),
    wait_until(
        fun() ->
            map_get_on(Pc, m, k1) =:= {ok, v1} andalso
                map_get_on(Pc, m, k2) =:= {ok, v2}
        end,
        15000
    ),
    ok.

%% A presence-style map (`prune_on_peer_down => true') drops a departed
%% node's entries when it leaves. C announces itself, then dies; the
%% survivors prune its presence key on peer_down.
presence_map_prunes_departed_node(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    [
        {ok, _} = peer:call(P, mycelium, new_map, [m, #{prune_on_peer_down => true}])
     || {P, _N} <- Peers
    ],

    {Pc, NodeC} = lists:last(Peers),
    ok = peer:call(Pc, mycelium, map_put, [m, NodeC, up]),
    Survivors = [{P, N} || {P, N} <- Peers, N =/= NodeC],
    wait_until(fun() -> converged(Survivors, m, NodeC, up) end, 15000),

    %% Kill C and force prompt failure detection (nodedown -> peer_down).
    ok = peer:stop(Pc),
    [peer:call(P, erlang, disconnect_node, [NodeC]) || {P, _N} <- Survivors],

    wait_until(fun() -> absent(Survivors, m, NodeC) end, 20000),
    ok.

%% A `persist => true' map reloads its contents after a FULL-cluster restart
%% (every node down, then up with the same data dirs). Each node recovers its
%% own on-disk copy and they re-converge.
persist_map_survives_full_cluster_restart(Config) ->
    Started = [start_peer(S, Config) || S <- ["a", "b", "c"]],
    Peers0 = [{Pid, Node} || {Pid, Node, _Tok, _} <- Started],
    Tokens = [Tok || {_Pid, _Node, Tok, _} <- Started],
    form_cluster(Peers0),
    Nodes0 = [N || {_P, N} <- Peers0],
    wait_until(fun() -> all_members_are(Peers0, Nodes0) end, 20000),

    [
        {ok, _} = peer:call(P, mycelium, new_map, [pm, #{persist => true}])
     || {P, _N} <- Peers0
    ],
    {Pa, _} = hd(Peers0),
    {Pb, _} = lists:nth(2, Peers0),
    ok = peer:call(Pa, mycelium, map_put, [pm, k1, v1]),
    ok = peer:call(Pb, mycelium, map_put, [pm, k2, v2]),
    wait_until(
        fun() ->
            converged(Peers0, pm, k1, v1) andalso
                converged(Peers0, pm, k2, v2)
        end,
        15000
    ),
    %% Let a scan snapshot cycle run on every node (default scan 1000ms), so
    %% each node persists its full converged state - gossiped writes are
    %% appended without fsync and only made durable by the snapshot. This is
    %% the realistic "clean restart of a quiesced cluster".
    timer:sleep(2000),

    %% Full-cluster restart with the same identities + data dirs.
    [ok = peer:stop(Pid) || {Pid, _N} <- Peers0],
    timer:sleep(1000),
    Peers1 = [restart_peer(Tok, Config) || Tok <- Tokens],
    form_cluster(Peers1),
    Nodes1 = [N || {_P, N} <- Peers1],
    wait_until(fun() -> all_members_are(Peers1, Nodes1) end, 20000),
    %% Re-host the map on each node (node-local); recovers from disk.
    [
        {ok, _} = peer:call(P, mycelium, new_map, [pm, #{persist => true}])
     || {P, _N} <- Peers1
    ],

    wait_until(
        fun() ->
            converged(Peers1, pm, k1, v1) andalso
                converged(Peers1, pm, k2, v2)
        end,
        20000
    ),
    ok.

%%====================================================================
%% Map probe (runs on the peer; records map events)
%%====================================================================

start_map_probe(Name) ->
    Self = self(),
    spawn(fun() ->
        ok = mycelium:subscribe_map(Name),
        register(map_probe, self()),
        Self ! probe_ready,
        map_probe_loop([])
    end),
    receive
        probe_ready -> ok
    after 5000 -> error
    end.

map_probe_loop(Events) ->
    receive
        {mycelium_map, _Name, Event} ->
            map_probe_loop([Event | Events]);
        {events, From} ->
            From ! {events, lists:reverse(Events)},
            map_probe_loop(Events);
        stop ->
            ok
    end.

probe_events() ->
    map_probe ! {events, self()},
    receive
        {events, E} -> E
    after 2000 -> []
    end.

%%====================================================================
%% Map helpers
%%====================================================================

new_map_everywhere(Peers, Name) ->
    [{ok, _} = peer:call(P, mycelium, new_map, [Name]) || {P, _N} <- Peers].

map_get_on(Peer, Name, Key) ->
    peer:call(Peer, mycelium, map_get, [Name, Key]).

events_on(Peer) ->
    peer:call(Peer, ?MODULE, probe_events, []).

converged(Peers, Name, Key, Expected) ->
    lists:all(fun({P, _N}) -> map_get_on(P, Name, Key) =:= {ok, Expected} end, Peers).

absent(Peers, Name, Key) ->
    lists:all(fun({P, _N}) -> map_get_on(P, Name, Key) =:= not_found end, Peers).

%% Every node reports the same present value for Key.
all_agree(Peers, Name, Key) ->
    case lists:usort([map_get_on(P, Name, Key) || {P, _N} <- Peers]) of
        [{ok, _}] -> true;
        _ -> false
    end.

%%====================================================================
%% Orchestration
%%====================================================================

start_cluster(Config) ->
    {Pa, NodeA, _, C1} = start_peer("a", Config),
    {Pb, NodeB, _, C2} = start_peer("b", C1),
    {Pc, NodeC, _, _} = start_peer("c", C2),
    Peers = [{Pa, NodeA}, {Pb, NodeB}, {Pc, NodeC}],
    form_cluster(Peers),
    Peers.

form_cluster(Peers) ->
    Pairs = [{Pi, Nj} || {Pi, Ni} <- Peers, {_Pj, Nj} <- Peers, Ni =/= Nj],
    [wait_until(fun() -> connect_ok(Pi, Nj) end, 30000) || {Pi, Nj} <- Pairs],
    [_ = peer:call(Pi, mycelium, join, [Nj]) || {Pi, Nj} <- Pairs],
    wait_until(fun() -> fully_connected(Peers) end, 30000).

connect_ok(P, N) ->
    peer:call(P, net_kernel, connect_node, [N], 15000) =:= true andalso
        lists:member(N, peer:call(P, erlang, nodes, [])).

fully_connected(Peers) ->
    Nodes = [N || {_P, N} <- Peers],
    lists:all(
        fun({P, N}) ->
            AV = peer:call(P, mycelium, active_view, []),
            lists:all(fun(O) -> lists:member(O, AV) end, Nodes -- [N])
        end,
        Peers
    ).

all_members_are(Peers, Expected) ->
    Want = lists:sort(Expected),
    lists:all(
        fun({P, _N}) ->
            lists:sort(peer:call(P, mycelium, members, [])) =:= Want
        end,
        Peers
    ).

%%====================================================================
%% Peer setup (mirrors mycelium_reminder_e2e_SUITE; short lease timings)
%%====================================================================

start_peer(Suffix, Config) ->
    Port = next_port(?config(base_port, Config)),
    Name = list_to_atom(
        "myc_" ++ Suffix ++ "_" ++
            integer_to_list(erlang:unique_integer([positive]))
    ),
    {Pid, Node} = spawn_peer(Suffix, Name, Port, Config),
    %% The 3rd element is a restart token: same Suffix/Name/Port reuses the
    %% node identity and (per-node) data dirs, so a restarted node recovers
    %% its persisted maps from disk.
    {Pid, Node, {Suffix, Name, Port}, Config}.

%% Restart a peer with the SAME identity and data dirs (full-restart tests).
restart_peer({Suffix, Name, Port}, Config) ->
    spawn_peer(Suffix, Name, Port, Config).

spawn_peer(Suffix, Name, Port, Config) ->
    TcDir = ?config(tc_dir, Config),
    NodeDir = filename:join(TcDir, Suffix),
    QuicDir = filename:join(NodeDir, "data/quic"),
    KeysDir = filename:join(NodeDir, "data/keys"),
    RemindersDir = filename:join(NodeDir, "data/reminders"),
    MapsDir = filename:join(NodeDir, "data/maps"),
    [
        ok = filelib:ensure_dir(filename:join(D, "dummy"))
     || D <- [QuicDir, KeysDir, RemindersDir, MapsDir]
    ],
    DiscoveryDir = ?config(discovery_dir, Config),
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
        %% Per-node persistence dirs, so peers do not collide on one dir and
        %% a restart recovers this node's state.
        "-mycelium",
        "reminder_data_dir",
        quote(RemindersDir),
        "-mycelium",
        "mycelium_map_data_dir",
        quote(MapsDir),
        "-mycelium",
        "active_size",
        "5",
        "-mycelium",
        "member_heartbeat_ms",
        "500",
        "-mycelium",
        "member_ttl_ms",
        "2000",
        "-mycelium",
        "member_skew_ms",
        "60000"
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
    put(?MODULE, [
        Pid
        | case get(?MODULE) of
            undefined -> [];
            L -> L
        end
    ]),
    {Pid, Node}.

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
