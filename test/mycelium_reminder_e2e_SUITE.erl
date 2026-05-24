%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E proof for durable reminders. Spawns real BEAM peers under
%%% `-proto_dist mycelium', forms a cluster, and shows the capability
%%% that motivates the feature: a reminder set on a key SURVIVES the
%%% death of the node that owned it, and fires from the survivor that
%%% takes over the partition. Also shows a steady-state reminder fires
%%% exactly once. The peer scaffolding mirrors mycelium_shard_e2e_SUITE.
-module(mycelium_reminder_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        steady_state_fires_exactly_once,
        reminder_fires_from_survivor_after_owner_dies,
        reminder_survives_full_cluster_restart
    ].

init_per_suite(Config) ->
    BasePort = 23000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
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

%% With a stable cluster, a reminder fires on exactly one node (its
%% owner) and exactly once.
steady_state_fires_exactly_once(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    [ok = peer:call(P, ?MODULE, start_probe, []) || {P, _N} <- Peers],

    K = <<"steady-key">>,
    {SetPeer, _} = hd(Peers),
    FireAt = now_on(SetPeer) + 3000,
    ok = peer:call(SetPeer, mycelium, remind, [K, FireAt, ping]),

    wait_until(fun() -> total_fires(Peers, K) >= 1 end, 15000),
    %% Give any erroneous second delivery time to show up.
    timer:sleep(1500),
    ?assertEqual(1, total_fires(Peers, K)),
    ok.

%% The capability proof: kill the key's owner well before the fire time,
%% and a survivor that takes over the partition fires the reminder.
reminder_fires_from_survivor_after_owner_dies(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    [ok = peer:call(P, ?MODULE, start_probe, []) || {P, _N} <- Peers],

    K = <<"survivor-key">>,
    {Q, _} = hd(Peers),
    Owner = peer:call(Q, mycelium, place, [K]),
    {VPeer, Owner} = lists:keyfind(Owner, 2, Peers),
    Survivors = [{P, N} || {P, N} <- Peers, N =/= Owner],
    SurvNodes = [N || {_P, N} <- Survivors],

    %% Set the reminder from a survivor, fire time far enough out that
    %% the owner is long gone (lease ttl is 2s) before it fires.
    {SetPeer, _} = hd(Survivors),
    FireAt = now_on(SetPeer) + 8000,
    ok = peer:call(SetPeer, mycelium, remind, [K, FireAt, wakeup]),
    %% Let the add gossip to every survivor before the owner dies.
    timer:sleep(1500),

    ok = peer:stop(VPeer),
    %% Lease expiry (not peer_down) removes the dead owner from the ring.
    wait_until(fun() -> all_members_are(Survivors, SurvNodes) end, 20000),

    %% Exactly one survivor (the new owner) fires it.
    wait_until(fun() -> total_fires(Survivors, K) >= 1 end, 15000),
    timer:sleep(1500),
    ?assertEqual(1, total_fires(Survivors, K)),
    %% And the fire landed on the node placement now points at.
    NewOwner = peer:call(SetPeer, mycelium, place, [K]),
    ?assert(lists:member(NewOwner, SurvNodes)),
    ?assertEqual([NewOwner], firing_nodes(Survivors, K)),
    ok.

%% The persistence proof: a reminder set before a FULL-cluster restart
%% (every node down, then up with the same data dirs) is recovered from
%% disk and still fires. Nothing to re-sync from peers - all were down.
reminder_survives_full_cluster_restart(Config) ->
    Started = [start_peer(S, Config) || S <- ["a", "b", "c"]],
    Peers0 = [{Pid, Node} || {Pid, Node, _Tok, _} <- Started],
    Tokens = [Tok || {_Pid, _Node, Tok, _} <- Started],
    form_cluster(Peers0),
    Nodes0 = [N || {_P, N} <- Peers0],
    wait_until(fun() -> all_members_are(Peers0, Nodes0) end, 20000),

    %% Set a reminder far enough out that it is still pending when the
    %% cluster goes down and only comes due after it is back and converged.
    K = <<"persist-key">>,
    {SetPeer, _} = hd(Peers0),
    FireAt = now_on(SetPeer) + 20000,
    ok = peer:call(SetPeer, mycelium, remind, [K, FireAt, wakeup]),
    %% remind/3 fsyncs on the set node before returning; let it gossip too.
    timer:sleep(1000),

    %% Full-cluster restart: stop every node, then bring them back with the
    %% same identities + data dirs.
    [ok = peer:stop(Pid) || {Pid, _N} <- Peers0],
    timer:sleep(1000),
    Peers1 = [restart_peer(Tok, Config) || Tok <- Tokens],
    %% Probe every node before the ring reforms, so no fire is missed.
    [ok = peer:call(Pid, ?MODULE, start_probe, []) || {Pid, _N} <- Peers1],
    form_cluster(Peers1),
    Nodes1 = [N || {_P, N} <- Peers1],
    wait_until(fun() -> all_members_are(Peers1, Nodes1) end, 20000),

    %% The reminder, recovered from disk, still fires after the restart.
    wait_until(fun() -> total_fires(Peers1, K) >= 1 end, 30000),
    ?assert(total_fires(Peers1, K) >= 1),
    ok.

%%====================================================================
%% Probe (runs on the peer; records reminder fires)
%%====================================================================

start_probe() ->
    Self = self(),
    spawn(fun() ->
        mycelium:subscribe_reminders(),
        register(reminder_probe, self()),
        Self ! probe_ready,
        probe_loop([])
    end),
    receive
        probe_ready -> ok
    after 5000 -> error
    end.

probe_loop(Fires) ->
    receive
        {mycelium_reminder, Key, Payload, Fence} ->
            probe_loop([{Key, Payload, Fence} | Fires]);
        {fires, From} ->
            From ! {fires, Fires},
            probe_loop(Fires);
        stop ->
            ok
    end.

probe_fires() ->
    reminder_probe ! {fires, self()},
    receive
        {fires, F} -> F
    after 2000 -> []
    end.

%%====================================================================
%% Orchestration
%%====================================================================

%% Total fires of Key across the given peers.
total_fires(Peers, Key) ->
    lists:sum([
        length([
            1
         || {K, _P, _F} <- peer:call(P, ?MODULE, probe_fires, []),
            K =:= Key
        ])
     || {P, _N} <- Peers
    ]).

%% Nodes that recorded a fire of Key.
firing_nodes(Peers, Key) ->
    lists:usort(
        [
            N
         || {P, N} <- Peers,
            lists:any(
                fun({K, _Pl, _F}) -> K =:= Key end,
                peer:call(P, ?MODULE, probe_fires, [])
            )
        ]
    ).

now_on(Peer) ->
    peer:call(Peer, erlang, system_time, [millisecond]).

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
%% Peer setup (mirrors mycelium_shard_e2e_SUITE; short lease timings)
%%====================================================================

start_peer(Suffix, Config) ->
    Port = next_port(?config(base_port, Config)),
    Name = list_to_atom(
        "myc_" ++ Suffix ++ "_" ++
            integer_to_list(erlang:unique_integer([positive]))
    ),
    {Pid, Node} = spawn_peer(Suffix, Name, Port, Config),
    %% The 3rd element is a restart token: the same Suffix/Name/Port reuses
    %% the node identity and (per-node) data dirs, so a restarted node
    %% recovers its persisted reminders from disk.
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
    ok = filelib:ensure_dir(filename:join(QuicDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(KeysDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(RemindersDir, "dummy")),
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
        %% Per-node reminder store, so peers do not collide on one dir and a
        %% restart recovers this node's reminders.
        "-mycelium",
        "reminder_data_dir",
        quote(RemindersDir),
        "-mycelium",
        "active_size",
        "5",
        %% Short leases so a dead node leaves the ring quickly, and a
        %% brisk scan so the survivor re-arms promptly.
        "-mycelium",
        "member_heartbeat_ms",
        "500",
        "-mycelium",
        "member_ttl_ms",
        "2000",
        "-mycelium",
        "member_skew_ms",
        "60000",
        "-mycelium",
        "reminder_scan_ms",
        "500"
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
