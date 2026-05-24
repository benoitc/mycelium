%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E proof for sharded placement. Spawns real BEAM peers under
%%% `-proto_dist mycelium', forms a cluster, and shows that every node
%%% agrees on `place(Key)' and that a node death reassigns its partitions
%%% to survivors (with `{acquired, P}' events) once its lease expires.
%%% The peer scaffolding mirrors mycelium_leader_e2e_SUITE.
-module(mycelium_shard_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        three_node_converge_and_agree,
        node_death_reassigns_partitions
    ].

init_per_suite(Config) ->
    BasePort = 22000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
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

three_node_converge_and_agree(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    %% Every node resolves each key to the same owner.
    [
        ?assertEqual(
            1,
            length(
                lists:usort(
                    [peer:call(P, mycelium, place, [K]) || {P, _N} <- Peers]
                )
            )
        )
     || K <- [k1, k2, k3, {a, b}, "x"]
    ],
    ok.

node_death_reassigns_partitions(Config) ->
    Peers = start_cluster(Config),
    Nodes = [N || {_P, N} <- Peers],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 20000),
    [ok = peer:call(P, ?MODULE, start_probe, []) || {P, _N} <- Peers],

    Victim = lists:min(Nodes),
    {VPeer, Victim} = lists:keyfind(Victim, 2, Peers),
    Survivors = [{P, N} || {P, N} <- Peers, N =/= Victim],
    SurvNodes = [N || {_P, N} <- Survivors],
    ok = peer:stop(VPeer),

    %% Lease expiry (not peer_down) removes the dead node from the ring.
    wait_until(fun() -> all_members_are(Survivors, SurvNodes) end, 20000),

    %% A survivor must have acquired the victim's partitions.
    wait_until(
        fun() ->
            lists:any(
                fun({P, _N}) ->
                    {Acquired, _Released} = peer:call(P, ?MODULE, probe_events, []),
                    Acquired =/= []
                end,
                Survivors
            )
        end,
        10000
    ),

    %% Survivors agree on placement and never point at the dead node.
    [
        begin
            Places = [peer:call(P, mycelium, place, [K]) || {P, _N} <- Survivors],
            ?assertEqual(1, length(lists:usort(Places))),
            ?assert(lists:member(hd(Places), SurvNodes))
        end
     || K <- [k1, k2, k3, k4, k5]
    ],
    ok.

%%====================================================================
%% Probe (runs on the peer; records ownership events)
%%====================================================================

start_probe() ->
    Self = self(),
    spawn(fun() ->
        mycelium:subscribe_shard(),
        register(shard_probe, self()),
        Self ! probe_ready,
        probe_loop([], [])
    end),
    receive
        probe_ready -> ok
    after 5000 -> error
    end.

probe_loop(Acq, Rel) ->
    receive
        {mycelium_shard, {acquired, P}} ->
            probe_loop([P | Acq], Rel);
        {mycelium_shard, {released, P}} ->
            probe_loop(Acq, [P | Rel]);
        {events, From} ->
            From ! {events, lists:usort(Acq), lists:usort(Rel)},
            probe_loop(Acq, Rel);
        stop ->
            ok
    end.

probe_events() ->
    shard_probe ! {events, self()},
    receive
        {events, A, R} -> {A, R}
    after 2000 -> {error, timeout}
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
%% Peer setup (mirrors mycelium_leader_e2e_SUITE; short lease timings)
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
    Name = list_to_atom(
        "myc_" ++ Suffix ++ "_" ++
            integer_to_list(erlang:unique_integer([positive]))
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
        "active_size",
        "5",
        %% Short leases so a dead node leaves the ring quickly.
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
    {Pid, Node, NodeDir, Config}.

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
