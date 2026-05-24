%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E proof for cluster-wide leader election. Spawns real BEAM peers
%%% under `-proto_dist mycelium', forms a fully-connected cluster, and
%%% exercises election, peer_down re-election, and fencing across a live
%%% node death. The peer scaffolding mirrors mycelium_proto_dist_SUITE.
%%%
%%% Candidates must be long-lived (mycelium monitors the campaigning
%%% process), so each node runs a persistent `leader_probe' process via
%%% start_candidate/2; candidate_status/0 reads its current role + fence.
-module(mycelium_leader_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        three_node_single_leader,
        leader_failover_increments_fence,
        priority_overrides_node_atom
    ].

init_per_suite(Config) ->
    %% Stay clear of the proto_dist (19100-19899) and audit (20000+)
    %% port bands.
    BasePort = 21000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
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

%% Three nodes campaign for the same singleton. Exactly one is elected,
%% it is the lowest node atom, and every node agrees on the leader.
three_node_single_leader(Config) ->
    Peers = start_cluster(Config),
    [campaign(P, job, 0) || {P, _N} <- Peers],
    Expected = lists:min([N || {_P, N} <- Peers]),
    wait_until(fun() -> agree_leader(Peers, job, Expected) end, 20000),

    {leader, _F} = status_of(Peers, Expected),
    [
        ?assertEqual({follower, undefined}, status_of(Peers, N))
     || {_P, N} <- Peers, N =/= Expected
    ],
    ok.

%% The core proof: kill the leader, a survivor takes over, and the new
%% term's fencing token is strictly greater than the old one.
leader_failover_increments_fence(Config) ->
    Peers = start_cluster(Config),
    [campaign(P, job, 0) || {P, _N} <- Peers],
    Expected = lists:min([N || {_P, N} <- Peers]),
    wait_until(fun() -> agree_leader(Peers, job, Expected) end, 20000),

    {leader, F1} = status_of(Peers, Expected),
    Survivors = [{P, N} || {P, N} <- Peers, N =/= Expected],

    %% Deterministic, not timing-based: every survivor must have learned
    %% the leader's term token before we kill it, so the next mint is
    %% guaranteed to advance past it.
    wait_until(
        fun() ->
            lists:all(
                fun({P, _N}) ->
                    case peer:call(P, mycelium_leader, high_water, [job]) of
                        HW when is_integer(HW) -> HW >= F1;
                        _ -> false
                    end
                end,
                Survivors
            )
        end,
        20000
    ),

    {LeaderPeer, Expected} = lists:keyfind(Expected, 2, Peers),
    ok = peer:stop(LeaderPeer),
    %% Force prompt failure detection: an abrupt QUIC peer death is not
    %% noticed until an idle timeout, so tear down each survivor's dist
    %% link to the dead node. This drives the real nodedown -> peer_down
    %% -> remove_node path under test, just without the idle wait.
    [peer:call(P, erlang, disconnect_node, [Expected]) || {P, _N} <- Survivors],

    NewExpected = lists:min([N || {_P, N} <- Survivors]),
    wait_until(
        fun() ->
            case status_of(Survivors, NewExpected) of
                {leader, F2} when is_integer(F2) -> F2 > F1;
                _ -> false
            end
        end,
        20000
    ),

    {leader, F2} = status_of(Survivors, NewExpected),
    ?assert(F2 > F1),

    NewPid = peer_whereis(Survivors, NewExpected),
    wait_until(
        fun() ->
            lists:all(
                fun({P, _N}) ->
                    peer:call(P, mycelium, leader, [job]) =:= {ok, NewExpected, NewPid}
                end,
                Survivors
            )
        end,
        20000
    ),
    ok.

%% A higher priority wins over a lower node atom.
priority_overrides_node_atom(Config) ->
    Peers = start_cluster(Config),
    HighAtom = lists:max([N || {_P, N} <- Peers]),
    [campaign(P, job, prio_for(N, HighAtom)) || {P, N} <- Peers],
    wait_until(fun() -> agree_leader(Peers, job, HighAtom) end, 20000),
    {leader, _F} = status_of(Peers, HighAtom),
    ok.

prio_for(Node, Node) -> 1;
prio_for(_Node, _High) -> 0.

%%====================================================================
%% Candidate helpers (run on the peer via peer:call)
%%====================================================================

%% Spawn a persistent candidate that campaigns for Name and tracks its
%% role + last fence. Registered as `leader_probe' so the orchestrator
%% can query it later. Returns once the initial role is known.
start_candidate(Name, Priority) ->
    Self = self(),
    spawn(fun() ->
        {Role, Fence} =
            case mycelium:lead(Name, #{priority => Priority}) of
                {ok, {leader, F}} -> {leader, F};
                {ok, follower} -> {follower, undefined};
                Other -> {{error, Other}, undefined}
            end,
        register(leader_probe, self()),
        Self ! {candidate_started, Role, Fence},
        candidate_loop(Name, Role, Fence)
    end),
    receive
        {candidate_started, Role, Fence} -> {ok, Role, Fence}
    after 10000 ->
        {error, timeout}
    end.

candidate_loop(Name, Role, Fence) ->
    receive
        {mycelium_leader, Name, {elected, F}} ->
            candidate_loop(Name, leader, F);
        {mycelium_leader, Name, revoked} ->
            candidate_loop(Name, follower, undefined);
        {status, From} ->
            From ! {status, Role, Fence},
            candidate_loop(Name, Role, Fence);
        stop ->
            ok
    end.

candidate_status() ->
    case whereis(leader_probe) of
        undefined ->
            {error, no_candidate};
        P ->
            P ! {status, self()},
            receive
                {status, Role, Fence} -> {Role, Fence}
            after 2000 ->
                {error, status_timeout}
            end
    end.

%%====================================================================
%% Orchestration helpers
%%====================================================================

start_cluster(Config) ->
    {Pa, NodeA, _, C1} = start_peer("a", Config, #{}),
    {Pb, NodeB, _, C2} = start_peer("b", C1, #{}),
    {Pc, NodeC, _, _} = start_peer("c", C2, #{}),
    Peers = [{Pa, NodeA}, {Pb, NodeB}, {Pc, NodeC}],
    form_cluster(Peers),
    Peers.

%% Every node joins every other node (full mesh), then wait until each
%% node's active view holds the other two so peer_down will fire on a
%% death.
form_cluster(Peers) ->
    Pairs = [{Pi, Nj} || {Pi, Ni} <- Peers, {_Pj, Nj} <- Peers, Ni =/= Nj],
    %% Establish the dist mesh first, with retries: the first
    %% connect_node between two fresh nodes can lose the race with the
    %% QUIC + auth handshake and return false.
    [wait_until(fun() -> connect_ok(Pi, Nj) end, 30000) || {Pi, Nj} <- Pairs],
    %% Form the gossip overlay: with dist already up, join takes the
    %% bridge's "already connected" fast path and populates the active
    %% view (and fires peer_up) on both ends.
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

campaign(Peer, Name, Priority) ->
    {ok, _Role, _Fence} = peer:call(Peer, ?MODULE, start_candidate, [Name, Priority]),
    ok.

%% Read the candidate role + fence on the node named Node.
status_of(Peers, Node) ->
    {Peer, Node} = lists:keyfind(Node, 2, Peers),
    peer:call(Peer, ?MODULE, candidate_status, []).

peer_whereis(Peers, Node) ->
    {Peer, Node} = lists:keyfind(Node, 2, Peers),
    peer:call(Peer, erlang, whereis, [leader_probe]).

%% Every node reports the same leader: {ok, ExpectedNode, ExpectedPid}.
agree_leader(Peers, Name, ExpectedNode) ->
    EPid = peer_whereis(Peers, ExpectedNode),
    is_pid(EPid) andalso
        lists:all(
            fun({P, _N}) ->
                peer:call(P, mycelium, leader, [Name]) =:= {ok, ExpectedNode, EPid}
            end,
            Peers
        ).

%%====================================================================
%% Peer setup (mirrors mycelium_proto_dist_SUITE:start_peer/3)
%%====================================================================

start_peer(Suffix, Config, _Opts) ->
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
        "5"
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
