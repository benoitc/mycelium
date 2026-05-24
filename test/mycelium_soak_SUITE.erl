%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Soak / chaos suite for mycelium.
%%%
%%% Gated by `MYCELIUM_CT_SOAK=1' so casual CI runs skip it.
%%%
%%% Active case:
%%%
%%%   broadcast_burst/1 — 5-node stable cluster, drive a burst of
%%%     plumtree broadcasts from the seed and assert every peer's
%%%     subscriber receives a marker. Validates that plumtree's
%%%     eager_peers list is correctly populated through HyParView's
%%%     JOIN handler (which used to silently drop the peer_up event,
%%%     fixed in mycelium_hyparview).
%%%
%%% Scaffolding kept below for future work:
%%%
%%%   partition_and_heal/1, cross_active_view_bang/1 — these surface
%%%     real timing gaps in HyParView's passive-promotion and dist
%%%     teardown paths. Wiring them up cleanly is its own effort.

-module(mycelium_soak_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT scaffolding
%%====================================================================

suite() ->
    [{timetrap, {minutes, 10}}].

all() ->
    case os:getenv("MYCELIUM_CT_SOAK") of
        false ->
            {skip, "set MYCELIUM_CT_SOAK=1 to run soak suite"};
        "" ->
            {skip, "MYCELIUM_CT_SOAK is empty"};
        _ ->
            %% cross_active_view_bang remains future-work scaffolding.
            [broadcast_burst, partition_and_heal]
    end.

init_per_suite(Config) ->
    BasePort = 19500 + erlang:phash2({?MODULE, erlang:system_time()}, 400) * 10,
    [{base_port, BasePort} | Config].

end_per_suite(_) ->
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
    %% Fresh port band per testcase so tear-down latency on one case
    %% does not poison the listener bind on the next one.
    BasePort = ?config(base_port, Config),
    Offset = erlang:phash2(TcDir, 200) * 20,
    TcBasePort = BasePort + Offset,
    [
        {tc_dir, TcDir},
        {discovery_dir, DiscoveryDir},
        {tc_base_port, TcBasePort}
        | Config
    ].

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

broadcast_burst(Config) ->
    %% active_size = NumPeers - 1 so every peer's active view holds
    %% every other. Plumtree converges through a full mesh once the
    %% gossip protocol stabilises.
    Peers = [
        start_peer(
            "c" ++ integer_to_list(I),
            Config,
            #{active_size => 4, auth_enabled => false}
        )
     || I <- lists:seq(1, 5)
    ],
    [PA | _] = Peers,
    {PA1, NodeA, _, _} = PA,
    %% Pair every non-seed peer to NodeA so a connected component forms.
    lists:foreach(
        fun
            ({PX, _NodeX, _, _}) when PX =/= PA1 ->
                ok = peer:call(PX, mycelium, join, [NodeA]);
            (_) ->
                ok
        end,
        Peers
    ),
    %% Wait until every peer has the seed visible in its active view.
    wait_until(
        fun() ->
            lists:all(
                fun
                    ({PX, _, _, _}) when PX =/= PA1 ->
                        AV = peer:call(PX, mycelium, active_view, []),
                        lists:member(NodeA, AV);
                    (_) ->
                        true
                end,
                Peers
            )
        end,
        10000
    ),
    NonSeedNodes = [N || {_, N, _, _} <- Peers, N =/= NodeA],
    %% Sustained gossip load: many tick broadcasts back-to-back. No
    %% leave/rejoin churn here; HyParView's passive-promotion path
    %% after a graceful leave has known timing issues that belong in
    %% a separate stabilisation effort.
    lists:foreach(
        fun(I) ->
            peer:call(
                PA1,
                mycelium_plumtree,
                broadcast,
                [tick, {tick, I}]
            ),
            timer:sleep(50)
        end,
        lists:seq(1, 40)
    ),
    %% Settle.
    wait_until(
        fun() ->
            SeedAV = peer:call(PA1, mycelium, active_view, []),
            lists:all(
                fun(N) -> lists:member(N, SeedAV) end,
                NonSeedNodes
            )
        end,
        15000
    ),
    timer:sleep(1500),
    %% Every peer subscribes a collector to plumtree.
    Survivors = [P || {P, _, _, _} <- Peers],
    [ok = peer:call(P, ?MODULE, start_collector, []) || P <- Survivors],
    %% Broadcast a marker from the seed via plumtree.
    Marker = {soak_marker, erlang:unique_integer([positive])},
    ok = peer:call(PA1, mycelium_plumtree, broadcast, [soak, Marker]),
    try
        wait_until(
            fun() ->
                lists:all(
                    fun(P) ->
                        peer:call(P, ?MODULE, was_seen, [Marker]) =:= true
                    end,
                    Survivors
                )
            end,
            15000
        )
    catch
        _:_ ->
            Seen = [
                {P, peer:call(P, ?MODULE, was_seen, [Marker])}
             || P <- Survivors
            ],
            Stats = [peer:call(P, mycelium_plumtree, get_stats, []) || P <- Survivors],
            ct:pal("seen=~p~nstats=~p", [Seen, Stats]),
            ?assert(false, "plumtree marker did not converge")
    end.

partition_and_heal(Config) ->
    %% active_size 4 (>= the 3 other nodes) so the view never fills and
    %% HyParView never evicts an intra-pair link during the merge churn; the
    %% two halves then stay connected pairs across the cut.
    [PA, PB, PC, PD] = [
        start_peer(
            "p" ++ integer_to_list(I),
            Config,
            #{active_size => 4}
        )
     || I <- lists:seq(1, 4)
    ],
    {Pa, NodeA, _, _} = PA,
    {Pb, NodeB, _, _} = PB,
    {Pc, NodeC, _, _} = PC,
    {Pd, NodeD, _, _} = PD,
    %% Build two connected pairs {A-B} and {C-D}, then merge into one
    %% cluster (C->A). A star (all join A) would leave B,C,D linked only to
    %% A, so cutting A would split into singletons rather than two pairs.
    ok = peer:call(Pb, mycelium, join, [NodeA]),
    ok = peer:call(Pd, mycelium, join, [NodeC]),
    wait_until(fun() -> in_view(Pa, [NodeB]) andalso in_view(Pc, [NodeD]) end, 15000),
    ok = peer:call(Pc, mycelium, join, [NodeA]),
    wait_until(fun() -> in_view(Pa, [NodeC]) orelse in_view(Pc, [NodeA]) end, 15000),

    %% Hold a real partition {A,B} | {C,D}: block across the cut so HyParView
    %% will not re-dial, then drop the dist links. (disconnect_node alone
    %% self-heals via passive promotion.)
    ok = peer:call(Pa, mycelium_hyparview, block_peers, [[NodeC, NodeD]]),
    ok = peer:call(Pb, mycelium_hyparview, block_peers, [[NodeC, NodeD]]),
    ok = peer:call(Pc, mycelium_hyparview, block_peers, [[NodeA, NodeB]]),
    ok = peer:call(Pd, mycelium_hyparview, block_peers, [[NodeA, NodeB]]),
    Disconnect = fun(P, Targets) ->
        [peer:call(P, erlang, disconnect_node, [N], 15000) || N <- Targets]
    end,
    Disconnect(Pa, [NodeC, NodeD]),
    Disconnect(Pb, [NodeC, NodeD]),
    Disconnect(Pc, [NodeA, NodeB]),
    Disconnect(Pd, [NodeA, NodeB]),
    %% The cut is in effect once neither half sees the other in its active
    %% view (immediate + stable: blocked nodes cannot re-enter). We assert
    %% the cut via active_view/0, not the lease-based members/0 (which lags
    %% by member_ttl_ms). Intra-pair connectivity is proven end-to-end by
    %% the within-side service propagation below (a transient drop simply
    %% re-promotes from passive, since the pair is not blocked).
    wait_until(
        fun() ->
            not_in_view(Pa, [NodeC, NodeD]) andalso
                not_in_view(Pc, [NodeA, NodeB])
        end,
        15000
    ),

    %% Each side registers its own service; it propagates within the side.
    ok = peer:call(Pa, ?MODULE, register_holder, [svc_left]),
    ok = peer:call(Pc, ?MODULE, register_holder, [svc_right]),
    wait_until(
        fun() -> visible(Pb, svc_left) andalso visible(Pd, svc_right) end,
        15000
    ),
    %% Hold the partition: neither service may cross the cut.
    timer:sleep(1000),
    ?assert(
        not visible(Pc, svc_left) andalso not visible(Pd, svc_left),
        "svc_left leaked across the partition to the right side"
    ),
    ?assert(
        not visible(Pa, svc_right) andalso not visible(Pb, svc_right),
        "svc_right leaked across the partition to the left side"
    ),

    %% Heal: clear the blocks, then re-join ACROSS the former cut so every
    %% node gets a fresh peer_up with a service holder. The registry
    %% full-syncs on peer_up; the surviving intra-pair links (A-B, C-D) get
    %% no new peer_up, and a service learned via full-sync is not
    %% re-broadcast, so each inner node needs its own cross-cut join:
    %%   C<->A : A learns svc_right, C learns svc_left
    %%   B<->C : B learns both from C
    %%   D<->A : D learns both from A
    [ok = peer:call(P, mycelium_hyparview, unblock_peers, []) || P <- [Pa, Pb, Pc, Pd]],
    ok = peer:call(Pc, mycelium, join, [NodeA]),
    ok = peer:call(Pb, mycelium, join, [NodeC]),
    ok = peer:call(Pd, mycelium, join, [NodeA]),
    wait_until(
        fun() ->
            lists:all(
                fun(P) -> visible(P, svc_left) andalso visible(P, svc_right) end,
                [Pa, Pb, Pc, Pd]
            )
        end,
        20000
    ).

%% A service is visible from P when its lookup returns at least one entry.
visible(P, Svc) ->
    case peer:call(P, mycelium, lookup, [Svc]) of
        {ok, [_ | _]} -> true;
        _ -> false
    end.

%% Every node in Nodes is in P's HyParView active view.
in_view(P, Nodes) ->
    AV = peer:call(P, mycelium, active_view, []),
    lists:all(fun(N) -> lists:member(N, AV) end, Nodes).

%% No node in Nodes is in P's HyParView active view.
not_in_view(P, Nodes) ->
    AV = peer:call(P, mycelium, active_view, []),
    lists:all(fun(N) -> not lists:member(N, AV) end, Nodes).

cross_active_view_bang(Config) ->
    N = 3,
    Peers = [
        start_peer(
            "x" ++ integer_to_list(I),
            Config,
            #{active_size => 1}
        )
     || I <- lists:seq(1, N)
    ],
    [PA | _] = Peers,
    {Pa1, NodeA, _, _} = PA,
    %% Every other peer joins via seed A.
    lists:foreach(
        fun
            ({P, _, _, _}) when P =/= Pa1 ->
                ok = peer:call(P, mycelium, join, [NodeA]);
            (_) ->
                ok
        end,
        Peers
    ),
    timer:sleep(2000),
    %% Every peer registers an echo.
    lists:foreach(
        fun({P, _Node, _, _}) ->
            ok = peer:call(P, ?MODULE, start_echo, [])
        end,
        Peers
    ),
    timer:sleep(500),
    %% Pre-warm dist channels across every pair. First-attempt connect
    %% can race the QUIC handshake, so wrap in wait_until with
    %% retries — same shape the proto_dist gc case uses.
    Pids = [P || {P, _, _, _} <- Peers],
    Nodes = [Node || {_, Node, _, _} <- Peers],
    [
        begin
            ct:pal("connect ~p -> ~p", [FromNode, To]),
            wait_until(
                fun() ->
                    true =:=
                        peer:call(
                            From,
                            net_kernel,
                            connect_node,
                            [To],
                            15000
                        ) andalso
                        lists:member(
                            To,
                            peer:call(From, erlang, nodes, [])
                        )
                end,
                30000
            ),
            timer:sleep(50)
        end
     || {From, FromNode} <- lists:zip(Pids, Nodes),
        To <- Nodes,
        To =/= FromNode
    ],
    timer:sleep(500),
    %% Cross-product: from each Pi send to every Pj's echo via raw bang.
    Results =
        [
            {From, To, peer:call(From, ?MODULE, echo_to, [To, payload], 15000)}
         || {From, FromNode} <- lists:zip(Pids, Nodes),
            To <- Nodes,
            To =/= FromNode
        ],
    Bad = [
        {From, To, R}
     || {From, To, R} <- Results,
        not is_ok_echo(R)
    ],
    ?assertEqual(
        [],
        Bad,
        io_lib:format("failed echoes: ~p", [Bad])
    ).

is_ok_echo({ok, payload, _Node}) -> true;
is_ok_echo(_) -> false.

%%====================================================================
%% Helpers run on peers via peer:call
%%====================================================================

%% Spawn a never-exiting holder process, register it under Name, and
%% advertise it through the mycelium service registry.
register_holder(Name) ->
    case whereis(soak_holder_key(Name)) of
        undefined ->
            Parent = self(),
            %% The HOLDER registers the service (register_service registers the
            %% CALLER), so the registration outlives this peer:call worker.
            Pid = spawn(fun() ->
                ok = mycelium:register_service(Name),
                Parent ! {holder_registered, Name},
                receive
                    _ -> ok
                end
            end),
            register(soak_holder_key(Name), Pid),
            receive
                {holder_registered, Name} -> ok
            after 5000 -> error(holder_register_timeout)
            end;
        _ ->
            ok
    end,
    ok.

soak_holder_key(Name) when is_atom(Name) ->
    list_to_atom("soak_holder_" ++ atom_to_list(Name)).

%% Subscribe a per-VM agent to plumtree and remember which payloads
%% have arrived. was_seen/1 reports membership.
start_collector() ->
    case whereis(soak_collector) of
        undefined ->
            Pid = spawn(fun() -> collector_loop(#{}) end),
            register(soak_collector, Pid),
            mycelium_plumtree:subscribe(Pid);
        _ ->
            ok
    end,
    ok.

collector_loop(Seen) ->
    receive
        {From, was_seen, Payload} ->
            From ! {Payload, maps:is_key(Payload, Seen)},
            collector_loop(Seen);
        {plumtree_broadcast, {_Tag, Payload}} ->
            collector_loop(Seen#{Payload => true});
        _Other ->
            collector_loop(Seen)
    end.

was_seen(Payload) ->
    Pid = whereis(soak_collector),
    Pid ! {self(), was_seen, Payload},
    receive
        {Payload, B} -> B
    after 1000 ->
        false
    end.

start_echo() ->
    case whereis(echo) of
        undefined ->
            Pid = spawn(fun loop_echo/0),
            register(echo, Pid);
        _ ->
            ok
    end,
    ok.

loop_echo() ->
    receive
        {From, Msg} ->
            From ! {echoed, Msg, node()},
            loop_echo()
    end.

echo_to(TargetNode, Msg) ->
    case rpc:call(TargetNode, erlang, whereis, [echo]) of
        Pid when is_pid(Pid) ->
            Pid ! {self(), Msg},
            receive
                {echoed, M, N} -> {ok, M, N}
            after 4000 ->
                timeout
            end;
        _ ->
            {error, no_echo}
    end.

%%====================================================================
%% Peer setup (shared shape with mycelium_proto_dist_SUITE)
%%====================================================================

start_peer(Suffix, Config, Opts) ->
    TcDir = ?config(tc_dir, Config),
    NodeDir = filename:join(TcDir, Suffix),
    ok = filelib:ensure_dir(filename:join(NodeDir, "dummy")),
    QuicDir = filename:join(NodeDir, "data/quic"),
    KeysDir = filename:join(NodeDir, "data/keys"),
    ok = filelib:ensure_dir(filename:join(QuicDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(KeysDir, "dummy")),
    DiscoveryDir = ?config(discovery_dir, Config),
    BasePort = ?config(tc_base_port, Config),
    Port = next_port(BasePort),
    ActiveSize = maps:get(active_size, Opts, 5),
    Name = list_to_atom(
        "soak_" ++ Suffix ++ "_" ++
            integer_to_list(erlang:unique_integer([positive]))
    ),
    AuthArgs =
        case maps:get(auth_enabled, Opts, undefined) of
            false -> ["-mycelium", "auth_enabled", "false"];
            true -> ["-mycelium", "auth_enabled", "true"];
            _ -> []
        end,
    BaseArgs =
        [
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
            "mycelium_soak",
            "-mycelium",
            "auth_key_dir",
            quote(KeysDir),
            "-mycelium",
            "discovery_dir",
            quote(DiscoveryDir),
            "-mycelium",
            "active_size",
            integer_to_list(ActiveSize),
            %% Short lease timings so the shard's members/0 set (and any
            %% membership-driven convergence) reacts quickly. Split assertions
            %% use active_view/0 + registry, which are immediate regardless.
            "-mycelium",
            "member_heartbeat_ms",
            "500",
            "-mycelium",
            "member_ttl_ms",
            "2000",
            "-mycelium",
            "member_skew_ms",
            "60000"
        ] ++ AuthArgs,
    PaArgs = lists:flatmap(fun(P) -> ["-pa", P] end, code:get_path()),
    Args = PaArgs ++ BaseArgs,
    {ok, Pid, Node} = peer:start(#{
        name => Name,
        longnames => true,
        host => "127.0.0.1",
        connection => standard_io,
        args => Args
    }),
    {ok, _} = peer:call(Pid, application, ensure_all_started, [mycelium]),
    put(?MODULE, [
        Pid
        | case get(?MODULE) of
            undefined -> [];
            L -> L
        end
    ]),
    {Pid, Node, NodeDir, Config}.

quote(S) -> "\"" ++ S ++ "\"".

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
    case Fun() of
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
