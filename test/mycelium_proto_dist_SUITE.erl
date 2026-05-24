%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E coverage for booting under `-proto_dist mycelium'.
%%%
%%% Uses the `peer' module (OTP 25+) with `connection => standard_io'
%%% so the orchestrator drives slaves over stdio, leaving the slaves
%%% free to talk between themselves over -proto_dist mycelium.

-module(mycelium_proto_dist_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

%%====================================================================
%% CT scaffolding
%%====================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        lazy_cert_generation,
        single_node_boot,
        mycelium_dist_port_alias,
        two_node_connect,
        pid_send_outside_active_view,
        active_eviction_keeps_dist,
        gc_skips_live_streams
    ].

init_per_suite(Config) ->
    BasePort = 19100 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
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

lazy_cert_generation(Config) ->
    {_Pid, _Node, NodeDir, _Config2} = start_peer("cert", Config, #{}),
    CertFile = filename:join([NodeDir, "data", "quic", "node.crt"]),
    KeyFile  = filename:join([NodeDir, "data", "quic", "node.key"]),
    ?assert(filelib:is_regular(CertFile),
            "cert file did not appear at " ++ CertFile),
    ?assert(filelib:is_regular(KeyFile)).

single_node_boot(Config) ->
    {Pid, _Node, _Dir, _} = start_peer("boot", Config, #{}),
    {ok, Dist} = peer:call(Pid, application, get_env, [quic, dist]),
    ?assertEqual({mycelium_dist_auth_callback, authenticate},
                 proplists:get_value(auth_callback, Dist)),
    ?assertEqual(mycelium_discovery,
                 proplists:get_value(discovery_module, Dist)),
    ?assertNotEqual(undefined, proplists:get_value(cert_file, Dist)),
    ?assertNotEqual(undefined, proplists:get_value(key_file, Dist)).

mycelium_dist_port_alias(Config) ->
    BasePort = ?config(base_port, Config),
    Port = BasePort + 5,
    {Pid, _Node, _Dir, _} = start_peer("portalias", Config, #{port => Port}),
    ?assertEqual({ok, Port}, peer:call(Pid, application, get_env,
                                       [quic, dist_port])).

two_node_connect(Config) ->
    {Pa, NodeA, _, Config1} = start_peer("a", Config, #{}),
    {Pb, NodeB, _, _Config2} = start_peer("b", Config1, #{}),
    true = peer:call(Pa, net_kernel, connect_node, [NodeB]),
    wait_until(fun() ->
        lists:member(NodeB, peer:call(Pa, erlang, nodes, [])) andalso
        lists:member(NodeA, peer:call(Pb, erlang, nodes, []))
    end, 5000).

%% Architectural keystone: a node not in B's active view can still
%% receive `Pid ! Msg' from a third node, demonstrating that raw dist
%% works independently of HyParView's bounded gossip topology.
pid_send_outside_active_view(Config) ->
    {Pa, _NodeA, _, C1} = start_peer("a", Config, #{active_size => 1}),
    {Pb, NodeB, _, C2}  = start_peer("b", C1, #{active_size => 1}),
    {Pc, NodeC, _, _}   = start_peer("c", C2, #{active_size => 1}),
    %% Pair A<->B at HyParView level.
    ok = peer:call(Pa, mycelium, join, [NodeB]),
    timer:sleep(800),
    %% Spawn echo on B.
    ok = peer:call(Pb, ?MODULE, start_echo, []),
    %% C opens a dist-only channel to B (no HyParView join).
    true = peer:call(Pc, net_kernel, connect_node, [NodeB]),
    timer:sleep(300),
    %% Precondition: C is NOT in B's active view.
    ActiveB = peer:call(Pb, mycelium, active_view, []),
    ?assertNot(lists:member(NodeC, ActiveB),
               "expected C absent from B's active view"),
    %% Raw bang from C to PidOnB.
    Result = peer:call(Pc, ?MODULE, echo_to, [NodeB, greetings_from_c], 5000),
    ?assertMatch({ok, greetings_from_c, NodeB}, Result).

active_eviction_keeps_dist(Config) ->
    {Pa, NodeA, _, C1} = start_peer("a", Config, #{active_size => 1}),
    {Pb, NodeB, _, C2} = start_peer("b", C1,     #{active_size => 1}),
    {Pc, NodeC, _, _}  = start_peer("c", C2,     #{active_size => 1}),
    ok = peer:call(Pa, mycelium, join, [NodeB]),
    timer:sleep(500),
    ok = peer:call(Pc, mycelium, join, [NodeB]),
    timer:sleep(1000),
    ActiveB = peer:call(Pb, mycelium, active_view, []),
    NodesB  = peer:call(Pb, erlang, nodes, []),
    NodesA  = peer:call(Pa, erlang, nodes, []),
    NodesC  = peer:call(Pc, erlang, nodes, []),
    ct:pal("active_view(B)=~p~nnodes(A)=~p~nnodes(B)=~p~nnodes(C)=~p",
           [ActiveB, NodesA, NodesB, NodesC]),
    ?assertEqual(1, length(ActiveB)),
    %% Whichever of A/C is still in B's active view, the OTHER is the
    %% evicted-but-should-still-be-dist-connected peer.
    Evicted = case lists:member(NodeA, ActiveB) of
        true  -> NodeC;
        false -> NodeA
    end,
    ?assert(lists:member(Evicted, NodesB),
            io_lib:format("evicted peer ~p missing from nodes(B)=~p",
                          [Evicted, NodesB])).

gc_skips_live_streams(Config) ->
    %% Large sweep_period_ms so the periodic sweep never fires during
    %% the test window; the only sweep that runs is the explicit
    %% sweep_now/0 below. min_age_ms stays low so the channel passes
    %% the age check by the time we explicitly sweep.
    GcOpts = #{gc_min_age_ms => 100, gc_sweep_period_ms => 60000},
    {Pa, _NodeA, _, C1} = start_peer("a", Config, GcOpts),
    {Pb, NodeB, _, _}   = start_peer("b", C1,     GcOpts),
    %% First connect_node call can race with the dist controller's
    %% init_state and the peer's auth handshake; use a connect_node
    %% timeout larger than peer:call's default 5s, and retry via
    %% wait_until in case the very first attempt times out.
    wait_until(fun() ->
        true =:= peer:call(Pa, net_kernel, connect_node, [NodeB], 15000)
            andalso lists:member(NodeB, peer:call(Pa, erlang, nodes, []))
    end, 30000),
    Tag = <<"gctest">>,
    ok = peer:call(Pb, ?MODULE, register_acceptor, [Tag]),
    %% Open a tagged stream and confirm it stays LIVE. The peer's dist
    %% controller refuses (resets) an inbound user stream that arrives
    %% before mycelium_streams has registered as its acceptor, so opening
    %% can race that registration on a fresh connection. Retry the whole
    %% open-and-confirm until a stream sticks, not just the open.
    {ok, _Holder} = wait_for_live_stream(Pa, Tag, NodeB, 15000),
    ok = peer:call(Pa, mycelium_dist_gc, sweep_now, []),
    NodesA = peer:call(Pa, erlang, nodes, []),
    ?assert(lists:member(NodeB, NodesA),
            "GC reaped a channel with a live user stream").

%%====================================================================
%% Helpers exported so peer:call/4 can invoke them
%%====================================================================

start_echo() ->
    Echo = spawn(fun loop_echo/0),
    register(echo, Echo),
    ok.

loop_echo() ->
    receive
        {From, Msg} ->
            From ! {echoed, Msg, node()},
            loop_echo()
    end.

echo_to(TargetNode, Msg) ->
    Pid = rpc:call(TargetNode, erlang, whereis, [echo]),
    Pid ! {self(), Msg},
    receive
        {echoed, M, N} -> {ok, M, N}
    after 3000 ->
        timeout
    end.

register_acceptor(Tag) ->
    %% Long-lived acceptor: it must survive the first stream-opened
    %% notification so the QUIC stream's remote owner stays alive and
    %% quic_dist:list_streams on the opening side keeps returning the
    %% stream until the test explicitly tears it down.
    Self = spawn(fun Loop() -> receive _ -> Loop() end end),
    mycelium_streams:register_acceptor(Tag, Self),
    ok.

%% Open a stream and HAND OWNERSHIP to a long-lived holder process
%% so it survives the peer:call returning. Returns the holder pid;
%% callers can `peer:call' it later to close the stream (or just
%% terminate the holder).
open_stream_persistent(Tag, Node) ->
    Caller = self(),
    Holder = spawn(fun() ->
        case mycelium_streams:open(Tag, Node) of
            {ok, SR} ->
                Caller ! {self(), ok, SR},
                receive close -> quic_dist:close_stream(SR) end;
            Err ->
                Caller ! {self(), Err}
        end
    end),
    receive
        {Holder, ok, _SR} -> {ok, Holder};
        {Holder, Err}     -> Err
    after 5000 ->
        exit(Holder, kill),
        {error, timeout}
    end.

%%====================================================================
%% peer setup
%%====================================================================

%% start_peer/3 -> {PeerPid, Node, NodeDir, Config'}.
%% Opts:
%%   port               -> integer (default derived from base_port + suffix hash)
%%   active_size        -> integer (default 5)
%%   gc_min_age_ms      -> integer (overrides default 300000)
%%   gc_sweep_period_ms -> integer (overrides default 60000)
start_peer(Suffix, Config, Opts) ->
    TcDir = ?config(tc_dir, Config),
    NodeDir = filename:join(TcDir, Suffix),
    ok = filelib:ensure_dir(filename:join(NodeDir, "dummy")),
    QuicDir = filename:join(NodeDir, "data/quic"),
    KeysDir = filename:join(NodeDir, "data/keys"),
    ok = filelib:ensure_dir(filename:join(QuicDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(KeysDir, "dummy")),
    DiscoveryDir = ?config(discovery_dir, Config),
    BasePort = ?config(base_port, Config),
    Port = maps:get(port, Opts, next_port(BasePort)),
    ActiveSize = maps:get(active_size, Opts, 5),
    Name = list_to_atom("myc_" ++ Suffix ++ "_"
                        ++ integer_to_list(erlang:unique_integer([positive]))),
    %% -AppName Key Value pairs land in application env automatically.
    %% Strings need to be quoted so init parses them as term strings.
    BaseArgs = [
        "-proto_dist", "mycelium",
        "-epmd_module", "mycelium_epmd",
        "-start_epmd", "false",
        "-mycelium_dist_port",     integer_to_list(Port),
        "-mycelium_dist_cert_dir", QuicDir,
        "-setcookie", "mycelium_ct",
        "-mycelium", "auth_key_dir",  quote(KeysDir),
        "-mycelium", "discovery_dir", quote(DiscoveryDir),
        "-mycelium", "active_size",   integer_to_list(ActiveSize)
    ],
    GcArgs = case Opts of
        #{gc_min_age_ms := MinAge, gc_sweep_period_ms := Period} ->
            ["-mycelium", "dist_gc_min_age_ms",      integer_to_list(MinAge),
             "-mycelium", "dist_gc_sweep_period_ms", integer_to_list(Period)];
        _ -> []
    end,
    %% Peer inherits no code path by default; inject the parent's.
    PaArgs = lists:flatmap(fun(P) -> ["-pa", P] end, code:get_path()),
    Args = PaArgs ++ BaseArgs ++ GcArgs,
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
    %% Process-dictionary counter so peers within a single test get
    %% unique, sequential ports rather than colliding via hash.
    Key = {?MODULE, next_port_offset},
    N = case get(Key) of undefined -> 0; X -> X end,
    put(Key, N + 1),
    BasePort + N.

%% Open a tagged stream and confirm it STAYS live. open_stream_persistent
%% returns {ok, Holder} as soon as the QUIC stream opens, but the peer can
%% still reset it (STREAM_REFUSED) if its user-stream acceptor isn't
%% registered yet. So after opening, wait briefly for a refusal to surface
%% and check the stream is still listed; retry the whole open-and-confirm
%% if it was refused.
wait_for_live_stream(Peer, Tag, Node, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    live_stream_loop(Peer, Tag, Node, Deadline).

live_stream_loop(Peer, Tag, Node, Deadline) ->
    Opened = peer:call(Peer, ?MODULE, open_stream_persistent, [Tag, Node]),
    Live = case Opened of
        {ok, Holder} ->
            %% Give a refusal-reset time to arrive, then confirm the
            %% stream is still there.
            timer:sleep(300),
            case peer:call(Peer, quic_dist, list_streams, [Node]) of
                [] -> false;
                L when is_list(L) -> {ok, Holder}
            end;
        _ ->
            false
    end,
    case Live of
        {ok, _} = Ok ->
            Ok;
        false ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true  -> ?assert(false, "no live user stream established");
                false -> timer:sleep(100),
                         live_stream_loop(Peer, Tag, Node, Deadline)
            end
    end.

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        _ ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> ?assert(false, "wait_until timed out");
                false ->
                    timer:sleep(100),
                    wait_loop(Fun, Deadline)
            end
    end.
