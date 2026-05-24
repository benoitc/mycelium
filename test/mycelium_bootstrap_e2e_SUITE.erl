%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E proof for config-driven seeding: nodes with `contact_nodes' set to
%%% a seed auto-join the cluster at boot, with NO manual `mycelium:join/1'
%%% anywhere. The seed is resolved through the (file) discovery chain. Peer
%%% scaffolding mirrors mycelium_reminder_e2e_SUITE.
-module(mycelium_bootstrap_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [nodes_auto_join_seed_via_contact_nodes].

init_per_suite(Config) ->
    BasePort = 25000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
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
%% Test case
%%====================================================================

%% A seed (empty contact_nodes) plus two nodes that list it in
%% contact_nodes. No test code calls join: the bootstrap worker joins the
%% seed once it resolves through discovery, and a 3-node overlay forms.
nodes_auto_join_seed_via_contact_nodes(Config) ->
    {Pa, NodeA} = start_peer("a", [], Config),
    {_Pb, NodeB} = start_peer("b", [NodeA], Config),
    {_Pc, NodeC} = start_peer("c", [NodeA], Config),

    %% The seed accepts both joiners into its active view.
    wait_until(
        fun() ->
            AV = peer:call(Pa, mycelium, active_view, []),
            lists:member(NodeB, AV) andalso lists:member(NodeC, AV)
        end,
        30000
    ),

    %% And every node ends up agreeing on the full membership set.
    Nodes = [NodeA, NodeB, NodeC],
    Peers = [{P, N} || {P, N} <- peers()],
    wait_until(fun() -> all_members_are(Peers, Nodes) end, 30000),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

peers() ->
    %% {Pid, Node} pairs recorded by start_peer, newest first.
    lists:reverse(get(peers_nodes)).

all_members_are(Peers, Expected) ->
    Want = lists:sort(Expected),
    lists:all(
        fun({P, _N}) ->
            lists:sort(peer:call(P, mycelium, members, [])) =:= Want
        end,
        Peers
    ).

%% Start a peer. `Contacts' is the contact_nodes list (empty for the seed);
%% it is injected as a boot arg so the bootstrap worker reads it before the
%% app starts. Returns {Pid, Node}.
start_peer(Suffix, Contacts, Config) ->
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
    ContactArgs =
        case Contacts of
            [] ->
                [];
            _ ->
                [
                    "-mycelium",
                    "contact_nodes",
                    lists:flatten(io_lib:format("~w", [Contacts]))
                ]
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
            %% Retry the seed join briskly so the test converges quickly.
            "-mycelium",
            "contact_retry_ms",
            "500",
            "-mycelium",
            "member_heartbeat_ms",
            "500",
            "-mycelium",
            "member_ttl_ms",
            "2000",
            "-mycelium",
            "member_skew_ms",
            "60000"
        ] ++ ContactArgs,
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
    put(peers_nodes, [{Pid, Node} | peers_nodes()]),
    {Pid, Node}.

tracked() ->
    case get(?MODULE) of
        undefined -> [];
        L -> L
    end.

peers_nodes() ->
    case get(peers_nodes) of
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
