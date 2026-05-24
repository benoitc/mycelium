%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% mycelium cluster basic Common Test suite.
%%%
%%% Spawns slave nodes via `ct_slave' over plain `inet_tcp_dist' and
%%% runs the mycelium application on each. Tests cluster mechanics
%%% (HyParView join, active view, service registry, message round-
%%% trip) without standing up Docker or making the CT BEAM itself a
%%% mycelium_dist node. Local-CT replacement for the docker-based
%%% `mycelium_integration_SUITE'.
%%%

-module(mycelium_dist_basic_SUITE).

%% ct_slave is deprecated since OTP 27 in favour of `peer'/`?CT_PEER',
%% but `peer:start_link' fails the boot handshake on this OTP build
%% (both stdio and dist connection modes). ct_slave still works and
%% is supported through OTP 30.
-compile([{nowarn_deprecated_function, [{ct_slave, start, 2}, {ct_slave, stop, 1}]}]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    node_connect_test/1,
    rpc_call_test/1,
    active_view_test/1,
    register_service_test/1,
    list_services_test/1,
    leave_rejoin_test/1,
    large_message_test/1,
    disconnect_reconnect_test/1,
    registry_replica_running_test/1,
    whereis_service_remote_test/1,
    proxy_creation_test/1,
    shuffle_test/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [{group, two_node}].

groups() ->
    [
        {two_node, [sequence], [
            node_connect_test,
            rpc_call_test,
            active_view_test,
            register_service_test,
            list_services_test,
            registry_replica_running_test,
            whereis_service_remote_test,
            proxy_creation_test,
            shuffle_test,
            leave_rejoin_test,
            large_message_test,
            disconnect_reconnect_test
        ]}
    ].

init_per_suite(Config) ->
    %% ct_slave needs the CT BEAM to be a distributed node. rebar3
    %% ct doesn't add -sname by default, so we start net_kernel
    %% ourselves.
    case net_kernel:start([ct_parent_for(?MODULE), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    erlang:set_cookie(node(), mycelium_ct_cookie),
    %% Earlier suites may have started the mycelium application on the
    %% CT BEAM, which sets dist_auto_connect=never. ct_slave relies on
    %% auto-connect to confirm slave boot, so reset it here.
    ok = application:set_env(kernel, dist_auto_connect, true),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(two_node, Config) ->
    case start_peer_nodes() of
        {ok, Node1, Node2} ->
            [{node1, Node1}, {node2, Node2} | Config];
        {error, Reason} ->
            {skip, {peer_start_failed, Reason}}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(two_node, Config) ->
    catch ct_slave:stop(?config(node1, Config)),
    catch ct_slave:stop(?config(node2, Config)),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% Two peers can connect over mycelium_dist via mycelium:join/1.
node_connect_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    ok = rpc:call(Node2, mycelium, join, [Node1]),
    wait_until(
        fun() ->
            lists:member(Node2, rpc:call(Node1, erlang, nodes, [])) andalso
                lists:member(Node1, rpc:call(Node2, erlang, nodes, []))
        end,
        5000
    ),
    ok.

%% RPC via Erlang dist works once the peers have joined.
rpc_call_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    %% Each peer can resolve the other's identity via rpc.
    Node2 = rpc:call(Node1, rpc, call, [Node2, erlang, node, []]),
    Node1 = rpc:call(Node2, rpc, call, [Node1, erlang, node, []]),
    ok.

%% After join, each peer's HyParView active view contains the other.
active_view_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    wait_until(
        fun() ->
            Active1 = rpc:call(Node1, mycelium, active_view, []),
            Active2 = rpc:call(Node2, mycelium, active_view, []),
            lists:member(Node2, Active1) andalso lists:member(Node1, Active2)
        end,
        5000
    ),
    ok.

%% Service registration on Node1 is visible via list_services/0.
register_service_test(Config) ->
    Node1 = ?config(node1, Config),

    %% Spawn a holder process on Node1: mycelium_registry monitors the
    %% caller and removes the entry when it exits, so we cannot use the
    %% short-lived RPC proxy pid.
    Holder = rpc:call(Node1, erlang, spawn, [
        fun() ->
            receive
                stop -> ok
            end
        end
    ]),
    ok = rpc:call(
        Node1,
        mycelium_registry,
        register_service,
        [my_service, Holder, #{kind => test}]
    ),
    Services = rpc:call(Node1, mycelium, list_services, []),
    ?assert(is_list(Services)),
    ?assert(lists:member(my_service, Services)),

    rpc:call(Node1, mycelium, unregister_service, [my_service]),
    Holder ! stop,
    ok.

%% list_services/0 is a gen_server call - exercises that path.
list_services_test(Config) ->
    Node1 = ?config(node1, Config),
    Result = rpc:call(Node1, mycelium, list_services, []),
    ?assert(is_list(Result)),
    ok.

%% leave/0 followed by join/1 reconverges the active view.
leave_rejoin_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    ok = rpc:call(Node2, mycelium, leave, []),
    wait_until(
        fun() ->
            Active = rpc:call(Node2, mycelium, active_view, []),
            not lists:member(Node1, Active)
        end,
        5000
    ),

    ok = rpc:call(Node2, mycelium, join, [Node1]),
    %% Rejoin re-adds Node1 to the active view only after an async
    %% peer_connected signal (join -> request_connect -> peer_connected),
    %% which on a loaded CI runner can take a fresh connect + handshake.
    %% Use the wider e2e budget so the multi-hop signal isn't raced.
    wait_until(
        fun() ->
            Active = rpc:call(Node2, mycelium, active_view, []),
            lists:member(Node1, Active)
        end,
        15000
    ),
    ok.

%% Large message round-trip between the two slaves.
large_message_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    %% Run the round-trip inside a Node1-rooted RPC so we exercise the
    %% slave-to-slave dist link rather than the CT-parent-to-slave one.
    Bin = crypto:strong_rand_bytes(1024 * 1024),
    Hash = crypto:hash(sha256, Bin),
    Result = rpc:call(Node1, erlang, apply, [
        fun(Peer, Data) ->
            Caller = self(),
            ReceiverPid = spawn(Peer, fun() ->
                receive
                    {large, B, From} ->
                        From ! {hash, crypto:hash(sha256, B)}
                after 30000 ->
                    ok
                end
            end),
            ReceiverPid ! {large, Data, Caller},
            receive
                {hash, H} -> H
            after 30000 ->
                timeout
            end
        end,
        [Node2, Bin]
    ]),
    ?assertEqual(Hash, Result).

%% disconnect_node clears the dist link; reconnect re-establishes it.
disconnect_reconnect_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    true = rpc:call(Node1, erlang, disconnect_node, [Node2]),
    timer:sleep(200),
    ?assertNot(lists:member(Node2, rpc:call(Node1, erlang, nodes, []))),

    %% Re-establish via explicit connect_node/1; the test doesn't rely
    %% on auto-connect timing.
    true = rpc:call(Node1, net_kernel, connect_node, [Node2]),
    wait_until(
        fun() ->
            lists:member(Node2, rpc:call(Node1, erlang, nodes, []))
        end,
        5000
    ),
    ok.

%% The registry's replication driver runs as a named process on every node.
registry_replica_running_test(Config) ->
    lists:foreach(
        fun(Node) ->
            Pid = rpc:call(Node, erlang, whereis, [mycelium_registry_replica]),
            ?assert(is_pid(Pid))
        end,
        [?config(node1, Config), ?config(node2, Config)]
    ),
    ok.

%% A service registered on Node2 is discoverable from Node1 via whereis.
whereis_service_remote_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    ServiceName = remote_svc_test,
    {ok, HolderPid} = rpc:call(
        Node2,
        mycelium,
        start_service_holder,
        [ServiceName]
    ),
    try
        wait_until(
            fun() ->
                case
                    rpc:call(
                        Node1,
                        mycelium,
                        whereis_service,
                        [ServiceName]
                    )
                of
                    {ok, _} -> true;
                    {ok, _, _} -> true;
                    {found, _, _} -> true;
                    _ -> false
                end
            end,
            5000
        )
    after
        rpc:call(Node2, mycelium, stop_service_holder, [HolderPid])
    end,
    ok.

%% ensure_proxy creates a local proxy pid that fronts a remote service.
proxy_creation_test(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),

    ServiceName = proxy_test_svc,
    {ok, HolderPid} = rpc:call(
        Node2,
        mycelium,
        start_service_holder,
        [ServiceName]
    ),
    try
        timer:sleep(500),
        {ok, ProxyPid} = rpc:call(
            Node1,
            mycelium_registry,
            ensure_proxy,
            [ServiceName, Node2]
        ),
        ?assert(is_pid(ProxyPid)),
        ?assertEqual(Node1, node(ProxyPid))
    after
        rpc:call(Node2, mycelium, stop_service_holder, [HolderPid])
    end,
    ok.

%% trigger_shuffle is the HyParView passive-view exchange. Just make
%% sure it doesn't crash and that the node still reports a sane view.
shuffle_test(Config) ->
    Node1 = ?config(node1, Config),
    ok = rpc:call(Node1, mycelium_hyparview_shuffle, trigger_shuffle, []),
    timer:sleep(500),
    Active = rpc:call(Node1, mycelium, active_view, []),
    ?assert(is_list(Active)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Spawn two slave nodes with plain `inet_tcp_dist' so the CT BEAM
%% can RPC into them (single-proto_dist avoids the EPMD registration
%% clobber you get with `-proto_dist inet_tcp mycelium'). Each slave
%% has the mycelium application running with its own QUIC listen_port,
%% which is what HyParView/registry/circuit code under test actually
%% uses; mycelium_dist as a dist carrier is exercised by other suites.
start_peer_nodes() ->
    Port1 = 14433,
    Port2 = 14434,

    Node1Short = unique_short_name("myc_ct_n1"),
    Node2Short = unique_short_name("myc_ct_n2"),

    Paths = [
        P
     || P <- code:get_path(),
        P =/= ".",
        filename:basename(P) =/= "erlang-mycelium",
        P =/= ""
    ],
    CodePath = string:join(Paths, " "),

    %% NOTE: do NOT pass -setcookie here. ct_slave's get_cmd already
    %% injects the parent's cookie; a duplicate flag causes boot_timeout.
    %% prevent_overlapping_partitions=false stops `global' from kicking
    %% the slaves apart when the CT parent observes them as disconnected
    %% from each other before mycelium:join links them.
    ErlFlags = fun() ->
        "-mycelium auth_enabled false "
        "-kernel prevent_overlapping_partitions false "
        "-pa " ++ CodePath
    end,

    try
        {ok, Node1} = ct_slave:start(Node1Short, [
            {erl_flags, ErlFlags()},
            {monitor_master, true},
            {boot_timeout, 30}
        ]),
        {ok, Node2} = ct_slave:start(Node2Short, [
            {erl_flags, ErlFlags()},
            {monitor_master, true},
            {boot_timeout, 30}
        ]),

        Cookie = erlang:get_cookie(),

        lists:foreach(
            fun({Node, Port}) ->
                %% Keep the slave on the parent's cookie - mycelium_app's
                %% init_dist_cookie/0 otherwise resets it to `mycelium'
                %% and breaks the established CT->slave connection on
                %% any later reconnect.
                ok = rpc:call(
                    Node,
                    application,
                    set_env,
                    [mycelium, dist_cookie, Cookie]
                ),
                ok = rpc:call(
                    Node,
                    application,
                    set_env,
                    [mycelium, listen_port, Port]
                ),
                ok = rpc:call(
                    Node,
                    application,
                    set_env,
                    [mycelium, auth_enabled, false]
                ),
                {ok, _} = rpc:call(
                    Node,
                    application,
                    ensure_all_started,
                    [mycelium]
                )
            end,
            [{Node1, Port1}, {Node2, Port2}]
        ),

        %% Pre-link Node1 and Node2 explicitly so test cases can RPC
        %% across them and `global' sees a fully-connected cluster
        %% without waiting for the first `Pid ! Msg' to trigger
        %% demand-driven auto-connect.
        true = rpc:call(Node1, net_kernel, connect_node, [Node2]),

        {ok, Node1, Node2}
    catch
        _:Reason:_St ->
            {error, Reason}
    end.

%% Use OS pid + monotonic time to make the name globally unique. A bare
%% monotonic counter restarts at 1 per BEAM, so two consecutive `rebar3
%% ct' runs both pick `prefix1', and a zombie from the first leaves an
%% EPMD registration that blocks the second.
unique_short_name(Prefix) ->
    list_to_atom(
        Prefix ++ "_" ++ os:getpid() ++ "_" ++
            integer_to_list(erlang:system_time(microsecond))
    ).

ct_parent_for(Mod) ->
    list_to_atom(
        "ct_parent_" ++ atom_to_list(Mod) ++ "_" ++
            integer_to_list(erlang:unique_integer([positive, monotonic]))
    ).

wait_until(Pred, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_until_loop(Pred, Deadline).

wait_until_loop(Pred, Deadline) ->
    case Pred() of
        true ->
            ok;
        _ ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    ?assert(false);
                false ->
                    timer:sleep(100),
                    wait_until_loop(Pred, Deadline)
            end
    end.
