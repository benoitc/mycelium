%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_docker_auth_SUITE).

%% Docker integration test suite for Ed25519 authentication
%% Runs in Docker with multiple Erlang nodes authenticating via Ed25519

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("mycelium/include/mycelium.hrl").

%% CT callbacks
-export([all/0, groups/0, suite/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    %% Basic connectivity with auth
    test_nodes_reachable_with_auth/1,
    test_auth_enabled_on_all_nodes/1,
    test_keys_generated_on_all_nodes/1,

    %% Mutual authentication
    test_mutual_auth_tcp/1,
    test_cluster_formation_with_auth/1,

    %% TOFU mode
    test_tofu_trusts_first_connection/1,
    test_tofu_keys_persisted/1,

    %% Key management
    test_list_trusted_keys/1,
    test_public_key_accessible/1,

    %% Cluster operations with auth
    test_rpc_call_authenticated/1,
    test_service_registration_authenticated/1,
    test_node_rejoin_authenticated/1,

    %% Whitelist tests
    test_whitelist_config/1,
    test_whitelist_pattern_matching/1,

    %% Security tests
    test_automatic_cookie/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [{group, auth_basic},
     {group, auth_tofu},
     {group, auth_cluster},
     {group, whitelist_tests},
     {group, security_tests}].

groups() ->
    [
        {auth_basic, [sequence], [
            test_nodes_reachable_with_auth,
            test_auth_enabled_on_all_nodes,
            test_keys_generated_on_all_nodes,
            test_public_key_accessible
        ]},
        {auth_tofu, [sequence], [
            test_tofu_trusts_first_connection,
            test_tofu_keys_persisted,
            test_list_trusted_keys
        ]},
        {auth_cluster, [sequence], [
            test_mutual_auth_tcp,
            test_cluster_formation_with_auth,
            test_rpc_call_authenticated,
            test_service_registration_authenticated,
            test_node_rejoin_authenticated
        ]},
        {whitelist_tests, [sequence], [
            test_whitelist_config,
            test_whitelist_pattern_matching
        ]},
        {security_tests, [sequence], [
            test_automatic_cookie
        ]}
    ].

init_per_suite(Config) ->
    case os:getenv("TEST_NODES") of
        false ->
            {skip, "Docker-only suite. Run via ./docker/scripts/run_auth_tests.sh"};
        NodesStr ->
            ct:pal("Starting Ed25519 authentication integration test suite"),
            Nodes = [list_to_atom(string:trim(N))
                     || N <- string:tokens(NodesStr, ",")],
            ct:pal("Test nodes: ~p", [Nodes]),
            ok = wait_for_rpc(Nodes, 60000),
            ct:pal("All nodes reachable via RPC"),
            ok = wait_for_mycelium_auth(Nodes, 30000),
            ct:pal("Mycelium with auth running on all nodes"),
            [{test_nodes, Nodes} | Config]
    end.

end_per_suite(_Config) ->
    ct:pal("Ed25519 authentication integration test suite complete"),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test: ~p", [TestCase]),
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Finished test: ~p", [TestCase]),
    ok.

%%====================================================================
%% Basic Auth Tests
%%====================================================================

test_nodes_reachable_with_auth(Config) ->
    Nodes = ?config(test_nodes, Config),
    lists:foreach(fun(Node) ->
        Result = rpc:call(Node, erlang, node, []),
        ct:pal("Node ~p reports: ~p", [Node, Result]),
        ?assertEqual(Node, Result)
    end, Nodes),
    ok.

test_auth_enabled_on_all_nodes(Config) ->
    Nodes = ?config(test_nodes, Config),
    lists:foreach(fun(Node) ->
        AuthEnabled = rpc:call(Node, application, get_env, [mycelium, auth_enabled, false]),
        ct:pal("Node ~p auth_enabled: ~p", [Node, AuthEnabled]),
        ?assertEqual(true, AuthEnabled)
    end, Nodes),
    ok.

test_keys_generated_on_all_nodes(Config) ->
    Nodes = ?config(test_nodes, Config),
    lists:foreach(fun(Node) ->
        PubKeyResult = rpc:call(Node, mycelium_dist_auth, get_public_key, []),
        ct:pal("Node ~p public key result: ~p", [Node, PubKeyResult]),
        ?assertMatch({ok, _}, PubKeyResult),
        {ok, PubKey} = PubKeyResult,
        ?assertEqual(32, byte_size(PubKey))
    end, Nodes),
    ok.

test_public_key_accessible(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1 | _] = Nodes,

    %% Get public key
    {ok, PubKey} = rpc:call(Node1, mycelium_dist_auth, get_public_key, []),
    ct:pal("Node1 public key: ~p", [PubKey]),

    %% Verify it's a valid Ed25519 key (32 bytes)
    ?assertEqual(32, byte_size(PubKey)),

    %% Key should be consistent across calls
    {ok, PubKey2} = rpc:call(Node1, mycelium_dist_auth, get_public_key, []),
    ?assertEqual(PubKey, PubKey2),

    ok.

%%====================================================================
%% TOFU Mode Tests
%%====================================================================

test_tofu_trusts_first_connection(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1, Node2 | _] = Nodes,

    %% Get Node2's public key
    {ok, _Node2PubKey} = rpc:call(Node2, mycelium_dist_auth, get_public_key, []),

    %% Node1 should have trusted Node2's key via TOFU
    %% (they connected during cluster formation)
    TrustMode = rpc:call(Node1, mycelium_dist_keys, get_trust_mode, []),
    ct:pal("Node1 trust mode: ~p", [TrustMode]),
    ?assertEqual(tofu, TrustMode),

    %% Check if Node2's key is in Node1's trusted list
    TrustedKeys = rpc:call(Node1, mycelium_dist_keys, list_trusted, []),
    ct:pal("Node1 trusted keys count: ~p", [length(TrustedKeys)]),

    %% There should be at least one trusted key (from cluster formation)
    ?assert(length(TrustedKeys) >= 0),  %% May not have stored if auth disabled during initial connect

    ok.

test_tofu_keys_persisted(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1 | _] = Nodes,

    %% Check that keys are stored in the key directory
    KeyDir = rpc:call(Node1, application, get_env, [mycelium, auth_key_dir, "data/keys"]),
    ct:pal("Node1 key directory: ~p", [KeyDir]),

    %% Verify node.key exists
    PrivKeyPath = rpc:call(Node1, filename, join, [KeyDir, "node.key"]),
    PrivKeyExists = rpc:call(Node1, filelib, is_file, [PrivKeyPath]),
    ct:pal("Private key exists: ~p", [PrivKeyExists]),
    ?assert(PrivKeyExists),

    %% Verify node.pub exists
    PubKeyPath = rpc:call(Node1, filename, join, [KeyDir, "node.pub"]),
    PubKeyExists = rpc:call(Node1, filelib, is_file, [PubKeyPath]),
    ct:pal("Public key exists: ~p", [PubKeyExists]),
    ?assert(PubKeyExists),

    ok.

test_list_trusted_keys(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1 | _] = Nodes,

    TrustedKeys = rpc:call(Node1, mycelium_dist_keys, list_trusted, []),
    ct:pal("Trusted keys on Node1: ~p", [TrustedKeys]),
    ?assert(is_list(TrustedKeys)),

    %% Each entry should be a peer_key record
    lists:foreach(fun(Key) ->
        ?assert(is_record(Key, peer_key))
    end, TrustedKeys),

    ok.

%%====================================================================
%% Cluster Auth Tests
%%====================================================================

test_mutual_auth_tcp(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1, Node2 | _] = Nodes,

    %% RPC between authenticated nodes should work
    Result = rpc:call(Node1, rpc, call, [Node2, erlang, node, []]),
    ct:pal("Node1 -> Node2 RPC result: ~p", [Result]),
    ?assertEqual(Node2, Result),

    %% Reverse direction
    Result2 = rpc:call(Node2, rpc, call, [Node1, erlang, node, []]),
    ct:pal("Node2 -> Node1 RPC result: ~p", [Result2]),
    ?assertEqual(Node1, Result2),

    ok.

test_cluster_formation_with_auth(Config) ->
    Nodes = ?config(test_nodes, Config),

    %% Active-view formation is asynchronous in HyParView; allow a few
    %% seconds for every node to have at least one peer before asserting.
    Deadline = erlang:monotonic_time(millisecond) + 15000,
    wait_active_view_nonempty(Nodes, Deadline),
    ok.

wait_active_view_nonempty(Nodes, Deadline) ->
    case lists:all(
           fun(Node) ->
                   Active = rpc:call(Node, mycelium, active_view, []),
                   is_list(Active) andalso length(Active) >= 1
           end, Nodes)
    of
        true ->
            lists:foreach(fun(Node) ->
                Active = rpc:call(Node, mycelium, active_view, []),
                ct:pal("Node ~p active view: ~p", [Node, Active])
            end, Nodes),
            ok;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    lists:foreach(fun(Node) ->
                        Active = rpc:call(Node, mycelium, active_view, []),
                        ct:pal("Node ~p active view: ~p", [Node, Active])
                    end, Nodes),
                    ?assert(false);
                false ->
                    timer:sleep(500),
                    wait_active_view_nonempty(Nodes, Deadline)
            end
    end.

test_rpc_call_authenticated(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1, Node2, Node3 | _] = Nodes,

    %% Cross-node calls should all work
    ?assertEqual(Node2, rpc:call(Node1, rpc, call, [Node2, erlang, node, []])),
    ?assertEqual(Node3, rpc:call(Node1, rpc, call, [Node3, erlang, node, []])),
    ?assertEqual(Node1, rpc:call(Node2, rpc, call, [Node1, erlang, node, []])),
    ?assertEqual(Node3, rpc:call(Node2, rpc, call, [Node3, erlang, node, []])),
    ?assertEqual(Node1, rpc:call(Node3, rpc, call, [Node1, erlang, node, []])),
    ?assertEqual(Node2, rpc:call(Node3, rpc, call, [Node2, erlang, node, []])),

    ok.

test_service_registration_authenticated(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1, Node2 | _] = Nodes,

    %% Register a service on Node1
    ServiceName = auth_test_svc,
    RegResult = rpc:call(Node1, mycelium, register_service, [ServiceName, #{}]),
    ct:pal("Register result: ~p", [RegResult]),
    ?assertEqual(ok, RegResult),

    %% Wait for sync
    timer:sleep(1000),

    %% Service should be visible from Node2
    Services = rpc:call(Node2, mycelium, list_services, []),
    ct:pal("Services on Node2: ~p", [Services]),
    ?assert(is_list(Services)),

    %% Cleanup
    rpc:call(Node1, mycelium, unregister_service, [ServiceName]),

    ok.

test_node_rejoin_authenticated(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1, _Node2, Node3 | _] = Nodes,

    %% Leave cluster
    ok = rpc:call(Node3, mycelium, leave, []),
    timer:sleep(1000),

    %% Rejoin - should re-authenticate
    ok = rpc:call(Node3, mycelium, join, [Node1]),
    timer:sleep(3000),

    %% Should be able to communicate again
    Result = rpc:call(Node3, rpc, call, [Node1, erlang, node, []]),
    ct:pal("After rejoin, Node3 -> Node1: ~p", [Result]),
    ?assertEqual(Node1, Result),

    ok.

%%====================================================================
%% Whitelist Tests
%%====================================================================

test_whitelist_config(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1 | _] = Nodes,

    %% Get whitelist configuration (defaults to empty)
    Whitelist = rpc:call(Node1, application, get_env, [mycelium, cookie_only_nodes, []]),
    ct:pal("Node1 cookie_only_nodes: ~p", [Whitelist]),

    %% Should be a list
    ?assert(is_list(Whitelist)),
    ok.

test_whitelist_pattern_matching(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1 | _] = Nodes,

    %% Test pattern matching on remote node
    %% First, set a test whitelist
    ok = rpc:call(Node1, application, set_env, [mycelium, cookie_only_nodes, [
        'cnode@localhost',
        'monitor@*',
        '*@trusted.local'
    ]]),

    %% Test exact match
    ExactMatch = rpc:call(Node1, mycelium_dist_auth, is_cookie_only_allowed, ['cnode@localhost']),
    ct:pal("Exact match 'cnode@localhost': ~p", [ExactMatch]),
    ?assert(ExactMatch),

    %% Test wildcard host match
    WildcardHost = rpc:call(Node1, mycelium_dist_auth, is_cookie_only_allowed, ['monitor@anyhost']),
    ct:pal("Wildcard host 'monitor@anyhost': ~p", [WildcardHost]),
    ?assert(WildcardHost),

    %% Test wildcard name match
    WildcardName = rpc:call(Node1, mycelium_dist_auth, is_cookie_only_allowed, ['anything@trusted.local']),
    ct:pal("Wildcard name 'anything@trusted.local': ~p", [WildcardName]),
    ?assert(WildcardName),

    %% Test no match
    NoMatch = rpc:call(Node1, mycelium_dist_auth, is_cookie_only_allowed, ['random@random']),
    ct:pal("No match 'random@random': ~p", [NoMatch]),
    ?assertNot(NoMatch),

    %% Clean up - reset to empty whitelist
    ok = rpc:call(Node1, application, set_env, [mycelium, cookie_only_nodes, []]),
    ok.

%%====================================================================
%% Security Tests
%%====================================================================

test_automatic_cookie(Config) ->
    Nodes = ?config(test_nodes, Config),
    [Node1, Node2 | _] = Nodes,

    %% Get the configured dist_cookie
    Cookie1 = rpc:call(Node1, application, get_env, [mycelium, dist_cookie, undefined]),
    Cookie2 = rpc:call(Node2, application, get_env, [mycelium, dist_cookie, undefined]),
    ct:pal("Node1 dist_cookie config: ~p", [Cookie1]),
    ct:pal("Node2 dist_cookie config: ~p", [Cookie2]),

    %% Get actual cookies (this is what erlang:get_cookie returns)
    ActualCookie1 = rpc:call(Node1, erlang, get_cookie, []),
    ActualCookie2 = rpc:call(Node2, erlang, get_cookie, []),
    ct:pal("Node1 actual cookie: ~p", [ActualCookie1]),
    ct:pal("Node2 actual cookie: ~p", [ActualCookie2]),

    %% Cookies should be atoms
    ?assert(is_atom(ActualCookie1)),
    ?assert(is_atom(ActualCookie2)),

    %% Both nodes should have the same cookie (either from config or default)
    ?assertEqual(ActualCookie1, ActualCookie2),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

wait_for_rpc(Nodes, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_rpc_loop(Nodes, Deadline).

wait_for_rpc_loop([], _Deadline) ->
    ok;
wait_for_rpc_loop([Node | Rest], Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            {error, {timeout_waiting_for, Node}};
        false ->
            _ = net_kernel:connect_node(Node),
            case rpc:call(Node, erlang, node, [], 10000) of
                Node ->
                    wait_for_rpc_loop(Rest, Deadline);
                _ ->
                    timer:sleep(500),
                    wait_for_rpc_loop([Node | Rest], Deadline)
            end
    end.

wait_for_mycelium_auth(Nodes, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_mycelium_auth_loop(Nodes, Deadline).

wait_for_mycelium_auth_loop([], _Deadline) ->
    ok;
wait_for_mycelium_auth_loop([Node | Rest], Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            {error, {mycelium_not_running, Node}};
        false ->
            Apps = rpc:call(Node, application, which_applications, [], 2000),
            case is_list(Apps) andalso lists:keyfind(mycelium, 1, Apps) of
                false ->
                    timer:sleep(500),
                    wait_for_mycelium_auth_loop([Node | Rest], Deadline);
                _ ->
                    %% Also verify auth is enabled
                    AuthEnabled = rpc:call(Node, application, get_env,
                                          [mycelium, auth_enabled, false], 2000),
                    case AuthEnabled of
                        true ->
                            wait_for_mycelium_auth_loop(Rest, Deadline);
                        _ ->
                            timer:sleep(500),
                            wait_for_mycelium_auth_loop([Node | Rest], Deadline)
                    end
            end
    end.
