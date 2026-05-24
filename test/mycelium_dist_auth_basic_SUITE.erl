%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% mycelium auth basic Common Test suite.
%%%
%%% Local-CT counterpart of `mycelium_docker_auth_SUITE'. Spawns three
%%% slave nodes via `ct_slave', each with its own `auth_key_dir', and
%%% drives `mycelium_dist_keys' / `mycelium_dist_auth' over RPC. We do
%%% not stand the slaves up on `-proto_dist mycelium' here, so the
%%% Ed25519 handshake driven by `mycelium_dist_protocol' is not
%%% exercised end-to-end - that path stays under the docker auth suite.
%%% What we do cover: per-node keypair generation, on-disk persistence,
%%% trust-mode toggling, and the `store_key' / `is_trusted' /
%%% `list_trusted' API that the handshake feeds into.
%%%

-module(mycelium_dist_auth_basic_SUITE).

-compile([{nowarn_deprecated_function, [{ct_slave, start, 2}, {ct_slave, stop, 1}]}]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    suite/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    keys_generated_test/1,
    keypair_persisted_test/1,
    trust_mode_test/1,
    tofu_store_if_new_test/1,
    strict_mode_rejects_change_test/1,
    list_trusted_test/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [{group, three_node}].

groups() ->
    [
        {three_node, [sequence], [
            keys_generated_test,
            keypair_persisted_test,
            trust_mode_test,
            tofu_store_if_new_test,
            strict_mode_rejects_change_test,
            list_trusted_test
        ]}
    ].

init_per_suite(Config) ->
    case net_kernel:start([ct_parent_for(?MODULE), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    erlang:set_cookie(node(), mycelium_auth_ct_cookie),
    %% Earlier suites may have started the mycelium application on the
    %% CT BEAM, which sets dist_auto_connect=never. ct_slave relies on
    %% auto-connect to confirm slave boot, so reset it here.
    ok = application:set_env(kernel, dist_auto_connect, true),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(three_node, Config) ->
    PrivDir = ?config(priv_dir, Config),
    case start_peer_nodes(PrivDir) of
        {ok, Nodes} ->
            [{nodes, Nodes} | Config];
        {error, Reason} ->
            {skip, {peer_start_failed, Reason}}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(three_node, Config) ->
    lists:foreach(
        fun(N) -> catch ct_slave:stop(N) end,
        [Node || {Node, _Dir} <- ?config(nodes, Config)]
    ),
    ok;
end_per_group(_Group, _Config) ->
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% Each peer's auth_key_dir contains node.key + node.pub after startup.
keys_generated_test(Config) ->
    Nodes = ?config(nodes, Config),
    lists:foreach(
        fun({Node, KeyDir}) ->
            PrivKey = filename:join(KeyDir, "node.key"),
            PubKey = filename:join(KeyDir, "node.pub"),
            ?assert(rpc:call(Node, filelib, is_file, [PrivKey])),
            ?assert(rpc:call(Node, filelib, is_file, [PubKey])),
            {ok, _} = rpc:call(Node, mycelium_dist_auth, get_public_key, []),
            {ok, _} = rpc:call(Node, mycelium_dist_auth, get_private_key, [])
        end,
        Nodes
    ),
    ok.

%% Restarting mycelium_dist_keys reloads the same keypair from disk.
keypair_persisted_test(Config) ->
    [{Node, _KeyDir} | _] = ?config(nodes, Config),

    {ok, PubKeyBefore} = rpc:call(Node, mycelium_dist_auth, get_public_key, []),
    ok = rpc:call(Node, application, stop, [mycelium]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [mycelium]),
    {ok, PubKeyAfter} = rpc:call(Node, mycelium_dist_auth, get_public_key, []),

    ?assertEqual(PubKeyBefore, PubKeyAfter),
    ok.

%% Trust mode flips between strict and tofu via the API.
trust_mode_test(Config) ->
    [{Node, _KeyDir} | _] = ?config(nodes, Config),

    ok = rpc:call(Node, mycelium_dist_keys, set_trust_mode, [strict]),
    strict = rpc:call(Node, mycelium_dist_keys, get_trust_mode, []),
    ok = rpc:call(Node, mycelium_dist_keys, set_trust_mode, [tofu]),
    tofu = rpc:call(Node, mycelium_dist_keys, get_trust_mode, []),
    ok.

%% store_key_if_new is the TOFU primitive: first key wins, second is
%% rejected silently (returns ok but does not overwrite).
tofu_store_if_new_test(Config) ->
    [{Node, _KeyDir}, {PeerNode, _PeerDir} | _] = ?config(nodes, Config),

    %% Drop any existing entry from prior tests so we start clean.
    ok = rpc:call(Node, mycelium_dist_keys, delete_key, [PeerNode]),
    ok = rpc:call(Node, mycelium_dist_keys, set_trust_mode, [tofu]),

    Key1 = crypto:strong_rand_bytes(32),
    Key2 = crypto:strong_rand_bytes(32),

    ok = rpc:call(Node, mycelium_dist_keys, store_key_if_new, [PeerNode, Key1]),
    ?assert(rpc:call(Node, mycelium_dist_keys, is_trusted, [PeerNode, Key1])),

    %% Second store with a different key must not overwrite.
    rpc:call(Node, mycelium_dist_keys, store_key_if_new, [PeerNode, Key2]),
    ?assertNot(rpc:call(Node, mycelium_dist_keys, is_trusted, [PeerNode, Key2])),
    ?assert(rpc:call(Node, mycelium_dist_keys, is_trusted, [PeerNode, Key1])),

    ok = rpc:call(Node, mycelium_dist_keys, delete_key, [PeerNode]),
    ok.

%% In strict mode a node already on disk with one key is not trusted
%% under a different key.
strict_mode_rejects_change_test(Config) ->
    [{Node, _KeyDir}, {PeerNode, _PeerDir} | _] = ?config(nodes, Config),

    ok = rpc:call(Node, mycelium_dist_keys, set_trust_mode, [strict]),
    Key1 = crypto:strong_rand_bytes(32),
    Key2 = crypto:strong_rand_bytes(32),

    ok = rpc:call(Node, mycelium_dist_keys, store_key, [PeerNode, Key1]),
    ?assert(rpc:call(Node, mycelium_dist_keys, is_trusted, [PeerNode, Key1])),
    ?assertNot(rpc:call(Node, mycelium_dist_keys, is_trusted, [PeerNode, Key2])),

    ok = rpc:call(Node, mycelium_dist_keys, delete_key, [PeerNode]),
    ok = rpc:call(Node, mycelium_dist_keys, set_trust_mode, [tofu]),
    ok.

%% list_trusted returns every node we have stored a key for, on top of
%% whatever the auth handshake may have added.
list_trusted_test(Config) ->
    [{Node, _KeyDir} | _] = ?config(nodes, Config),

    PeerA = 'peer_a@host',
    PeerB = 'peer_b@host',
    KeyA = crypto:strong_rand_bytes(32),
    KeyB = crypto:strong_rand_bytes(32),

    ok = rpc:call(Node, mycelium_dist_keys, store_key, [PeerA, KeyA]),
    ok = rpc:call(Node, mycelium_dist_keys, store_key, [PeerB, KeyB]),

    Trusted = rpc:call(Node, mycelium_dist_keys, list_trusted, []),
    Nodes = [element(2, T) || T <- Trusted, is_tuple(T)],
    ?assert(lists:member(PeerA, Nodes)),
    ?assert(lists:member(PeerB, Nodes)),

    ok = rpc:call(Node, mycelium_dist_keys, delete_key, [PeerA]),
    ok = rpc:call(Node, mycelium_dist_keys, delete_key, [PeerB]),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

start_peer_nodes(PrivDir) ->
    Specs = [
        {"myc_auth_n1", 14443},
        {"myc_auth_n2", 14444},
        {"myc_auth_n3", 14445}
    ],

    Paths = [
        P
     || P <- code:get_path(),
        P =/= ".",
        filename:basename(P) =/= "erlang-mycelium",
        P =/= ""
    ],
    CodePath = string:join(Paths, " "),

    ErlFlags =
        "-mycelium auth_enabled true "
        "-mycelium auth_trust_mode tofu "
        "-kernel prevent_overlapping_partitions false "
        "-pa " ++ CodePath,

    try
        Cookie = erlang:get_cookie(),

        Nodes = lists:map(
            fun({Prefix, Port}) ->
                Short = unique_short_name(Prefix),
                {ok, Node} = ct_slave:start(Short, [
                    {erl_flags, ErlFlags},
                    {monitor_master, true},
                    {boot_timeout, 30}
                ]),
                KeyDir = filename:join(PrivDir, atom_to_list(Short)),
                ok = filelib:ensure_dir(filename:join(KeyDir, "dummy")),

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
                    [mycelium, auth_enabled, true]
                ),
                ok = rpc:call(
                    Node,
                    application,
                    set_env,
                    [mycelium, auth_trust_mode, tofu]
                ),
                ok = rpc:call(
                    Node,
                    application,
                    set_env,
                    [mycelium, auth_key_dir, KeyDir]
                ),
                {ok, _} = rpc:call(
                    Node,
                    application,
                    ensure_all_started,
                    [mycelium]
                ),
                {Node, KeyDir}
            end,
            Specs
        ),

        %% Pre-link the dist mesh so global stays happy and the test
        %% doesn't have to wait for demand-driven auto-connect on the
        %% first RPC.
        [{First, _} | Rest] = Nodes,
        lists:foreach(
            fun({Other, _}) ->
                true = rpc:call(First, net_kernel, connect_node, [Other])
            end,
            Rest
        ),

        {ok, Nodes}
    catch
        _:Reason:_St ->
            {error, Reason}
    end.

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
