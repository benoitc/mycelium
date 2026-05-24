%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Eunit for mycelium_discovery + the file/static/dns backends.

-module(mycelium_discovery_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    Dir = make_tmp_dir(),
    application:set_env(mycelium, discovery_dir, Dir),
    application:set_env(
        mycelium,
        discovery_backends,
        [
            mycelium_discovery_static,
            mycelium_discovery_file,
            mycelium_discovery_dns
        ]
    ),
    application:set_env(quic, dist, []),
    Dir.

teardown(Dir) ->
    application:unset_env(mycelium, discovery_dir),
    application:unset_env(mycelium, discovery_backends),
    application:unset_env(quic, dist),
    os:cmd("rm -rf " ++ Dir),
    ok.

with(Test) ->
    {setup, fun setup/0, fun teardown/1, fun(Dir) -> [?_test(Test(Dir))] end}.

%%====================================================================
%% file backend
%%====================================================================

file_register_lookup_roundtrip_test_() ->
    with(fun(_Dir) ->
        {ok, _} = mycelium_discovery_file:init(#{}),
        {ok, _} = mycelium_discovery_file:register('node1@host', 9100, undefined),
        ?assertEqual(
            {ok, {"host", 9100}},
            mycelium_discovery_file:lookup('node1@host', "host")
        ),
        ?assertEqual(
            {error, not_found},
            mycelium_discovery_file:lookup('missing@host', "host")
        )
    end).

file_list_nodes_test_() ->
    with(fun(_Dir) ->
        {ok, _} = mycelium_discovery_file:init(#{}),
        {ok, _} = mycelium_discovery_file:register('a@h', 9100, undefined),
        {ok, _} = mycelium_discovery_file:register('b@h', 9101, undefined),
        {ok, Nodes} = mycelium_discovery_file:list_nodes("h"),
        ?assertEqual(
            [{'a@h', 9100}, {'b@h', 9101}],
            lists:sort(Nodes)
        )
    end).

file_unregister_test_() ->
    with(fun(_Dir) ->
        {ok, _} = mycelium_discovery_file:init(#{}),
        {ok, _} = mycelium_discovery_file:register('a@h', 9100, undefined),
        ok = mycelium_discovery_file:unregister('a@h'),
        ?assertEqual(
            {error, not_found},
            mycelium_discovery_file:lookup('a@h', "h")
        ),
        %% unregistering an absent node is a no-op
        ?assertEqual(ok, mycelium_discovery_file:unregister('a@h'))
    end).

%% Upstream `quic_dist' may pass a bare name string (no `@host') on
%% the listen-time register-with-epmd path. Accept it without
%% crashing.
file_register_string_input_test_() ->
    with(fun(_Dir) ->
        {ok, _} = mycelium_discovery_file:init(#{}),
        ?assertMatch(
            {ok, _},
            mycelium_discovery_file:register("smoke", 9100, undefined)
        ),
        ?assertEqual(
            {ok, {"127.0.0.1", 9100}},
            mycelium_discovery_file:lookup("smoke", "h")
        )
    end).

file_register_binary_input_test_() ->
    with(fun(_Dir) ->
        {ok, _} = mycelium_discovery_file:init(#{}),
        ?assertMatch(
            {ok, _},
            mycelium_discovery_file:register(<<"smoke@h">>, 9100, undefined)
        ),
        ?assertEqual(
            {ok, {"h", 9100}},
            mycelium_discovery_file:lookup(<<"smoke@h">>, "h")
        )
    end).

%% A crafted filename that does not match the `name@host' atom
%% shape must be skipped instead of minting an atom. discovery_dir
%% can be shared or writable by another local process; without
%% validation, arbitrary file names would exhaust the atom table.
file_skips_malformed_filename_test_() ->
    with(fun(Dir) ->
        {ok, _} = mycelium_discovery_file:init(#{}),
        Bad = filename:join(Dir, "not a valid name!.endpoint"),
        ok = file:write_file(Bad, <<"{\"h\", 9100}.">>),
        Worse = filename:join(Dir, "../escape@host.endpoint"),
        ok = file:write_file(Worse, <<"{\"h\", 9100}.">>),
        %% list_nodes ignores the malformed entries entirely.
        {ok, Nodes} = mycelium_discovery_file:list_nodes("h"),
        ?assertEqual([], Nodes)
    end).

%%====================================================================
%% dns backend
%%====================================================================

%% Same input-shape contract as the file backend.
dns_extract_host_string_input_test_() ->
    %% Bare name (no @host) should not crash; lookup falls back to
    %% the Host argument.
    ?_assertMatch(
        {ok, _},
        mycelium_discovery_dns:lookup("smoke", "127.0.0.1")
    ).

dns_extract_host_binary_input_test_() ->
    ?_assertMatch(
        {ok, _},
        mycelium_discovery_dns:lookup(
            <<"smoke@127.0.0.1">>,
            "unreachable.invalid"
        )
    ).

%%====================================================================
%% static backend
%%====================================================================

static_lookup_2_tuple_test_() ->
    with(fun(_) ->
        application:set_env(
            quic,
            dist,
            [{nodes, [{'foo@h', {{127, 0, 0, 1}, 9100}}]}]
        ),
        ?assertEqual(
            {ok, {{127, 0, 0, 1}, 9100}},
            mycelium_discovery_static:lookup('foo@h', "h")
        )
    end).

static_lookup_3_tuple_test_() ->
    with(fun(_) ->
        application:set_env(
            quic,
            dist,
            [{nodes, [{'foo@h', "127.0.0.1", 9100}]}]
        ),
        ?assertEqual(
            {ok, {"127.0.0.1", 9100}},
            mycelium_discovery_static:lookup('foo@h', "h")
        )
    end).

static_lookup_miss_test_() ->
    with(fun(_) ->
        application:set_env(quic, dist, []),
        ?assertEqual(
            {error, not_found},
            mycelium_discovery_static:lookup('foo@h', "h")
        )
    end).

%%====================================================================
%% composing dispatcher
%%====================================================================

dispatcher_first_hit_wins_test_() ->
    with(fun(_) ->
        %% Static wins over file when both have the node.
        {ok, _} = mycelium_discovery_file:init(#{}),
        {ok, _} = mycelium_discovery_file:register('foo@h', 9999, undefined),
        application:set_env(
            quic,
            dist,
            [{nodes, [{'foo@h', {{1, 2, 3, 4}, 4242}}]}]
        ),
        {ok, _} = mycelium_discovery:init(#{}),
        ?assertEqual(
            {ok, {{1, 2, 3, 4}, 4242}},
            mycelium_discovery:lookup('foo@h', "h")
        )
    end).

dispatcher_falls_through_to_file_test_() ->
    with(fun(_) ->
        application:set_env(quic, dist, []),
        {ok, _} = mycelium_discovery_file:init(#{}),
        {ok, _} = mycelium_discovery_file:register('foo@h', 9100, undefined),
        {ok, _} = mycelium_discovery:init(#{}),
        ?assertEqual(
            {ok, {"h", 9100}},
            mycelium_discovery:lookup('foo@h', "h")
        )
    end).

dispatcher_register_fans_out_test_() ->
    with(fun(_) ->
        {ok, S0} = mycelium_discovery:init(#{}),
        {ok, _S1} = mycelium_discovery:register('bar@h', 9100, S0),
        %% file backend should have an entry on disk now
        ?assertEqual(
            {ok, {"h", 9100}},
            mycelium_discovery_file:lookup('bar@h', "h")
        ),
        %% dispatcher list_nodes unions both backends
        application:set_env(
            quic,
            dist,
            [{nodes, [{'baz@h', {{1, 1, 1, 1}, 4433}}]}]
        ),
        {ok, Nodes} = mycelium_discovery:list_nodes("h"),
        Sorted = lists:sort(Nodes),
        ?assertEqual([{'bar@h', 9100}, {'baz@h', 4433}], Sorted)
    end).

%%====================================================================
%% Helpers
%%====================================================================

make_tmp_dir() ->
    Base = filename:join(["/tmp", "mycelium_discovery_" ++ integer_to_list(rand:uniform(100000))]),
    ok = filelib:ensure_dir(filename:join(Base, "dummy")),
    Base.
