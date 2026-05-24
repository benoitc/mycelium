%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_dist_keys.

-module(mycelium_dist_keys_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% fingerprint/1
%%====================================================================

fingerprint_returns_32_bytes_test() ->
    Fp = mycelium_dist_keys:fingerprint(<<0:256>>),
    ?assertEqual(32, byte_size(Fp)).

fingerprint_distinguishes_keys_test() ->
    Fp1 = mycelium_dist_keys:fingerprint(<<0:256>>),
    Fp2 = mycelium_dist_keys:fingerprint(<<1, 0:248>>),
    ?assertNotEqual(Fp1, Fp2).

fingerprint_rejects_short_input_test() ->
    ?assertError(
        function_clause,
        mycelium_dist_keys:fingerprint(<<0, 0, 0>>)
    ).

%%====================================================================
%% lookup_pin/1 and atomic save
%%====================================================================

lookup_pin_test_() ->
    {setup, fun setup_keys/0, fun cleanup_keys/1, fun(_) ->
        [
            {"not_pinned for unknown node", fun() ->
                ?assertEqual(
                    not_pinned,
                    mycelium_dist_keys:lookup_pin('unknown@host')
                )
            end},
            {"pinned tuple for stored node", fun() ->
                Key = <<1:256>>,
                ok = mycelium_dist_keys:store_key('a@host', Key),
                ?assertEqual(
                    {pinned, Key},
                    mycelium_dist_keys:lookup_pin('a@host')
                )
            end},
            {"is_trusted/2 returns true on exact match, false on mismatch", fun() ->
                Key = <<2:256>>,
                ok = mycelium_dist_keys:store_key('b@host', Key),
                ?assert(mycelium_dist_keys:is_trusted('b@host', Key)),
                ?assertNot(mycelium_dist_keys:is_trusted('b@host', <<3:256>>))
            end}
        ]
    end}.

atomic_save_test_() ->
    {setup, fun setup_keys/0, fun cleanup_keys/1, fun({Dir, _}) ->
        [
            {"trusted key file appears at final path, no .tmp left", fun() ->
                Key = <<7:256>>,
                ok = mycelium_dist_keys:store_key('atomic@host', Key),
                Final = filename:join(
                    [Dir, "trusted", "atomic@host.pub"]
                ),
                ?assert(filelib:is_file(Final)),
                ?assertNot(filelib:is_file(Final ++ ".tmp")),
                {ok, OnDisk} = file:read_file(Final),
                ?assertEqual(Key, OnDisk)
            end}
        ]
    end}.

setup_keys() ->
    Dir = filename:join(
        [
            "/tmp",
            "mycelium_dist_keys_tests",
            integer_to_list(erlang:unique_integer([positive]))
        ]
    ),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    application:set_env(mycelium, auth_key_dir, Dir),
    {ok, Pid} = mycelium_dist_keys:start_link(),
    {Dir, Pid}.

cleanup_keys({Dir, Pid}) ->
    gen_server:stop(Pid),
    os:cmd("rm -rf " ++ Dir),
    application:unset_env(mycelium, auth_key_dir),
    ok.
