%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Eunit for mycelium_rotate. Exercises rotate_identity end-to-end on
%%% a temp dir. rotate_cert lives in the CT suite (it depends on the
%%% public_key cert chain and is heavier).

-module(mycelium_rotate_tests).

-include_lib("eunit/include/eunit.hrl").

with_tmp(F) ->
    Tmp = filename:join(
        "/tmp",
        [
            "mycelium_rotate_test_",
            integer_to_list(erlang:unique_integer([positive]))
        ]
    ),
    ok = filelib:ensure_dir(filename:join(Tmp, "dummy")),
    try
        F(Tmp)
    after
        _ = os:cmd("rm -rf " ++ Tmp)
    end.

rotate_identity_fresh_test() ->
    with_tmp(fun(Tmp) ->
        {ok, Info} = mycelium_rotate:rotate_identity(#{key_dir => Tmp}),
        ?assertEqual(undefined, maps:get(backup_dir, Info)),
        ?assertEqual(false, maps:get(restart_required, Info)),
        ?assert(filelib:is_regular(filename:join(Tmp, "node.pub"))),
        ?assert(filelib:is_regular(filename:join(Tmp, "node.key")))
    end).

rotate_identity_backs_up_test() ->
    with_tmp(fun(Tmp) ->
        %% First rotation seeds the directory.
        {ok, _} = mycelium_rotate:rotate_identity(#{key_dir => Tmp}),
        OrigPub = read(filename:join(Tmp, "node.pub")),
        OrigPriv = read(filename:join(Tmp, "node.key")),
        %% Second rotation must back up the original and produce a new one.
        {ok, Info} = mycelium_rotate:rotate_identity(#{key_dir => Tmp}),
        BackupDir = maps:get(backup_dir, Info),
        ?assertNotEqual(undefined, BackupDir),
        ?assertEqual(OrigPub, read(filename:join(BackupDir, "node.pub"))),
        ?assertEqual(OrigPriv, read(filename:join(BackupDir, "node.key"))),
        ?assertNotEqual(OrigPub, read(filename:join(Tmp, "node.pub"))),
        ?assertNotEqual(OrigPriv, read(filename:join(Tmp, "node.key")))
    end).

rotate_cert_fresh_test() ->
    with_tmp(fun(Tmp) ->
        {ok, Info} = mycelium_rotate:rotate_cert(#{cert_dir => Tmp}),
        ?assertEqual(undefined, maps:get(backup_dir, Info)),
        ?assertEqual(true, maps:get(restart_required, Info)),
        ?assert(filelib:is_regular(filename:join(Tmp, "node.crt"))),
        ?assert(filelib:is_regular(filename:join(Tmp, "node.key")))
    end).

rotate_cert_backs_up_test() ->
    with_tmp(fun(Tmp) ->
        {ok, _} = mycelium_rotate:rotate_cert(#{cert_dir => Tmp}),
        OrigCert = read(filename:join(Tmp, "node.crt")),
        OrigKey = read(filename:join(Tmp, "node.key")),
        %% Sleep one second so the backup timestamp directory differs.
        timer:sleep(1100),
        {ok, Info} = mycelium_rotate:rotate_cert(#{cert_dir => Tmp}),
        BackupDir = maps:get(backup_dir, Info),
        ?assertNotEqual(undefined, BackupDir),
        ?assertEqual(OrigCert, read(filename:join(BackupDir, "node.crt"))),
        ?assertEqual(OrigKey, read(filename:join(BackupDir, "node.key"))),
        ?assertNotEqual(OrigCert, read(filename:join(Tmp, "node.crt"))),
        ?assertNotEqual(OrigKey, read(filename:join(Tmp, "node.key")))
    end).

read(Path) ->
    {ok, B} = file:read_file(Path),
    B.
