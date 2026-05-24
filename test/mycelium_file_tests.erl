%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_file:write_secure/2.

-module(mycelium_file_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

tmp_path() ->
    Dir = filename:join([
        "/tmp",
        "mycelium_file_tests",
        integer_to_list(erlang:unique_integer([positive]))
    ]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    {Dir, filename:join(Dir, "secret")}.

cleanup(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

write_secure_creates_file_with_0600_test() ->
    {Dir, Path} = tmp_path(),
    try
        ok = mycelium_file:write_secure(Path, <<"hello">>),
        {ok, FI} = file:read_file_info(Path),
        Mode = FI#file_info.mode band 8#777,
        ?assertEqual(8#600, Mode),
        {ok, Data} = file:read_file(Path),
        ?assertEqual(<<"hello">>, Data)
    after
        cleanup(Dir)
    end.

write_secure_no_tmp_after_success_test() ->
    {Dir, Path} = tmp_path(),
    try
        ok = mycelium_file:write_secure(Path, <<"abc">>),
        ?assertNot(filelib:is_regular(Path ++ ".tmp"))
    after
        cleanup(Dir)
    end.

write_secure_overwrites_existing_test() ->
    {Dir, Path} = tmp_path(),
    try
        ok = mycelium_file:write_secure(Path, <<"first">>),
        ok = mycelium_file:write_secure(Path, <<"second">>),
        {ok, Data} = file:read_file(Path),
        ?assertEqual(<<"second">>, Data)
    after
        cleanup(Dir)
    end.

write_secure_returns_error_on_bad_path_test() ->
    %% Parent dir does not exist; open should fail.
    BadPath = "/nonexistent/path/that/does/not/exist/secret",
    ?assertMatch({error, _}, mycelium_file:write_secure(BadPath, <<"x">>)).
