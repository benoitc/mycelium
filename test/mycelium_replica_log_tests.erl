%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Unit tests for mycelium_replica_log: the WAL + snapshot store. These
%%% exercise recovery (snapshot + log replay), truncation, the repaired-log
%%% path after an unclean shutdown, idempotent replay, and delete. The HLC
%%% server is up so OR-Map add/remove/merge work.
-module(mycelium_replica_log_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").

%%====================================================================
%% Fixtures
%%====================================================================

setup() ->
    case whereis(mycelium_hlc) of
        undefined -> {ok, _} = mycelium_hlc:start_link();
        _ -> ok
    end,
    Dir = filename:join(
        "/tmp",
        "myc_replica_log_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    Dir.

cleanup(Dir) ->
    _ = catch disk_log:close(t),
    os:cmd("rm -rf " ++ Dir),
    ok.

with_dir(Fun) ->
    {setup, fun setup/0, fun cleanup/1, Fun}.

%% A live value entry for Key, authored by this node.
entry(Val) ->
    {value, Val, #{{node(), mycelium_hlc:now()} => true}}.

%%====================================================================
%% Tests
%%====================================================================

open_empty_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, Map} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:close(H),
        [?_assertEqual(#{}, Map)]
    end).

append_and_recover_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, _} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:append(H, #{a => entry(1)}),
        ok = mycelium_replica_log:append(H, #{b => entry(2)}),
        ok = mycelium_replica_log:sync(H),
        ok = mycelium_replica_log:close(H),
        %% Reopen: the log replays on top of the (empty) snapshot.
        {ok, H2, Map} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:close(H2),
        [
            ?_assertEqual({ok, 1}, mycelium_ormap:get(a, Map)),
            ?_assertEqual({ok, 2}, mycelium_ormap:get(b, Map))
        ]
    end).

snapshot_truncates_log_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, _} = mycelium_replica_log:open(t, Dir),
        Map0 = mycelium_ormap:add(a, 1, mycelium_ormap:new()),
        ok = mycelium_replica_log:append(H, Map0),
        %% Snapshot writes the full map and truncates the WAL.
        ok = mycelium_replica_log:snapshot(H, Map0),
        ok = mycelium_replica_log:append(H, #{b => entry(2)}),
        ok = mycelium_replica_log:close(H),
        %% The snapshot file exists and recovery sees both keys (snapshot a
        %% + post-snapshot logged b).
        {ok, H2, Map} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:close(H2),
        [
            ?_assert(filelib:is_file(filename:join(Dir, "t.snapshot"))),
            ?_assertEqual({ok, 1}, mycelium_ormap:get(a, Map)),
            ?_assertEqual({ok, 2}, mycelium_ormap:get(b, Map))
        ]
    end).

idempotent_replay_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, _} = mycelium_replica_log:open(t, Dir),
        E = entry(1),
        %% The same entry both in the snapshot and replayed from the log:
        %% merge is idempotent, so it appears once with value 1.
        ok = mycelium_replica_log:snapshot(H, #{a => E}),
        ok = mycelium_replica_log:append(H, #{a => E}),
        ok = mycelium_replica_log:close(H),
        {ok, H2, Map} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:close(H2),
        [
            ?_assertEqual({ok, 1}, mycelium_ormap:get(a, Map)),
            ?_assertEqual([a], mycelium_ormap:keys(Map))
        ]
    end).

tombstone_survives_recovery_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, _} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:append(H, #{a => entry(1)}),
        %% Tombstone minted from the HLC server, so it sorts strictly after
        %% the value (the remove wins on recovery).
        ok = mycelium_replica_log:append(H, #{a => {tombstone, mycelium_hlc:now()}}),
        ok = mycelium_replica_log:close(H),
        {ok, H2, Map} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:close(H2),
        %% The remove (tombstone, newer HLC) wins on recovery.
        [
            ?_assertEqual(not_found, mycelium_ormap:get(a, Map)),
            ?_assertMatch({ok, {tombstone, _}}, mycelium_ormap:get_entry(a, Map))
        ]
    end).

recover_after_torn_tail_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, _} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:append(H, #{a => entry(1)}),
        ok = mycelium_replica_log:sync(H),
        ok = mycelium_replica_log:append(H, #{b => entry(2)}),
        ok = mycelium_replica_log:sync(H),
        ok = mycelium_replica_log:close(H),
        %% Simulate a torn final write: chop bytes off the log tail. The
        %% reopen repairs (disk_log default repair) and the good prefix
        %% still recovers.
        LogFile = filename:join(Dir, "t.log"),
        {ok, Bin} = file:read_file(LogFile),
        ok = file:write_file(LogFile, binary:part(Bin, 0, byte_size(Bin) - 3)),
        {ok, H2, Map} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:close(H2),
        [?_assertEqual({ok, 1}, mycelium_ormap:get(a, Map))]
    end).

delete_removes_files_test_() ->
    with_dir(fun(Dir) ->
        {ok, H, _} = mycelium_replica_log:open(t, Dir),
        ok = mycelium_replica_log:snapshot(H, #{a => entry(1)}),
        ok = mycelium_replica_log:close(H),
        ok = mycelium_replica_log:delete(t, Dir),
        [
            ?_assertNot(filelib:is_file(filename:join(Dir, "t.snapshot"))),
            ?_assertNot(filelib:is_file(filename:join(Dir, "t.log")))
        ]
    end).

undefined_handle_is_noop_test() ->
    ?assertEqual(ok, mycelium_replica_log:append(undefined, #{a => entry(1)})),
    ?assertEqual(ok, mycelium_replica_log:sync(undefined)),
    ?assertEqual(ok, mycelium_replica_log:snapshot(undefined, #{})),
    ?assertEqual(ok, mycelium_replica_log:close(undefined)).
