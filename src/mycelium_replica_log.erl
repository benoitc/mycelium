%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Disk persistence for a gossiped OR-Map: a write-ahead log plus periodic
%%% snapshots, so the state survives a full-cluster restart (every node
%%% down, then up). Used by `mycelium_reminder' (always) and `mycelium_map'
%%% (opt-in); each holds the full replicated OR-Map, so every node persists
%%% its own copy and the cluster reloads + re-converges on boot.
%%%
%%% Layout under `Dir' (one store per Name):
%%%   <Name>.log       - disk_log (halt, internal), one appended delta per
%%%                      mutation: a `#{Key => entry}' OR-Map fragment.
%%%   <Name>.snapshot  - the full OR-Map, written atomically (temp+rename).
%%%
%%% Recovery merges the logged deltas on top of the snapshot. Because
%%% `mycelium_ormap:merge/2' is commutative, associative and idempotent,
%%% replay is order-independent and safe even when a delta is also already
%%% in the snapshot (e.g. after a crash between snapshot and truncate).
%%% A snapshot writes the full map FIRST, then truncates the log, so a
%%% crash in between only replays already-applied deltas.
%%%
%%% Recovery validates every snapshot/logged entry with
%%% `mycelium_crdt_wire:accept/2' (wrapper-shape + non-map guard), so a
%%% corrupt or wrong-shaped file cannot crash `merge/2' or `absorb_clock/1'
%%% at boot; unrecognised entries are dropped, not fatal.
%%%
%%% Callers MUST `mycelium_ormap:absorb_clock/1' the recovered map before
%%% using it, so the restarted node's HLC advances past every persisted
%%% dot/version and cannot mint a timestamp behind it.
%%%
%%% A `handle()' of `undefined' makes every operation a no-op, so an
%%% opt-out caller (a map with `persist => false') runs the same code path
%%% with persistence disabled.
-module(mycelium_replica_log).

-export([open/2, append/2, sync/1, snapshot/2, close/1, delete/2]).

-export_type([handle/0]).

-type handle() :: undefined | #{name := atom(),
                                log := atom(),
                                snapshot := file:filename_all()}.

%%====================================================================
%% API
%%====================================================================

%% @doc Open (creating if needed) the store for `Name' under `Dir' and
%% return the recovered OR-Map (snapshot with the logged deltas merged on
%% top). The caller must `mycelium_ormap:absorb_clock/1' the result.
-spec open(atom(), file:filename_all()) ->
    {ok, handle(), mycelium_ormap:ormap()} | {error, term()}.
open(Name, Dir) ->
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok ->
            Base = read_snapshot(snapshot_path(Dir, Name)),
            case open_log(Name, log_path(Dir, Name)) of
                {ok, Log} ->
                    Map = replay(Log, Base),
                    Handle = #{name => Name, log => Log,
                               snapshot => snapshot_path(Dir, Name)},
                    {ok, Handle, Map};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Append a delta (the concrete OR-Map entries just minted, so replay
%% never re-mints an HLC). A non-map or empty delta is ignored.
-spec append(handle(), mycelium_ormap:ormap()) -> ok.
append(undefined, _Delta) ->
    ok;
append(#{log := Log}, Delta) when is_map(Delta), map_size(Delta) > 0 ->
    disk_log:log(Log, Delta);
append(_Handle, _Delta) ->
    ok.

%% @doc Flush the log to disk (persisted-before-ack for user writes).
-spec sync(handle()) -> ok.
sync(undefined) ->
    ok;
sync(#{log := Log}) ->
    disk_log:sync(Log).

%% @doc Write the full map to the snapshot atomically, then truncate the
%% log. Bounds the log and purges GC'd tombstones from disk.
-spec snapshot(handle(), mycelium_ormap:ormap()) -> ok | {error, term()}.
snapshot(undefined, _Map) ->
    ok;
snapshot(#{log := Log, snapshot := SnapPath}, Map) ->
    case mycelium_file:write_secure(SnapPath, term_to_binary(Map)) of
        ok              -> disk_log:truncate(Log);
        {error, _} = Err -> Err
    end.

%% @doc Close the log cleanly (called from the owner's `terminate/2').
-spec close(handle()) -> ok.
close(undefined) ->
    ok;
close(#{log := Log}) ->
    _ = disk_log:close(Log),
    ok.

%% @doc Remove the on-disk files for `Name' under `Dir'. Idempotent;
%% used by `delete_map/1' so a re-created map does not reload stale data.
-spec delete(atom(), file:filename_all()) -> ok.
delete(Name, Dir) ->
    _ = catch disk_log:close(Name),
    _ = file:delete(snapshot_path(Dir, Name)),
    _ = file:delete(log_path(Dir, Name)),
    ok.

%%====================================================================
%% Internal
%%====================================================================

open_log(Name, LogPath) ->
    case disk_log:open([{name, Name}, {file, LogPath},
                        {type, halt}, {format, internal}]) of
        {ok, Name} ->
            {ok, Name};
        {repaired, Name, {recovered, _R}, {badbytes, _B}} ->
            %% Unclean prior shutdown: the log was truncated to the last
            %% good term. The snapshot covers the rest; carry on.
            {ok, Name};
        {error, _} = Err ->
            Err
    end.

replay(Log, Base) ->
    replay(Log, start, Base).

replay(Log, Cont, Acc) ->
    case disk_log:chunk(Log, Cont) of
        eof ->
            Acc;
        {error, _Reason} ->
            %% Best-effort: stop at the first unreadable chunk, keep what
            %% we merged so far (the snapshot is still authoritative).
            Acc;
        {Cont2, Terms} ->
            replay(Log, Cont2, apply_terms(Terms, Acc));
        {Cont2, Terms, _Bad} ->
            replay(Log, Cont2, apply_terms(Terms, Acc))
    end.

apply_terms(Terms, Acc) ->
    lists:foldl(fun(Delta, M) -> mycelium_ormap:merge(M, validate(Delta)) end,
                Acc, Terms).

read_snapshot(SnapPath) ->
    case file:read_file(SnapPath) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                Term -> validate(Term)
            catch
                _:_ -> mycelium_ormap:new()
            end;
        {error, _} ->
            mycelium_ormap:new()
    end.

%% Keep only OR-Map-shaped entries (wrapper validation via mycelium_crdt_wire,
%% which also guards a non-map argument). A corrupt or wrong-shaped file then
%% cannot crash merge/2 or absorb_clock/1 at boot.
validate(Term) ->
    mycelium_crdt_wire:accept(Term, fun(_) -> true end).

snapshot_path(Dir, Name) ->
    filename:join(Dir, atom_to_list(Name) ++ ".snapshot").

log_path(Dir, Name) ->
    filename:join(Dir, atom_to_list(Name) ++ ".log").
