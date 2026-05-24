%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Cert and identity rotation. Two flavours:
%%%
%%%   rotate_cert/0,1     -- regenerate the QUIC TLS cert/key pair.
%%%                          Takes effect on the next listener bind, so a
%%%                          node restart is required to pick the new
%%%                          credentials up. The atomic swap is still
%%%                          done in place so a crash mid-rotation does
%%%                          not leave torn files.
%%%
%%%   rotate_identity/0,1 -- regenerate the Ed25519 identity keypair.
%%%                          Takes effect on the next auth handshake
%%%                          because mycelium_dist_auth reads the keys
%%%                          off disk per attempt. No restart needed,
%%%                          but peers running in strict-trust mode will
%%%                          reject the new identity until re-pinned.
%%%
%%% Both flavours back the previous material up under a `backups/`
%%% subdirectory of the relevant key/cert directory, keyed by ISO-8601
%%% timestamp. The backup path is returned to the caller so a runbook
%%% can roll back deterministically.

-module(mycelium_rotate).

-export([
    rotate_cert/0, rotate_cert/1,
    rotate_identity/0, rotate_identity/1
]).

%%====================================================================
%% Public API
%%====================================================================

-type result() ::
    {ok, #{
        cert_file => file:filename(),
        key_file => file:filename(),
        backup_dir => file:filename() | undefined,
        restart_required => boolean()
    }}
    | {error, term()}.

-spec rotate_cert() -> result().
rotate_cert() ->
    rotate_cert(#{}).

-spec rotate_cert(map()) -> result().
rotate_cert(Opts) ->
    Dir = maps:get(
        cert_dir,
        Opts,
        application:get_env(
            mycelium,
            quic_cert_dir,
            "data/quic"
        )
    ),
    CertFile = filename:join(Dir, "node.crt"),
    KeyFile = filename:join(Dir, "node.key"),
    case backup_pair(Dir, [CertFile, KeyFile]) of
        {ok, BackupDir} ->
            case mycelium_quic_cert:generate_cert(Dir) of
                ok ->
                    logger:warning(
                        "mycelium_rotate: cert rotated in ~s; "
                        "node restart required for the listener "
                        "to load the new credentials. Backup at ~s",
                        [Dir, BackupDir]
                    ),
                    {ok, #{
                        cert_file => CertFile,
                        key_file => KeyFile,
                        backup_dir => BackupDir,
                        restart_required => true
                    }};
                Error ->
                    restore_backup(BackupDir, [CertFile, KeyFile]),
                    Error
            end;
        Error ->
            Error
    end.

-spec rotate_identity() -> result().
rotate_identity() ->
    rotate_identity(#{}).

-spec rotate_identity(map()) -> result().
rotate_identity(Opts) ->
    Dir = maps:get(
        key_dir,
        Opts,
        application:get_env(mycelium, auth_key_dir, "data/keys")
    ),
    PubFile = filename:join(Dir, "node.pub"),
    PrivFile = filename:join(Dir, "node.key"),
    case backup_pair(Dir, [PubFile, PrivFile]) of
        {ok, BackupDir} ->
            {PubKey, PrivKey} = mycelium_dist_auth:generate_keypair(),
            case mycelium_dist_auth:save_keypair(Dir, PubKey, PrivKey) of
                ok ->
                    Fp = mycelium_dist_keys:fingerprint(PubKey),
                    logger:warning(
                        "mycelium_rotate: identity rotated in ~s; "
                        "new fingerprint ~s. Peers running in "
                        "strict-trust mode must re-pin. Backup at ~s",
                        [Dir, hex(Fp), BackupDir]
                    ),
                    {ok, #{
                        cert_file => PubFile,
                        key_file => PrivFile,
                        backup_dir => BackupDir,
                        restart_required => false
                    }};
                Error ->
                    restore_backup(BackupDir, [PubFile, PrivFile]),
                    Error
            end;
        Error ->
            Error
    end.

%%====================================================================
%% Internal
%%====================================================================

%% Move every file in `Files' that currently exists into a fresh
%% timestamped subdirectory of `<Dir>/backups/'. Returns the backup
%% directory path (or `undefined' if no files existed). Files that
%% didn't exist are skipped without error.
backup_pair(Dir, Files) ->
    Existing = [F || F <- Files, filelib:is_regular(F)],
    case Existing of
        [] ->
            ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
            {ok, undefined};
        _ ->
            BackupDir = filename:join([Dir, "backups", timestamp_dir()]),
            case filelib:ensure_dir(filename:join(BackupDir, "dummy")) of
                ok ->
                    case copy_all(Existing, BackupDir) of
                        ok -> {ok, BackupDir};
                        Error -> Error
                    end;
                {error, Reason} ->
                    {error, {backup_mkdir_failed, Reason}}
            end
    end.

copy_all([], _BackupDir) ->
    ok;
copy_all([F | Rest], BackupDir) ->
    Target = filename:join(BackupDir, filename:basename(F)),
    case file:copy(F, Target) of
        {ok, _} -> copy_all(Rest, BackupDir);
        {error, Reason} -> {error, {backup_copy_failed, F, Reason}}
    end.

%% Best-effort restore. Called when generation fails after the backup
%% completed but before the new file is in place. Crash-safety still
%% relies on the operator inspecting the backup dir.
restore_backup(undefined, _Files) ->
    ok;
restore_backup(BackupDir, Files) ->
    lists:foreach(
        fun(F) ->
            Source = filename:join(BackupDir, filename:basename(F)),
            case filelib:is_regular(Source) of
                true ->
                    _ = file:copy(Source, F),
                    ok;
                false ->
                    ok
            end
        end,
        Files
    ),
    ok.

timestamp_dir() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:universal_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w~2..0w~2..0wT~2..0w~2..0w~2..0wZ",
            [Y, M, D, H, Mi, S]
        )
    ).

hex(Bin) when is_binary(Bin) ->
    [io_lib:format("~2.16.0b", [B]) || <<B>> <= Bin].
