%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Mycelium proto_dist alt-dist module.
%%%
%%% Boot a mycelium node with just:
%%%
%%%   -proto_dist mycelium
%%%   -epmd_module mycelium_epmd
%%%   -start_epmd false
%%%
%%% Everything else (TLS cert generation, auth callback wiring,
%%% discovery module, on-disk cert/key paths) is filled in here
%%% before delegating to upstream `quic_dist'. Values the user sets
%%% explicitly under `{quic, [{dist, [...]}]}' or via `-quic_dist_*'
%%% init args are preserved.
%%%
%%% The shim is intentionally tiny: it owns the listen-time defaults
%%% and forwards the other alt-dist callbacks straight through. That
%%% keeps upstream `?MODULE'-relative spawns (do_setup, acceptor_loop,
%%% accept_connection inline fun) resolving to the right module.

-module(mycelium_dist).

%% Distribution module callbacks (the alt-dist contract).
-export([
    listen/1,
    listen/2,
    accept/1,
    accept_connection/5,
    setup/5,
    close/1,
    select/1,
    address/0,
    is_node_name/1,
    project_defaults/0,
    validate_auth_config/1
]).

%%====================================================================
%% Distribution Module Callbacks
%%====================================================================

listen(Name) ->
    listen(Name, #{}).

listen(Name, ExtraOpts) ->
    ok = ensure_modules_loaded(),
    ok = project_init_args(),
    case ensure_cert() of
        ok ->
            ok = project_defaults(),
            ok = project_listen_port(),
            %% Snapshot the SHA-256 of the effective listener cert now
            %% that quic.dist is final, for the auth channel binding (H1).
            %% This is exactly the cert quic_dist:listen serves below.
            ok = mycelium_dist_auth:cache_server_cert_binding(),
            quic_dist:listen(Name, ExtraOpts);
        {error, _} = Err ->
            Err
    end.

accept(Listener) ->
    quic_dist:accept(Listener).

accept_connection(AcceptPid, DistCtrl, MyNode, Allowed, SetupTime) ->
    quic_dist:accept_connection(AcceptPid, DistCtrl, MyNode, Allowed, SetupTime).

setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    %% Replicate upstream quic_dist:setup/5 but stash the dialed Node
    %% in the setup process's dictionary. mycelium_dist_auth_callback
    %% reads it back to gate the AUTH_OK short-circuit on the client's
    %% own cookie_only_nodes whitelist.
    Kernel = self(),
    spawn_opt(
        fun() ->
            erlang:put(mycelium_dial_target, Node),
            quic_dist:do_setup(
                Kernel, Node, Type, MyNode, LongOrShortNames, SetupTime
            )
        end,
        [link, {priority, max}]
    ).

close(Listener) ->
    quic_dist:close(Listener).

select(Node) ->
    quic_dist:select(Node).

address() ->
    quic_dist:address().

is_node_name(Node) ->
    quic_dist:is_node_name(Node).

%%====================================================================
%% Internal: early-boot wiring
%%====================================================================

%% Modules referenced by quic_dist at runtime that may not be auto-
%% loaded yet during early boot (proto_dist listen/1 runs before the
%% application controller starts mycelium).
ensure_modules_loaded() ->
    lists:foreach(
        fun(M) -> _ = code:ensure_loaded(M) end,
        [
            public_key,
            mycelium_quic_cert,
            mycelium_discovery,
            mycelium_discovery_static,
            mycelium_discovery_file,
            mycelium_discovery_dns,
            mycelium_dist_auth_callback,
            mycelium_dist_auth_stream,
            mycelium_dist_auth
        ]
    ),
    ok.

%% Lazily materialise the QUIC TLS cert/key if they aren't on disk
%% yet. quic_dist:load_credentials runs straight after this and would
%% otherwise fail with the less direct {credentials, no_credentials}.
%% Propagate the real reason so listen/2 short-circuits here.
ensure_cert() ->
    CertDir = cert_dir(),
    case mycelium_quic_cert:ensure_cert(CertDir) of
        ok ->
            ok;
        {error, Reason} ->
            logger:error("mycelium_dist: cert ensure failed: ~p", [Reason]),
            {error, {cert_ensure_failed, Reason}}
    end.

%% Resolve the cert dir at listen time. App env may not be set yet,
%% so check init args first, fall through to app env, then default.
cert_dir() ->
    case init:get_argument(mycelium_dist_cert_dir) of
        {ok, [[CD] | _]} -> CD;
        _ -> application:get_env(mycelium, quic_cert_dir, "data/quic")
    end.

%% Project mycelium defaults into the {quic, dist, ...} app env that
%% upstream quic_dist:load_config/0 reads. User-supplied values win:
%% we only fill keys that are absent.
%%
%% Loads the `quic' app before reading its env: sys.config entries
%% for unloaded apps are pending until load, so without this the
%% `{quic, [{dist, [...]}]}' the operator set in sys.config is
%% invisible at proto_dist-boot time.
project_defaults() ->
    _ = application:load(quic),
    _ = application:load(mycelium),
    User = application:get_env(quic, dist, []),
    Defaults = build_defaults(),
    Merged = merge_defaults(User, Defaults),
    application:set_env(quic, dist, Merged),
    validate_auth_config(Merged),
    ok.

%% Refuse to boot if mycelium auth is enabled but the projected
%% quic.dist config has no auth_callback (or has it explicitly set
%% to undefined). The PR-1 default flip means an unset auth_enabled
%% is `true', so a user who silently overrides auth_callback would
%% otherwise ship unauthenticated peers without warning.
validate_auth_config(QuicDist) ->
    AuthEnabled = application:get_env(mycelium, auth_enabled, true),
    Callback = proplists:get_value(auth_callback, QuicDist),
    case AuthEnabled andalso (Callback =:= undefined) of
        true ->
            erlang:error({mycelium_dist, auth_enabled_without_callback});
        false ->
            ok
    end,
    %% Ed25519 off leaves only the dist cookie over an unauthenticated TLS
    %% channel - warn loudly so it is never a silent default.
    case AuthEnabled of
        false ->
            logger:warning(
                "mycelium: auth_enabled=false - Ed25519 peer authentication is "
                "OFF. Peers are gated by the dist cookie only, with no "
                "protection against an active MITM."
            );
        true ->
            ok
    end.

build_defaults() ->
    CertDir = cert_dir(),
    CertFile = filename:join(CertDir, "node.crt"),
    KeyFile = filename:join(CertDir, "node.key"),
    AuthTimeout = application:get_env(mycelium, auth_handshake_timeout, 10000),
    %% register_with_epmd intentionally left at upstream default
    %% (false). mycelium_app:start/2 publishes the node into the
    %% discovery chain itself once sys.config envs are live, using
    %% the full atom node name; the listen-time path passes a bare
    %% name string which the file backend doesn't accept.
    [
        {auth_callback, {mycelium_dist_auth_callback, authenticate}},
        {auth_handshake_timeout, AuthTimeout},
        {discovery_module, mycelium_discovery},
        {cert_file, list_to_binary(CertFile)},
        {key_file, list_to_binary(KeyFile)}
    ].

%% Translate `-mycelium_dist_*' init args to their `quic.dist'
%% equivalents so users never have to type the upstream knob names.
%% Recognised today: -mycelium_dist_port N.
project_init_args() ->
    case init:get_argument(mycelium_dist_port) of
        {ok, [[PortStr] | _]} ->
            case catch list_to_integer(PortStr) of
                P when is_integer(P), P >= 0 ->
                    application:set_env(quic, dist_port, P);
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    ok.

%% Project mycelium.listen_port -> quic.dist_port so users keep a
%% single {mycelium, [{listen_port, ...}]} knob. Upstream's
%% -quic_dist_port init arg still wins over this (it's read straight
%% from init args, not the app env).
project_listen_port() ->
    case application:get_env(quic, dist_port) of
        {ok, _} ->
            ok;
        undefined ->
            case application:get_env(mycelium, listen_port) of
                {ok, Port} when is_integer(Port) ->
                    application:set_env(quic, dist_port, Port);
                _ ->
                    ok
            end
    end,
    ok.

merge_defaults(User, Defaults) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            case proplists:is_defined(K, Acc) of
                true -> Acc;
                false -> [{K, V} | Acc]
            end
        end,
        User,
        Defaults
    ).
