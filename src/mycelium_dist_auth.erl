%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Pure helpers for the Ed25519 dist auth handshake. The live
%%% handshake itself runs in `mycelium_dist_auth_stream' over a pair
%%% of QUIC unidirectional streams; this module owns key I/O, the
%%% challenge build/verify primitives, and the cookie_only_nodes
%%% policy.
%%%
-module(mycelium_dist_auth).

%% Public API
-export([
    init/0,
    ensure_keypair/0,
    get_public_key/0,
    get_private_key/0,
    is_cookie_only_allowed/1
]).

%% Challenge-response primitives used by the auth stream module.
-export([
    create_challenge/0,
    sign_challenge/3,
    verify_response/4,
    validate_peer_ts/1
]).

%% TLS channel binding (H1): SHA-256 of the listener's TLS cert.
-export([
    cache_server_cert_binding/0,
    server_cert_binding/0
]).

%% Internal exports for testing.
-export([
    generate_keypair/0,
    load_keypair/1,
    save_keypair/3
]).

-include("mycelium.hrl").

-define(NONCE_SIZE, 32).
-define(SIGNATURE_SIZE, 64).
-define(PUBLIC_KEY_SIZE, 32).
-define(PRIVATE_KEY_SIZE, 32).
-define(TIMESTAMP_WINDOW_MS, 30000).
%% SHA-256 of the listener TLS cert, mixed into every signed handshake
%% message as the channel binding.
-define(BINDING_SIZE, 32).
-define(SERVER_CERT_BINDING_KEY, {?MODULE, server_cert_binding}).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Initialize the authentication subsystem.
-spec init() -> ok | {error, term()}.
init() ->
    case ensure_keypair() of
        ok -> ok;
        Error -> Error
    end.

%% @doc Ensure a node keypair exists, generating one if needed.
-spec ensure_keypair() -> ok | {error, term()}.
ensure_keypair() ->
    KeyDir = get_key_dir(),
    case filelib:ensure_dir(filename:join(KeyDir, "dummy")) of
        ok ->
            PrivKeyFile = filename:join(KeyDir, "node.key"),
            PubKeyFile  = filename:join(KeyDir, "node.pub"),
            case filelib:is_file(PrivKeyFile)
                 andalso filelib:is_file(PubKeyFile) of
                true ->
                    case load_keypair(KeyDir) of
                        {ok, _PubKey, _PrivKey} -> ok;
                        Error -> Error
                    end;
                false ->
                    {PubKey, PrivKey} = generate_keypair(),
                    save_keypair(KeyDir, PubKey, PrivKey)
            end;
        {error, Reason} ->
            {error, {mkdir_failed, Reason}}
    end.

%% @doc Read the node's Ed25519 public key from disk.
-spec get_public_key() -> {ok, binary()} | {error, term()}.
get_public_key() ->
    KeyDir = get_key_dir(),
    PubKeyFile = filename:join(KeyDir, "node.pub"),
    file:read_file(PubKeyFile).

%% @doc Read the node's Ed25519 private key from disk.
-spec get_private_key() -> {ok, binary()} | {error, term()}.
get_private_key() ->
    KeyDir = get_key_dir(),
    PrivKeyFile = filename:join(KeyDir, "node.key"),
    file:read_file(PrivKeyFile).

%%====================================================================
%% Challenge-Response Protocol
%%====================================================================

%% @doc Build a fresh challenge. The wall-clock timestamp is what
%% the peer signs and what is compared cross-host. The monotonic
%% start lets the responder measure the handshake duration without
%% an NTP-induced spurious failure.
-spec create_challenge() ->
    {Nonce :: binary(), WallTs :: integer(), MonoStart :: integer()}.
create_challenge() ->
    Nonce = crypto:strong_rand_bytes(?NONCE_SIZE),
    WallTs = erlang:system_time(millisecond),
    MonoStart = erlang:monotonic_time(millisecond),
    {Nonce, WallTs, MonoStart}.

%% @doc Sign a challenge built locally. The message format
%% `Nonce | Timestamp | OwnPubKey | Binding' binds the signature to the
%% signer's own identity and to the QUIC TLS channel: `Binding' is the
%% SHA-256 of the server's TLS certificate (H1). A relayed signature
%% computed over a different channel's cert no longer verifies.
-spec sign_challenge(binary(), integer(), binary()) ->
    {ok, binary()} | {error, term()}.
sign_challenge(Nonce, Timestamp, Binding)
  when byte_size(Nonce) =:= ?NONCE_SIZE, byte_size(Binding) =:= ?BINDING_SIZE ->
    case get_private_key() of
        {ok, PrivKey} ->
            case get_public_key() of
                {ok, PubKey} ->
                    Message =
                        <<Nonce/binary, Timestamp:64/big,
                          PubKey/binary, Binding/binary>>,
                    Sig = crypto:sign(eddsa, none, Message, [PrivKey, ed25519]),
                    {ok, Sig};
                Error -> Error
            end;
        Error -> Error
    end;
sign_challenge(_, _, _) ->
    {error, invalid_binding}.

%% @doc Verify a peer's response. Rebuilds the signed message as
%% `Nonce | WallTs | ResponderPubKey' and checks the signature. The
%% handshake-elapsed window is measured against the monotonic clock
%% captured by `create_challenge/0', so an NTP step during the
%% handshake cannot spuriously fail (or pass) the duration check.
-spec verify_response(
    binary(), binary(),
    {binary(), integer(), integer()},
    binary()
) -> boolean().
verify_response(Signature, ResponderPubKey, {Nonce, WallTs, MonoStart}, Binding)
  when byte_size(Signature)       =:= ?SIGNATURE_SIZE,
       byte_size(ResponderPubKey) =:= ?PUBLIC_KEY_SIZE,
       byte_size(Nonce)           =:= ?NONCE_SIZE,
       byte_size(Binding)         =:= ?BINDING_SIZE,
       is_integer(WallTs),
       is_integer(MonoStart) ->
    Elapsed = erlang:monotonic_time(millisecond) - MonoStart,
    case Elapsed =< get_timestamp_window() of
        true ->
            Message =
                <<Nonce/binary, WallTs:64/big,
                  ResponderPubKey/binary, Binding/binary>>,
            crypto:verify(
                eddsa, none, Message, Signature, [ResponderPubKey, ed25519]
            );
        false ->
            false
    end;
verify_response(_, _, _, _) ->
    false.

%% @doc Reject a peer-supplied wall timestamp that is too far from
%% local wall time. Defense-in-depth against an attacker replaying
%% an old peer CHALLENGE: the nonce alone protects against replay
%% within a single signing session, this widens that to gross
%% clock skew.
-spec validate_peer_ts(integer()) -> ok | {error, peer_ts_skew}.
validate_peer_ts(PeerTs) when is_integer(PeerTs) ->
    Now = erlang:system_time(millisecond),
    case abs(Now - PeerTs) =< 2 * get_timestamp_window() of
        true  -> ok;
        false -> {error, peer_ts_skew}
    end;
validate_peer_ts(_) ->
    {error, peer_ts_skew}.

%%====================================================================
%% Key Generation
%%====================================================================

%% @doc Generate a fresh Ed25519 keypair.
-spec generate_keypair() -> {PublicKey :: binary(), PrivateKey :: binary()}.
generate_keypair() ->
    {PubKey, PrivKey} = crypto:generate_key(eddsa, ed25519),
    {PubKey, PrivKey}.

%% @doc Load a keypair from disk. Verifies that the public key on
%% disk is the one derived from the private key: a crash between the
%% two file renames in save_keypair/3 could otherwise leave a
%% mismatched pair, and load would silently return inconsistent
%% material.
-spec load_keypair(string()) ->
    {ok, binary(), binary()} | {error, term()}.
load_keypair(KeyDir) ->
    PrivKeyFile = filename:join(KeyDir, "node.key"),
    PubKeyFile  = filename:join(KeyDir, "node.pub"),
    case {file:read_file(PrivKeyFile), file:read_file(PubKeyFile)} of
        {{ok, PrivKey}, {ok, PubKey}}
          when byte_size(PrivKey) =:= ?PRIVATE_KEY_SIZE,
               byte_size(PubKey)  =:= ?PUBLIC_KEY_SIZE ->
            case derived_pubkey(PrivKey) of
                PubKey ->
                    {ok, PubKey, PrivKey};
                _Other ->
                    {error, keypair_mismatch}
            end;
        {{ok, _}, {ok, _}} ->
            {error, invalid_key_size};
        {{error, Reason}, _} ->
            {error, {read_privkey_failed, Reason}};
        {_, {error, Reason}} ->
            {error, {read_pubkey_failed, Reason}}
    end.

%% @doc Save a keypair to disk atomically. Each file goes through the
%% mycelium_file:write_secure/2 chmod-before-write+rename helper, so
%% neither key is ever world-readable mid-write. Two separate renames
%% are not collectively atomic, but load_keypair/1 detects the
%% mismatched-pair window and refuses to load.
-spec save_keypair(string(), binary(), binary()) -> ok | {error, term()}.
save_keypair(KeyDir, PubKey, PrivKey) ->
    PrivKeyFile = filename:join(KeyDir, "node.key"),
    PubKeyFile  = filename:join(KeyDir, "node.pub"),
    case mycelium_file:write_secure(PrivKeyFile, PrivKey) of
        ok ->
            case mycelium_file:write_secure(PubKeyFile, PubKey) of
                ok -> ok;
                {error, Reason} -> {error, {write_pubkey_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {write_privkey_failed, Reason}}
    end.

%% Derive the Ed25519 public key from a private key. Used by
%% load_keypair/1 to detect a torn rotation.
derived_pubkey(PrivKey) ->
    try
        case crypto:generate_key(eddsa, ed25519, PrivKey) of
            {Pub, _Priv} when byte_size(Pub) =:= ?PUBLIC_KEY_SIZE ->
                Pub;
            _ ->
                undefined
        end
    catch _:_ ->
        undefined
    end.

%%====================================================================
%% C-Node Whitelist
%%====================================================================

%% @doc Check if a node may bypass the Ed25519 handshake on the
%% strength of the Erlang dist cookie alone (c-nodes, legacy tools).
%% Accepts a node atom or a (peer-supplied) name binary. The binary
%% form is matched without atomising, so the cookie-only check that
%% runs before Ed25519 verification cannot mint atoms.
-spec is_cookie_only_allowed(node() | binary()) -> boolean().
is_cookie_only_allowed(Node) ->
    case node_to_list(Node) of
        undefined ->
            false;
        NodeStr ->
            Whitelist = application:get_env(mycelium, cookie_only_nodes, []),
            lists:any(fun(P) -> match_node_pattern(P, NodeStr) end, Whitelist)
    end.

node_to_list(Node) when is_atom(Node)   -> atom_to_list(Node);
node_to_list(Node) when is_binary(Node) -> binary_to_list(Node);
node_to_list(_)                         -> undefined.

%% @doc Match a node name string against a pattern that may contain
%% wildcards. Patterns support `*' for any name or any host:
%%   'cnode@localhost'  - exact match
%%   'monitor@*'        - any host
%%   '*@trusted.local'  - any name on specific host
-spec match_node_pattern(atom(), string()) -> boolean().
match_node_pattern(Pattern, NodeStr) when is_atom(Pattern), is_list(NodeStr) ->
    PatternStr = atom_to_list(Pattern),
    case {string:split(PatternStr, "@"), string:split(NodeStr, "@")} of
        {[PName, PHost], [NName, NHost]} ->
            match_part(PName, NName) andalso match_part(PHost, NHost);
        _ -> false
    end;
match_node_pattern(_, _) -> false.

-spec match_part(string(), string()) -> boolean().
match_part("*", _) -> true;
match_part(P,   N) -> P =:= N.

%%====================================================================
%% TLS channel binding (H1)
%%====================================================================

%% @doc Cache the SHA-256 of the effective listener TLS certificate, for
%% use as the handshake channel binding. Called once at listen time,
%% after the `quic.dist' config is projected, so the hash matches exactly
%% the cert `quic_dist' serves. Reads `cert_file' from the merged config
%% (honours `-mycelium_dist_cert_dir' and a user-supplied cert), not the
%% default path. Best-effort: logs and continues on failure (the auth
%% path then fails closed when it cannot resolve the binding).
-spec cache_server_cert_binding() -> ok.
cache_server_cert_binding() ->
    case compute_server_cert_binding() of
        {ok, Hash} ->
            persistent_term:put(?SERVER_CERT_BINDING_KEY, Hash);
        {error, Reason} ->
            logger:error("mycelium_dist_auth: cannot hash listener cert for "
                         "channel binding: ~p", [Reason])
    end,
    ok.

%% @doc The server-side channel binding: SHA-256 of the listener cert.
%% Returns the cached value, recomputing from the effective config if the
%% cache is cold (e.g. the auth path runs before listen cached it).
-spec server_cert_binding() -> {ok, binary()} | {error, term()}.
server_cert_binding() ->
    case persistent_term:get(?SERVER_CERT_BINDING_KEY, undefined) of
        undefined -> compute_server_cert_binding();
        Hash      -> {ok, Hash}
    end.

compute_server_cert_binding() ->
    DistOpts = application:get_env(quic, dist, []),
    case proplists:get_value(cert_file, DistOpts) of
        undefined -> {error, no_cert_file};
        CertFile  -> cert_file_hash(CertFile)
    end.

cert_file_hash(CertFile) ->
    case file:read_file(CertFile) of
        {ok, Pem} ->
            case first_cert_der(Pem) of
                {ok, Der}      -> {ok, crypto:hash(sha256, Der)};
                {error, _} = E -> E
            end;
        {error, Reason} ->
            {error, {read_cert_failed, Reason}}
    end.

first_cert_der(Pem) ->
    try [Der || {'Certificate', Der, not_encrypted} <- public_key:pem_decode(Pem)] of
        [Der | _] -> {ok, Der};
        []        -> {error, no_certificate_in_pem}
    catch _:Reason ->
        {error, {pem_decode_failed, Reason}}
    end.

%%====================================================================
%% Configuration Helpers
%%====================================================================

get_key_dir() ->
    application:get_env(mycelium, auth_key_dir, "data/keys").

get_timestamp_window() ->
    application:get_env(
        mycelium, auth_timestamp_window, ?TIMESTAMP_WINDOW_MS
    ).
