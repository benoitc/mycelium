%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_dist_protocol).

%% Wire protocol encoding for Ed25519 distribution authentication
%% Message format: `<<Type:8, Length:16/big, Payload/binary>>'

-export([
    encode_hello/2,
    encode_challenge/2,
    encode_response/1,
    encode_ok/0,
    encode_fail/1,
    encode_key_exchange/1,
    decode/1,
    validate_node_name/1
]).

%% Protocol version. v2 binds each signature to the QUIC TLS channel
%% (the signed message gains a 32-byte cert-hash binding); a v1 peer is
%% rejected at HELLO with {unsupported_version, 1} rather than failing
%% later with an opaque signature error.
-define(PROTOCOL_VERSION, 2).

%% Message types
-define(AUTH_HELLO, 1).
-define(AUTH_CHALLENGE, 2).
-define(AUTH_RESPONSE, 3).
-define(AUTH_OK, 4).
-define(AUTH_FAIL, 5).
-define(AUTH_KEY_EXCHANGE, 6).

%% Key sizes
-define(PUBLIC_KEY_SIZE, 32).
-define(X25519_KEY_SIZE, 32).
-define(NONCE_SIZE, 32).
-define(SIGNATURE_SIZE, 64).

%% Hard cap on a peer-claimed node name. Erlang nodes are usually
%% well under 100 bytes; 255 is generous.
-define(MAX_NODE_NAME_LEN, 255).

%%====================================================================
%% Encoding Functions
%%====================================================================

%% @doc Encode AUTH_HELLO message
%% Format: `<<Type:8, Version:8, NodeNameLen:16/big, NodeName/binary, PubKey:32/binary>>'
-spec encode_hello(node(), binary()) -> binary().
encode_hello(NodeName, PubKey) when byte_size(PubKey) =:= ?PUBLIC_KEY_SIZE ->
    NodeBin = atom_to_binary(NodeName, utf8),
    NodeLen = byte_size(NodeBin),
    Payload = <<?PROTOCOL_VERSION:8, NodeLen:16/big, NodeBin/binary, PubKey/binary>>,
    <<?AUTH_HELLO:8, Payload/binary>>.

%% @doc Encode AUTH_CHALLENGE message
%% Format: `<<Type:8, Nonce:32/binary, Timestamp:64/big>>'
-spec encode_challenge(binary(), integer()) -> binary().
encode_challenge(Nonce, Timestamp) when byte_size(Nonce) =:= ?NONCE_SIZE ->
    Payload = <<Nonce/binary, Timestamp:64/big>>,
    <<?AUTH_CHALLENGE:8, Payload/binary>>.

%% @doc Encode AUTH_RESPONSE message
%% Format: `<<Type:8, Signature:64/binary>>'
-spec encode_response(binary()) -> binary().
encode_response(Signature) when byte_size(Signature) =:= ?SIGNATURE_SIZE ->
    <<?AUTH_RESPONSE:8, Signature/binary>>.

%% @doc Encode AUTH_OK message
%% Format: `<<Type:8>>'
-spec encode_ok() -> binary().
encode_ok() ->
    <<?AUTH_OK:8>>.

%% @doc Encode AUTH_FAIL message
%% Format: `<<Type:8, ReasonLen:16/big, Reason/binary>>'
-spec encode_fail(binary()) -> binary().
encode_fail(Reason) when is_binary(Reason) ->
    ReasonLen = byte_size(Reason),
    <<?AUTH_FAIL:8, ReasonLen:16/big, Reason/binary>>.

%% @doc Encode AUTH_KEY_EXCHANGE message
%% Format: `<<Type:8, EphemeralPubKey:32/binary>>'
-spec encode_key_exchange(binary()) -> binary().
encode_key_exchange(EphemeralPubKey) when byte_size(EphemeralPubKey) =:= ?X25519_KEY_SIZE ->
    <<?AUTH_KEY_EXCHANGE:8, EphemeralPubKey/binary>>.

%%====================================================================
%% Decoding Functions
%%====================================================================

%% @doc Decode an authentication message
%% The HELLO node name is returned as a *validated binary*, not an atom.
%% Atomising peer-controlled bytes here would let an unauthenticated peer
%% flood the (never-GC'd) atom table; the caller mints the atom only after
%% the Ed25519 signature is verified. See mycelium_dist_auth_stream.
-spec decode(binary()) ->
    {hello, binary(), binary()} |
    {challenge, binary(), integer()} |
    {response, binary()} |
    {key_exchange, binary()} |
    ok |
    {fail, binary()} |
    {error, term()}.
decode(<<?AUTH_HELLO:8, ?PROTOCOL_VERSION:8, NodeLen:16/big, Rest/binary>>) ->
    case Rest of
        <<NodeBin:NodeLen/binary, PubKey:?PUBLIC_KEY_SIZE/binary>> ->
            case validate_node_name(NodeBin) of
                ok             -> {hello, NodeBin, PubKey};
                {error, _} = E -> E
            end;
        _ ->
            {error, invalid_hello_payload}
    end;
decode(<<?AUTH_HELLO:8, Version:8, _/binary>>) ->
    {error, {unsupported_version, Version}};

decode(<<?AUTH_CHALLENGE:8, Nonce:?NONCE_SIZE/binary, Timestamp:64/big>>) ->
    {challenge, Nonce, Timestamp};
decode(<<?AUTH_CHALLENGE:8, _/binary>>) ->
    {error, invalid_challenge_payload};

decode(<<?AUTH_RESPONSE:8, Signature:?SIGNATURE_SIZE/binary>>) ->
    {response, Signature};
decode(<<?AUTH_RESPONSE:8, _/binary>>) ->
    {error, invalid_response_payload};

decode(<<?AUTH_OK:8>>) ->
    ok;

decode(<<?AUTH_FAIL:8, ReasonLen:16/big, Reason:ReasonLen/binary>>) ->
    {fail, Reason};
decode(<<?AUTH_FAIL:8, _/binary>>) ->
    {error, invalid_fail_payload};

decode(<<?AUTH_KEY_EXCHANGE:8, EphemeralPubKey:?X25519_KEY_SIZE/binary>>) ->
    {key_exchange, EphemeralPubKey};
decode(<<?AUTH_KEY_EXCHANGE:8, _/binary>>) ->
    {error, invalid_key_exchange_payload};

decode(<<Type:8, _/binary>>) ->
    {error, {unknown_message_type, Type}};

decode(_) ->
    {error, malformed_message}.

%%====================================================================
%% Node-name validation
%%====================================================================

%% @doc Validate a node name binary. Returns `ok' if the bytes form
%% a well-shaped `name@host' atom by Erlang dist conventions.
-spec validate_node_name(binary()) -> ok | {error, invalid_node_name}.
validate_node_name(Bin)
  when is_binary(Bin), byte_size(Bin) > 0, byte_size(Bin) =< ?MAX_NODE_NAME_LEN ->
    case binary:split(Bin, <<"@">>, [global]) of
        [Name, Host] when byte_size(Name) > 0, byte_size(Host) > 0 ->
            case is_valid_part(Name) andalso is_valid_part(Host) of
                true  -> ok;
                false -> {error, invalid_node_name}
            end;
        _ ->
            {error, invalid_node_name}
    end;
validate_node_name(_) ->
    {error, invalid_node_name}.

is_valid_part(<<C, _/binary>>) when C =:= $.; C =:= $- ->
    false;
is_valid_part(Bin) ->
    is_valid_chars(Bin).

is_valid_chars(<<>>) -> true;
is_valid_chars(<<C, Rest/binary>>) ->
    case is_name_byte(C) of
        true  -> is_valid_chars(Rest);
        false -> false
    end.

is_name_byte(C) when C >= $a, C =< $z -> true;
is_name_byte(C) when C >= $A, C =< $Z -> true;
is_name_byte(C) when C >= $0, C =< $9 -> true;
is_name_byte($_) -> true;
is_name_byte($.) -> true;
is_name_byte($-) -> true;
is_name_byte(_)  -> false.
