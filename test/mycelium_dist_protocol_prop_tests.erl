%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Property tests for mycelium_dist_protocol.
%%%
%%% Two flavours:
%%%
%%%   1. Round-trip: for every well-formed input to one of the
%%%      `encode_*' functions, `decode/1' returns the matching tuple.
%%%
%%%   2. Fuzz: `decode/1' never crashes on a random binary; it
%%%      always returns either a tagged success or {error, _}.

-module(mycelium_dist_protocol_prop_tests).

-include_lib("eunit/include/eunit.hrl").
%% eunit and proper both define ?LET; proper's is the one we use here.
-undef('LET').
-include_lib("proper/include/proper.hrl").

-define(NUMTESTS, 500).
-define(PUBLIC_KEY_SIZE, 32).
-define(X25519_KEY_SIZE, 32).
-define(NONCE_SIZE, 32).
-define(SIGNATURE_SIZE, 64).

prop_test_() ->
    [{timeout, 60, ?_assert(run(prop_hello_roundtrip()))},
     {timeout, 60, ?_assert(run(prop_challenge_roundtrip()))},
     {timeout, 60, ?_assert(run(prop_response_roundtrip()))},
     {timeout, 60, ?_assert(run(prop_key_exchange_roundtrip()))},
     {timeout, 60, ?_assert(run(prop_fail_roundtrip()))},
     {timeout, 60, ?_assert(run(prop_ok_roundtrip()))},
     {timeout, 60, ?_assert(run(prop_decode_random_never_crashes()))},
     {timeout, 60, ?_assert(run(prop_hello_rejects_oversized_name()))},
     {timeout, 60, ?_assert(run(prop_hello_rejects_invalid_charset()))}].

run(Prop) ->
    proper:quickcheck(Prop, [{numtests, ?NUMTESTS}, {to_file, user}]).

%%====================================================================
%% Generators
%%====================================================================

fixed_bin(N) ->
    ?LET(Bytes, vector(N, choose(0, 255)),
         list_to_binary(Bytes)).

node_name() ->
    ?LET(S, non_empty(list(choose($a, $z))),
         list_to_atom(S ++ "@host")).

%%====================================================================
%% Round-trip properties
%%====================================================================

prop_hello_roundtrip() ->
    ?FORALL({Node, PubKey},
            {node_name(), fixed_bin(?PUBLIC_KEY_SIZE)},
            begin
                Enc = mycelium_dist_protocol:encode_hello(Node, PubKey),
                %% decode/1 returns the node name as a validated binary;
                %% the atom is minted only after auth succeeds.
                NodeBin = atom_to_binary(Node, utf8),
                case mycelium_dist_protocol:decode(Enc) of
                    {hello, NodeBin, PubKey} -> true;
                    _ -> false
                end
            end).

prop_challenge_roundtrip() ->
    ?FORALL({Nonce, TS},
            {fixed_bin(?NONCE_SIZE), choose(0, 16#FFFFFFFFFFFFFFFF)},
            begin
                Enc = mycelium_dist_protocol:encode_challenge(Nonce, TS),
                {challenge, Nonce, TS} =:= mycelium_dist_protocol:decode(Enc)
            end).

prop_response_roundtrip() ->
    ?FORALL(Sig, fixed_bin(?SIGNATURE_SIZE),
            begin
                Enc = mycelium_dist_protocol:encode_response(Sig),
                {response, Sig} =:= mycelium_dist_protocol:decode(Enc)
            end).

prop_key_exchange_roundtrip() ->
    ?FORALL(Key, fixed_bin(?X25519_KEY_SIZE),
            begin
                Enc = mycelium_dist_protocol:encode_key_exchange(Key),
                {key_exchange, Key} =:= mycelium_dist_protocol:decode(Enc)
            end).

prop_fail_roundtrip() ->
    ?FORALL(Reason, binary(),
            begin
                Enc = mycelium_dist_protocol:encode_fail(Reason),
                {fail, Reason} =:= mycelium_dist_protocol:decode(Enc)
            end).

prop_ok_roundtrip() ->
    ?FORALL(_, integer(),
            ok =:= mycelium_dist_protocol:decode(
                     mycelium_dist_protocol:encode_ok())).

%%====================================================================
%% Robustness
%%====================================================================

%% Any binary input must produce either a tagged success or
%% {error, _}. No crash, no badmatch.
prop_decode_random_never_crashes() ->
    ?FORALL(Bin, binary(),
            try mycelium_dist_protocol:decode(Bin) of
                ok                  -> true;
                {error, _}          -> true;
                {hello, _, _}       -> true;
                {challenge, _, _}   -> true;
                {response, _}       -> true;
                {key_exchange, _}   -> true;
                {fail, _}           -> true;
                _                   -> false
            catch _:_ ->
                false
            end).

%% A hello frame carrying a node name longer than 255 bytes must
%% never produce a `hello' tuple. Reject before the atom mint.
prop_hello_rejects_oversized_name() ->
    ?FORALL({Pad, PubKey},
            {choose(0, 100), fixed_bin(?PUBLIC_KEY_SIZE)},
            begin
                Name = list_to_binary(
                    [lists:duplicate(256 + Pad, $a), $@, $h]
                ),
                Bin = build_hello_wire(Name, PubKey),
                case mycelium_dist_protocol:decode(Bin) of
                    {error, _} -> true;
                    _ -> false
                end
            end).

%% A hello frame whose name contains a byte outside the allowed
%% charset must never produce a `hello' tuple. Allowed bytes are
%% letters, digits, `_', `.', `-' and exactly one `@'.
prop_hello_rejects_invalid_charset() ->
    ?FORALL({Pos, BadByte, PubKey},
            {choose(0, 10), oneof([0, 1, 16#7F, 16#80, $\s, $!, $/, $:, $;]),
             fixed_bin(?PUBLIC_KEY_SIZE)},
            begin
                Prefix = list_to_binary(lists:duplicate(Pos, $a)),
                Name = <<Prefix/binary, BadByte:8, "x@host">>,
                Bin = build_hello_wire(Name, PubKey),
                case mycelium_dist_protocol:decode(Bin) of
                    {error, _} -> true;
                    _ -> false
                end
            end).

build_hello_wire(NameBin, PubKey) ->
    NameLen = byte_size(NameBin),
    %% AUTH_HELLO = 1, version 1, length-prefixed name, fixed pubkey.
    <<1:8, 1:8, NameLen:16/big, NameBin/binary, PubKey/binary>>.
