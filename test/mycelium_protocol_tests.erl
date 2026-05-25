%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_protocol: encode/decode round-trip for every
%%% HyParView message, the send/2 framing to mycelium_bridge, and the
%%% handle_message/2 dispatch.
-module(mycelium_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("mycelium.hrl").

-define(TAG, '$mycelium_hyparview').

%% A peer whose non-wire fields already hold the decode defaults, so
%% decode(encode(P)) is exactly P.
peer(Id) ->
    #peer{
        id = Id,
        address = {127, 0, 0, 1},
        port = 9000,
        priority = high,
        connected = false,
        last_seen = undefined
    }.

encode_decode_roundtrip_test() ->
    A = peer('a@h'),
    B = peer('b@h'),
    C = peer('c@h'),
    Msgs = [
        {join, A},
        {forward_join, B, 3, A},
        {disconnect, A},
        {neighbor, high, A},
        {neighbor_reply, true, A},
        {shuffle, 2, [B, C], A},
        {shuffle_reply, [B, C], A}
    ],
    [
        ?assertEqual(M, mycelium_protocol:decode(mycelium_protocol:encode(M)))
     || M <- Msgs
    ].

%% send/2 frames the message as {TAG, FromNode, Msg} and delivers it to
%% the mycelium_bridge registered name on the target node.
send_delivers_to_bridge_test() ->
    true = register(mycelium_bridge, self()),
    try
        Msg = {join, peer('a@h')},
        ok = mycelium_protocol:send(node(), Msg),
        receive
            Got -> ?assertEqual({?TAG, node(), Msg}, Got)
        after 1000 ->
            erlang:error(no_message_received)
        end
    after
        catch unregister(mycelium_bridge)
    end.

%% A tagged message is dispatched to mycelium_hyparview:handle_msg/2.
handle_message_dispatches_to_hyparview_test() ->
    meck:new(mycelium_hyparview, [passthrough]),
    Self = self(),
    try
        meck:expect(mycelium_hyparview, handle_msg, fun(M, From) ->
            Self ! {dispatched, M, From},
            ok
        end),
        Msg = {disconnect, peer('a@h')},
        ?assertEqual(ok, mycelium_protocol:handle_message({?TAG, 'x@h', Msg}, ignored)),
        receive
            {dispatched, M, From} ->
                ?assertEqual(Msg, M),
                ?assertEqual('x@h', From)
        after 1000 ->
            erlang:error(not_dispatched)
        end
    after
        meck:unload(mycelium_hyparview)
    end.

%% An untagged message is ignored.
handle_message_ignores_unknown_test() ->
    ?assertEqual(ok, mycelium_protocol:handle_message({something_else, foo}, ignored)).
