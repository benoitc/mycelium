%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_discovery_static).
-behaviour(quic_discovery).

%% Static-config discovery backend. Reads a list of explicit node
%% endpoints from `application:get_env(quic, dist, [{nodes, [...]}])'.
%% Two entry shapes accepted (matching the upstream `quic_dist' parser):
%%
%%   { Node, {Addr, Port} }       Addr is an inet:ip4_address() tuple
%%                                or a string (IP literal or hostname).
%%   { Node, Addr, Port }         Same, but as a 3-tuple.
%%
%% No on-disk side effects; `register/3' is a no-op so this backend
%% can be combined with the filesystem backend without double-writes.

-export([lookup/2, list_nodes/1]).

%%====================================================================
%% quic_discovery callbacks
%%====================================================================

lookup(Node, _Host) ->
    case lists:keyfind(Node, 1, static_nodes()) of
        {Node, {Addr, Port}} -> {ok, {Addr, Port}};
        {Node, Addr, Port} -> {ok, {Addr, Port}};
        false -> {error, not_found}
    end.

list_nodes(_Host) ->
    Nodes = lists:filtermap(
        fun
            ({Node, {_, Port}}) -> {true, {Node, Port}};
            ({Node, _, Port}) -> {true, {Node, Port}};
            (_) -> false
        end,
        static_nodes()
    ),
    {ok, Nodes}.

%%====================================================================
%% Internal
%%====================================================================

static_nodes() ->
    DistOpts = application:get_env(quic, dist, []),
    proplists:get_value(nodes, DistOpts, []).
