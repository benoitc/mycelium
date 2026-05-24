%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_discovery_dns).
-behaviour(quic_discovery).

%% DNS-fallback discovery backend. Resolves the host part of a
%% `name@host' node atom via `inet:getaddr/2', then pairs it with
%% the QUIC-dist port from `application:get_env(quic, dist, [{port, _}])'
%% (default 4433).

-define(DEFAULT_PORT, 4433).

-export([lookup/2]).

lookup(Node, Host) ->
    Port = application:get_env(quic, dist_port, ?DEFAULT_PORT),
    case resolve_host(Host) of
        {ok, IP} ->
            {ok, {IP, Port}};
        {error, _} ->
            case extract_host(Node) of
                {ok, NodeHost} ->
                    case resolve_host(NodeHost) of
                        {ok, IP} -> {ok, {IP, Port}};
                        Err -> Err
                    end;
                Err ->
                    Err
            end
    end.

%%====================================================================
%% Internal
%%====================================================================

%% Accept either a node atom (`name@host') or a bare name string
%% (`"name"') as upstream `quic_dist' may pass either depending on
%% the path. Bare names have no host part to extract; return error
%% so the caller falls back to the host argument.
extract_host(Node) when is_atom(Node) ->
    extract_host(atom_to_list(Node));
extract_host(Node) when is_binary(Node) ->
    extract_host(binary_to_list(Node));
extract_host(Node) when is_list(Node) ->
    case string:split(Node, "@") of
        [_, Host] -> {ok, Host};
        _ -> {error, invalid_node_name}
    end;
extract_host(_) ->
    {error, invalid_node_name}.

resolve_host(Host) when is_list(Host) ->
    case inet:parse_address(Host) of
        {ok, IP} ->
            {ok, IP};
        {error, _} ->
            case inet:getaddr(Host, inet) of
                {ok, IP} -> {ok, IP};
                {error, _} -> inet:getaddr(Host, inet6)
            end
    end;
resolve_host(Host) when is_binary(Host) ->
    resolve_host(binary_to_list(Host));
resolve_host(_) ->
    {error, invalid_host}.
