%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_discovery).
-behaviour(quic_discovery).

%% Composing discovery module for mycelium.
%%
%% Implements the upstream `quic_discovery' behaviour so it can be
%% wired into `quic_dist' as `discovery_module'. Delegates to a chain
%% of backend modules, each itself a `quic_discovery' implementation.
%%
%% The chain is read at every call from the application env:
%%   application:get_env(mycelium, discovery_backends, [...]).
%%
%% Default chain:
%%   [mycelium_discovery_static,  %% explicit {Node, {Addr, Port}} map
%%    mycelium_discovery_file,    %% data/discovery/<node>.endpoint files
%%    mycelium_discovery_dns]     %% DNS host-from-name fallback
%%
%% Semantics:
%%   - register/3   fans out to every backend that exports register/3.
%%   - lookup/2     tries each backend in order; first {ok, _} wins.
%%   - list_nodes/1 unions every backend's output.
%%
%% State for the composing module is a map of `Module => BackendState'.
%% Backends without an `init/1' callback get the empty map as state.

-export([init/1, register/3, lookup/2, list_nodes/1]).

-define(DEFAULT_BACKENDS, [
    mycelium_discovery_static,
    mycelium_discovery_file,
    mycelium_discovery_dns
]).

%%====================================================================
%% quic_discovery callbacks
%%====================================================================

init(Opts) ->
    Backends = configured_backends(Opts),
    States = lists:foldl(
        fun(Mod, Acc) ->
            case init_backend(Mod, Opts) of
                {ok, S} -> Acc#{Mod => S};
                {error, _} -> Acc
            end
        end,
        #{},
        Backends
    ),
    {ok, States}.

register(Node, Port, State) when is_map(State) ->
    Backends = configured_backends(#{}),
    NewState = lists:foldl(
        fun(Mod, Acc) -> register_one(Mod, Node, Port, Acc) end,
        State,
        Backends
    ),
    {ok, NewState}.

register_one(Mod, Node, Port, Acc) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, register, 3) of
        false ->
            Acc;
        true ->
            BS = maps:get(Mod, Acc, undefined),
            case Mod:register(Node, Port, BS) of
                {ok, NS} ->
                    Acc#{Mod => NS};
                {error, R} ->
                    logger:warning(
                        "[mycelium_discovery] ~p register failed: ~p",
                        [Mod, R]
                    ),
                    Acc
            end
    end.

lookup(Node, Host) ->
    Backends = configured_backends(#{}),
    try_lookup(Backends, Node, Host).

list_nodes(Host) ->
    Backends = configured_backends(#{}),
    All = lists:foldl(
        fun(Mod, Acc) ->
            case erlang:function_exported(Mod, list_nodes, 1) of
                false ->
                    Acc;
                true ->
                    case Mod:list_nodes(Host) of
                        {ok, L} -> L ++ Acc;
                        {error, _} -> Acc
                    end
            end
        end,
        [],
        Backends
    ),
    {ok, lists:usort(All)}.

%%====================================================================
%% Helpers
%%====================================================================

configured_backends(Opts) when is_map(Opts) ->
    case maps:get(backends, Opts, undefined) of
        undefined ->
            application:get_env(mycelium, discovery_backends, ?DEFAULT_BACKENDS);
        Backends when is_list(Backends) ->
            Backends
    end;
configured_backends(_) ->
    application:get_env(mycelium, discovery_backends, ?DEFAULT_BACKENDS).

init_backend(Mod, Opts) ->
    case erlang:function_exported(Mod, init, 1) of
        false -> {ok, undefined};
        true -> Mod:init(Opts)
    end.

try_lookup([], _Node, _Host) ->
    {error, not_found};
try_lookup([Mod | Rest], Node, Host) ->
    case Mod:lookup(Node, Host) of
        {ok, _} = Hit -> Hit;
        {error, _} -> try_lookup(Rest, Node, Host)
    end.
