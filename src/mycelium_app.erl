%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% Set distribution cookie automatically
    init_dist_cookie(),
    %% Disable global's partition prevention - mycelium manages topology
    ok = application:set_env(kernel, prevent_overlapping_partitions, false),
    %% HyParView owns the bounded gossip topology (active view); OTP
    %% owns demand-driven dist channels (`Pid ! Msg' to any cluster
    %% node auto-connects through the mycelium discovery chain). The
    %% two are decoupled: `mycelium_dist_gc' reaps idle non-gossip
    %% channels so the natural fan-out stays bounded.
    ok = application:set_env(kernel, dist_auto_connect, once),
    %% Publish ourselves through the discovery chain. quic_dist's own
    %% registration path runs before sys.config envs apply, so we
    %% redo it here now that mycelium's discovery_backends env is set.
    publish_self(),
    mycelium_sup:start_link().

%% @private Register this node with the discovery chain after sys.config
%% has applied, so backends configured in `discovery_backends' actually
%% see the call. Best-effort; logs and moves on if anything fails.
publish_self() ->
    case node() of
        nonode@nohost ->
            ok;
        Node ->
            case find_listen_port() of
                {ok, Port} ->
                    DistOpts = application:get_env(quic, dist, []),
                    {ok, S0} = mycelium_discovery:init(#{}),
                    _ = mycelium_discovery:register(Node, Port, S0),
                    _ = DistOpts,
                    ok;
                error ->
                    logger:debug(
                        "[mycelium_app] no listen port found; "
                        "skipping discovery publish"
                    ),
                    ok
            end
    end.

%% @private Find the live quic_dist listener port via persistent_term.
%% Upstream `quic_dist' stashes `{quic_dist_early_listener, _} ->
%% #{port => P, ...}` during early boot, and the same entry sticks
%% around once the quic app adopts the listener.
find_listen_port() ->
    Hits = [
        V
     || {{quic_dist_early_listener, _}, V} <- persistent_term:get(),
        is_map(V)
    ],
    case Hits of
        [#{port := Port} | _] when is_integer(Port) -> {ok, Port};
        _ -> error
    end.

%% @doc Set the distribution cookie automatically.
%% Uses the configured dist_cookie or defaults to 'mycelium'.
%% This removes the need for users to set -setcookie on the command line.
%% Only sets cookie when running as a distributed node.
init_dist_cookie() ->
    case node() of
        nonode@nohost ->
            %% Not a distributed node, skip cookie setup
            ok;
        Node ->
            Cookie = application:get_env(mycelium, dist_cookie, mycelium),
            check_cookie_safety(Cookie),
            erlang:set_cookie(Node, Cookie)
    end.

%% @private The default cookie is a public constant. With Ed25519 auth on
%% it is only defense-in-depth, so we warn. But for cookie_only_nodes
%% peers the cookie is the sole barrier - refuse to boot in that
%% combination rather than ship a cluster gated by a known cookie.
check_cookie_safety(Cookie) ->
    CookieOnly = application:get_env(mycelium, cookie_only_nodes, []),
    case Cookie =:= mycelium of
        true when CookieOnly =/= [] ->
            erlang:error({mycelium, cookie_only_nodes_with_default_cookie});
        true ->
            logger:warning(
                "mycelium: using the default distribution cookie 'mycelium'. "
                "Set {mycelium, [{dist_cookie, <secret>}]} for production."
            );
        false ->
            ok
    end.

stop(_State) ->
    ok.
