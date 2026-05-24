%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_registry_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },

    ServiceEvents = #{
        id => mycelium_service_events,
        start => {mycelium_service_events, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_service_events]
    },

    Registry = #{
        id => mycelium_registry,
        start => {mycelium_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_registry]
    },

    Sync = #{
        id => mycelium_registry_replica,
        start =>
            {mycelium_replica, start_link, [
                #{
                    name => mycelium_registry_replica,
                    callback => mycelium_registry
                }
            ]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_replica]
    },

    Router = #{
        id => mycelium_router,
        start => {mycelium_router, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_router]
    },

    ProxySup = #{
        id => mycelium_proxy_sup,
        start => {mycelium_proxy_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [mycelium_proxy_sup]
    },

    ChildSpecs = [ServiceEvents, Registry, Sync, Router, ProxySup],
    {ok, {SupFlags, ChildSpecs}}.
