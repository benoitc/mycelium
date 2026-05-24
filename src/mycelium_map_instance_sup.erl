%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Per-map supervisor: starts the map owner, then its mycelium_replica
%%% instance. `rest_for_one' enforces that order on every (re)start - the
%%% replica casts into the owner, so the owner must exist first; if the
%%% owner dies, the replica is restarted too (and re-attaches to the fresh
%%% owner, then full-syncs from peers).
-module(mycelium_map_instance_sup).
-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).

start_link(Name, Opts) ->
    supervisor:start_link(?MODULE, {Name, Opts}).

init({Name, Opts}) ->
    SupFlags = #{strategy => rest_for_one, intensity => 10, period => 10},

    Owner = #{
        id => owner,
        start => {mycelium_map, start_link, [Name, Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_map]
    },

    Replica = #{
        id => replica,
        start => {mycelium_replica, start_link,
                  [#{name => mycelium_map:replica_name(Name),
                     callback => mycelium_map}]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_replica]
    },

    {ok, {SupFlags, [Owner, Replica]}}.
