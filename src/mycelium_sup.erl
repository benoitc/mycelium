%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_sup).
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

    %% HLC must start first - other components depend on it
    HLC = #{
        id => mycelium_hlc,
        start => {mycelium_hlc, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_hlc]
    },

    %% Distribution keys manager - handles Ed25519 authentication keys
    DistKeys = #{
        id => mycelium_dist_keys,
        start => {mycelium_dist_keys, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_dist_keys]
    },

    HyparviewSup = #{
        id => mycelium_hyparview_sup,
        start => {mycelium_hyparview_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [mycelium_hyparview_sup]
    },

    RegistrySup = #{
        id => mycelium_registry_sup,
        start => {mycelium_registry_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [mycelium_registry_sup]
    },

    %% Leader election. Started after the registry so Plumtree and the
    %% HyParView event bus are already up (the sync worker subscribes to
    %% both in init). Leader before its sync: the sync casts into it.
    Leader = #{
        id => mycelium_leader,
        start => {mycelium_leader, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_leader]
    },

    LeaderSync = #{
        id => mycelium_leader_replica,
        start => {mycelium_replica, start_link,
                  [#{name => mycelium_leader_replica,
                     callback => mycelium_leader}]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_replica]
    },

    %% Sharded placement. Started after the registry (Plumtree + event
    %% bus up). Shard before its replica instance: the replica casts into
    %% it, and the shard's first heartbeat is deferred to a timer.
    Shard = #{
        id => mycelium_shard,
        start => {mycelium_shard, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_shard]
    },

    ShardReplica = #{
        id => mycelium_members_replica,
        start => {mycelium_replica, start_link,
                  [#{name => mycelium_members_replica,
                     callback => mycelium_shard}]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_replica]
    },

    %% Durable reminders. Started after the shard (it subscribes to
    %% ownership events and resolves owners via mycelium_shard:place/1).
    %% Reminder before its replica instance: the replica casts into it.
    Reminder = #{
        id => mycelium_reminder,
        start => {mycelium_reminder, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_reminder]
    },

    ReminderReplica = #{
        id => mycelium_reminder_replica,
        start => {mycelium_replica, start_link,
                  [#{name => mycelium_reminder_replica,
                     callback => mycelium_reminder}]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_replica]
    },

    PlumtreeSup = #{
        id => mycelium_plumtree_sup,
        start => {mycelium_plumtree_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [mycelium_plumtree_sup]
    },

    %% Public replicated maps (mycelium_map). Dynamic: starts empty and
    %% hosts whatever maps the app declares in `replicated_maps' or creates
    %% at runtime. After Plumtree + HyParView (the per-map replica
    %% subscribes to both).
    MapSup = #{
        id => mycelium_map_sup,
        start => {mycelium_map_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [mycelium_map_sup]
    },

    Bridge = #{
        id => mycelium_bridge,
        start => {mycelium_bridge, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_bridge]
    },

    %% Tagged user-stream multiplex. Apps register their own tag and
    %% get a stream-shaped channel on top of quic_dist user streams.
    Streams = #{
        id => mycelium_streams,
        start => {mycelium_streams, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_streams]
    },

    %% Idle dist-channel GC. Reaps QUIC connections that are not in
    %% the HyParView active view and carry no live user streams.
    %% Architectural pillar of the decoupled design - not optional.
    DistGc = #{
        id => mycelium_dist_gc,
        start => {mycelium_dist_gc, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_dist_gc]
    },

    ChildSpecs = [HLC, DistKeys, HyparviewSup, PlumtreeSup, RegistrySup,
                  Leader, LeaderSync, Shard, ShardReplica,
                  Reminder, ReminderReplica, MapSup,
                  Bridge, Streams, DistGc],
    {ok, {SupFlags, ChildSpecs}}.
