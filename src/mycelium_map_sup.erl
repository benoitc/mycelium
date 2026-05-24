%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Dynamic supervisor for public replicated maps (`mycelium_map'). One
%%% per-map instance supervisor child per named map. Holds an ETS registry
%%% (map name -> instance-sup pid) for idempotent `start_map' and lookup.
%%%
%%% At boot it starts every map declared in the `replicated_maps' app env,
%%% so a map declared in config exists on every node without a per-node
%%% call. Placed after Plumtree + HyParView in the tree (the replica
%%% subscribes to both).
-module(mycelium_map_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([start_map/2, stop_map/1, which_maps/0]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(TAB, mycelium_maps).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    case supervisor:start_link({local, ?SERVER}, ?MODULE, []) of
        {ok, Pid} ->
            %% The supervisor is up now, so start_child works. Best-effort:
            %% a bad declared map is logged, not fatal to boot.
            start_declared_maps(),
            {ok, Pid};
        Other ->
            Other
    end.

%% @doc Start (or return the existing) named map. Idempotent.
-spec start_map(atom(), mycelium_map:opts()) -> {ok, pid()} | {error, term()}.
start_map(Name, Opts) when is_atom(Name) ->
    case lookup(Name) of
        {ok, Pid} ->
            {ok, Pid};
        not_found ->
            case supervisor:start_child(?SERVER, [Name, Opts]) of
                {ok, Pid} ->
                    ets:insert(?TAB, {Name, Pid}),
                    {ok, Pid};
                {error, _} = Error ->
                    Error
            end
    end;
start_map(_Name, _Opts) ->
    {error, invalid_map_name}.

%% @doc Stop the named map on this node.
-spec stop_map(atom()) -> ok.
stop_map(Name) ->
    case lookup(Name) of
        {ok, Pid} ->
            ets:delete(?TAB, Name),
            _ = supervisor:terminate_child(?SERVER, Pid),
            ok;
        not_found ->
            ok
    end.

%% @doc Names of the maps running on this node.
-spec which_maps() -> [atom()].
which_maps() ->
    [N || {N, _Pid} <- ets:tab2list(?TAB)].

%%====================================================================
%% supervisor callback
%%====================================================================

init([]) ->
    ?TAB = ets:new(?TAB, [named_table, public, set, {read_concurrency, true}]),
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    Child = #{
        id => map_instance,
        start => {mycelium_map_instance_sup, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [mycelium_map_instance_sup]
    },
    {ok, {SupFlags, [Child]}}.

%%====================================================================
%% Internal
%%====================================================================

lookup(Name) ->
    case ets:lookup(?TAB, Name) of
        [{_, Pid}] ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Pid};
                false ->
                    ets:delete(?TAB, Name),
                    not_found
            end;
        [] ->
            not_found
    end.

start_declared_maps() ->
    Declared = application:get_env(mycelium, replicated_maps, []),
    lists:foreach(
        fun
            ({Name, Opts}) when is_atom(Name), is_map(Opts) ->
                case start_map(Name, Opts) of
                    {ok, _} ->
                        ok;
                    {error, Reason} ->
                        logger:error(
                            "mycelium_map_sup: declared map ~p "
                            "failed to start: ~p",
                            [Name, Reason]
                        )
                end;
            (Bad) ->
                logger:error(
                    "mycelium_map_sup: invalid replicated_maps "
                    "entry (want {atom(), map()}): ~p",
                    [Bad]
                )
        end,
        Declared
    ).
