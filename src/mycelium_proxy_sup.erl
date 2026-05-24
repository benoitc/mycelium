%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_proxy_sup).
-behaviour(supervisor).

%% Supervisor for service proxy processes

%% API
-export([start_link/0]).
-export([start_proxy/2, stop_proxy/1]).
-export([get_proxy/1, list_proxies/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_proxy(atom() | binary(), node()) -> {ok, pid()} | {error, term()}.
start_proxy(Name, TargetNode) ->
    case get_proxy(Name) of
        {ok, Pid} ->
            {ok, Pid};
        not_found ->
            case supervisor:start_child(?SERVER, [Name, TargetNode]) of
                {ok, Pid} ->
                    %% Store mapping in ETS for lookup
                    ets:insert(mycelium_proxies, {Name, Pid}),
                    {ok, Pid};
                {error, _} = Error ->
                    Error
            end
    end.

-spec stop_proxy(atom() | binary()) -> ok.
stop_proxy(Name) ->
    case get_proxy(Name) of
        {ok, Pid} ->
            ets:delete(mycelium_proxies, Name),
            supervisor:terminate_child(?SERVER, Pid);
        not_found ->
            ok
    end.

-spec get_proxy(atom() | binary()) -> {ok, pid()} | not_found.
get_proxy(Name) ->
    case ets:lookup(mycelium_proxies, Name) of
        [{_, Pid}] ->
            case is_process_alive(Pid) of
                true ->
                    {ok, Pid};
                false ->
                    ets:delete(mycelium_proxies, Name),
                    not_found
            end;
        [] ->
            not_found
    end.

-spec list_proxies() -> [{atom() | binary(), pid()}].
list_proxies() ->
    ets:tab2list(mycelium_proxies).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for proxy mapping
    mycelium_proxies = ets:new(mycelium_proxies, [
        named_table,
        public,
        set,
        {read_concurrency, true}
    ]),

    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },

    ChildSpec = #{
        id => proxy,
        start => {mycelium_service_proxy, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [mycelium_service_proxy]
    },

    {ok, {SupFlags, [ChildSpec]}}.
