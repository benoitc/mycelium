%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_service_proxy).
-behaviour(gen_server).

%% Proxy that forwards messages to remote service through overlay
%% Registered with global for transparency: global:whereis_name returns proxy pid

-include("mycelium.hrl").

%% API
-export([start_link/2]).
-export([relay/3, relay/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(CALL_TIMEOUT, 10000).
-define(RELAY_TAG, '$mycelium_relay').

%% Cap on concurrent overlay-cast helpers. Each cast through a
%% `{via, NextHop}' route used to spawn an unbounded process.
-define(DEFAULT_MAX_IN_FLIGHT, 32).

%% Maximum overlay hops a relayed call may traverse. Matches the
%% route-lookup TTL in mycelium_router.
-define(DEFAULT_TTL, 5).

-record(state, {
    name :: atom() | binary(),
    target_node :: node(),
    in_flight = 0 :: non_neg_integer(),
    max_in_flight :: pos_integer(),
    watch = #{} :: mycelium_source_monitor:watch()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(atom() | binary(), node()) -> {ok, pid()} | {error, term()}.
start_link(Name, TargetNode) ->
    gen_server:start_link(?MODULE, [Name, TargetNode], []).

%% Relay a call through this node to the target. The 3-arity form
%% starts a fresh hop budget; the 4-arity form is what every relay
%% hop calls recursively, carrying the TTL and visited list.
-spec relay(atom() | binary(), node(), term()) -> term().
relay(Name, TargetNode, Request) ->
    relay(
        Name,
        TargetNode,
        Request,
        #{ttl => ?DEFAULT_TTL, visited => [node()]}
    ).

-spec relay(atom() | binary(), node(), term(), map()) -> term().
relay(_Name, _TargetNode, _Request, #{ttl := TTL}) when TTL =< 0 ->
    {error, ttl_expired};
relay(Name, TargetNode, Request, #{visited := Visited} = Ctx) ->
    case mycelium_router:find_route(TargetNode) of
        {direct, _} ->
            gen_server:call({Name, TargetNode}, Request, ?CALL_TIMEOUT);
        {via, NextHop} ->
            case lists:member(NextHop, Visited) of
                true ->
                    {error, relay_loop};
                false ->
                    NextCtx = Ctx#{
                        ttl => maps:get(ttl, Ctx) - 1,
                        visited => [node() | Visited]
                    },
                    rpc:call(
                        NextHop,
                        ?MODULE,
                        relay,
                        [Name, TargetNode, Request, NextCtx],
                        ?CALL_TIMEOUT
                    )
            end;
        no_route ->
            {error, no_route}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Name, TargetNode]) ->
    %% Subscribe to the service event bus so we hear when the remote
    %% service dies. mycelium:subscribe/1 routes to HyParView events,
    %% which never carry service_* notifications. Keep the subscription
    %% alive across a service-events restart.
    Watch = mycelium_source_monitor:start([mycelium_service_events]),
    Max = application:get_env(
        mycelium, proxy_cast_max_in_flight, ?DEFAULT_MAX_IN_FLIGHT
    ),
    {ok, #state{
        name = Name,
        target_node = TargetNode,
        max_in_flight = Max,
        watch = Watch
    }}.

handle_call(Request, _From, #state{name = Name, target_node = Target} = State) ->
    Result = forward_call(Name, Target, Request),
    {reply, Result, State}.

handle_cast(Request, #state{name = Name, target_node = Target} = State) ->
    State1 = forward_cast(Name, Target, Request, State),
    {noreply, State1}.

handle_info(
    {mycelium_service_event, {service_down, Name, _Node, _Reason}},
    #state{name = Name} = State
) ->
    %% Service died on remote node, proxy should terminate.
    {stop, normal, State};
handle_info({mycelium_service_event, _}, State) ->
    %% Other service events are not relevant to this proxy.
    {noreply, State};
%% Re-subscribe if a watched source (service events) restarted.
handle_info({mycelium_source_monitor, retry, Source}, #state{watch = W} = State) ->
    {noreply, State#state{watch = mycelium_source_monitor:retry(Source, W)}};
handle_info(
    {'DOWN', Ref, process, _Pid, _Reason},
    #state{watch = W} = State
) ->
    case mycelium_source_monitor:down(Ref, W) of
        {down, _Source, W1} ->
            {noreply, State#state{watch = W1}};
        ignore ->
            case State#state.in_flight of
                N when N > 0 -> {noreply, State#state{in_flight = N - 1}};
                _ -> {noreply, State}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{name = Name}) ->
    %% Unregister from global if registered
    catch global:unregister_name(Name),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

forward_call(Name, Target, Request) ->
    case mycelium_router:find_route(Target) of
        {direct, _} ->
            %% Direct connection exists
            try
                gen_server:call({Name, Target}, Request, ?CALL_TIMEOUT)
            catch
                exit:{noproc, _} -> {error, service_not_found};
                exit:{{nodedown, _}, _} -> {error, node_down};
                exit:{timeout, _} -> {error, timeout}
            end;
        {via, NextHop} ->
            %% Route through overlay. Start the hop with a fresh
            %% TTL and visited list seeded with our own node.
            Ctx = #{ttl => ?DEFAULT_TTL, visited => [node()]},
            case
                rpc:call(
                    NextHop,
                    ?MODULE,
                    relay,
                    [Name, Target, Request, Ctx],
                    ?CALL_TIMEOUT
                )
            of
                {badrpc, Reason} -> {error, Reason};
                Result -> Result
            end;
        no_route ->
            {error, no_route}
    end.

forward_cast(Name, Target, Request, State) ->
    case mycelium_router:find_route(Target) of
        {direct, _} ->
            gen_server:cast({Name, Target}, Request),
            State;
        {via, NextHop} ->
            spawn_overlay_cast(NextHop, Name, Target, Request, State);
        no_route ->
            State
    end.

%% Fire-and-forget overlay cast, bounded by `max_in_flight'. Excess
%% casts are dropped with a metric.
spawn_overlay_cast(
    _NextHop,
    _Name,
    _Target,
    _Request,
    #state{in_flight = N, max_in_flight = Max} = State
) when
    N >= Max
->
    mycelium_metrics:proxy_cast_dropped(),
    State;
spawn_overlay_cast(
    NextHop,
    Name,
    Target,
    Request,
    #state{in_flight = N} = State
) ->
    {_Pid, _Ref} = spawn_monitor(fun() ->
        rpc:call(NextHop, gen_server, cast, [{Name, Target}, Request])
    end),
    State#state{in_flight = N + 1}.
