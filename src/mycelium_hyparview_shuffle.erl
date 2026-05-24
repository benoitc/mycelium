%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_hyparview_shuffle).
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([trigger_shuffle/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%% Shuffle period bounds (ms)

%% 2s minimum during high churn
-define(MIN_SHUFFLE_PERIOD, 2000).
%% 30s maximum during low churn
-define(MAX_SHUFFLE_PERIOD, 30000).

%% Churn thresholds

%% >10 events = high churn
-define(HIGH_CHURN_THRESHOLD, 10).
%% >5 events = medium churn
-define(MEDIUM_CHURN_THRESHOLD, 5).

-record(state, {
    base_shuffle_period :: pos_integer(),
    shuffle_length :: pos_integer(),
    timer_ref :: reference() | undefined,
    current_period :: pos_integer()
}).

%%====================================================================
%% API
%%====================================================================

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

-spec trigger_shuffle() -> ok.
trigger_shuffle() ->
    gen_server:cast(?SERVER, trigger_shuffle).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    Period = maps:get(shuffle_period, Config, 10000),
    Length = maps:get(shuffle_length, Config, 8),
    State = #state{
        base_shuffle_period = Period,
        shuffle_length = Length,
        current_period = Period
    },
    {ok, schedule_shuffle(State)}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(trigger_shuffle, State) ->
    do_shuffle(State),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(shuffle_timeout, State) ->
    do_shuffle(State),
    NewPeriod = calculate_shuffle_period(State),
    State1 = State#state{current_period = NewPeriod},
    {noreply, schedule_shuffle(State1)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ =
        case State#state.timer_ref of
            undefined -> ok;
            Ref -> erlang:cancel_timer(Ref)
        end,
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

schedule_shuffle(State) ->
    Ref = erlang:send_after(State#state.current_period, self(), shuffle_timeout),
    State#state{timer_ref = Ref}.

do_shuffle(State) ->
    case mycelium_hyparview:active_view() of
        [] ->
            ok;
        ActiveNodes ->
            %% Pick random active peer
            Target = lists:nth(rand:uniform(length(ActiveNodes)), ActiveNodes),
            mycelium_hyparview:initiate_shuffle(Target, State#state.shuffle_length)
    end.

%% Calculate adaptive shuffle period based on churn rate
calculate_shuffle_period(State) ->
    {Joins, Leaves} = mycelium_hyparview:get_churn_stats(),
    ChurnRate = Joins + Leaves,
    BasePeriod = State#state.base_shuffle_period,

    Period =
        if
            ChurnRate > ?HIGH_CHURN_THRESHOLD ->
                %% High churn: use minimum period for faster view refresh
                ?MIN_SHUFFLE_PERIOD;
            ChurnRate > ?MEDIUM_CHURN_THRESHOLD ->
                %% Medium churn: use half of base period
                max(?MIN_SHUFFLE_PERIOD, BasePeriod div 2);
            true ->
                %% Normal: use base period, capped at max
                min(BasePeriod, ?MAX_SHUFFLE_PERIOD)
        end,

    %% Ensure within bounds
    max(?MIN_SHUFFLE_PERIOD, min(Period, ?MAX_SHUFFLE_PERIOD)).
