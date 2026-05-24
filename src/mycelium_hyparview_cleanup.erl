%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_hyparview_cleanup).
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([trigger_cleanup/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    cleanup_period :: pos_integer(),
    timer_ref :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

-spec trigger_cleanup() -> ok.
trigger_cleanup() ->
    gen_server:cast(?SERVER, trigger_cleanup).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    %% Default 1 minute
    Period = maps:get(passive_cleanup_period, Config, 60000),
    State = #state{
        cleanup_period = Period
    },
    {ok, schedule_cleanup(State)}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(trigger_cleanup, State) ->
    do_cleanup(),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(cleanup_timeout, State) ->
    do_cleanup(),
    {noreply, schedule_cleanup(State)};
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

schedule_cleanup(State) ->
    Ref = erlang:send_after(State#state.cleanup_period, self(), cleanup_timeout),
    State#state{timer_ref = Ref}.

do_cleanup() ->
    mycelium_hyparview:cleanup_passive_view().
