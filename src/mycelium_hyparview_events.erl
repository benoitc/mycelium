%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_hyparview_events).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([notify/1, subscribe/1, unsubscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    subscribers = #{} :: #{pid() => reference()}
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec notify(term()) -> ok.
notify(Event) ->
    mycelium_metrics:hyparview_event(Event),
    gen_server:cast(?SERVER, {notify, Event}).

-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
    gen_server:call(?SERVER, {subscribe, Pid}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({subscribe, Pid}, _From, State) ->
    case maps:is_key(Pid, State#state.subscribers) of
        true ->
            {reply, ok, State};
        false ->
            Ref = monitor(process, Pid),
            Subs = maps:put(Pid, Ref, State#state.subscribers),
            {reply, ok, State#state{subscribers = Subs}}
    end;
handle_call({unsubscribe, Pid}, _From, State) ->
    case maps:take(Pid, State#state.subscribers) of
        {Ref, Subs} ->
            demonitor(Ref, [flush]),
            {reply, ok, State#state{subscribers = Subs}};
        error ->
            {reply, ok, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({notify, Event}, State) ->
    maps:foreach(
        fun(Pid, _Ref) ->
            Pid ! {mycelium_event, Event}
        end,
        State#state.subscribers
    ),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:get(Pid, State#state.subscribers, undefined) of
        Ref ->
            Subs = maps:remove(Pid, State#state.subscribers),
            {noreply, State#state{subscribers = Subs}};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
