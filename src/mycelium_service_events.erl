%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_service_events).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([notify/1, subscribe/1, subscribe/2, unsubscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    subscribers = #{} :: #{pid() => {reference(), filter()}}
}).

-type filter() :: all | {name, atom() | binary()} | {pattern, binary()}.

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec notify(term()) -> ok.
notify(Event) ->
    gen_server:cast(?SERVER, {notify, Event}).

-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
    subscribe(Pid, all).

-spec subscribe(pid(), filter()) -> ok.
subscribe(Pid, Filter) ->
    gen_server:call(?SERVER, {subscribe, Pid, Filter}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({subscribe, Pid, Filter}, _From, State) ->
    case maps:is_key(Pid, State#state.subscribers) of
        true ->
            %% Update filter for existing subscriber
            {Ref, _OldFilter} = maps:get(Pid, State#state.subscribers),
            Subs = maps:put(Pid, {Ref, Filter}, State#state.subscribers),
            {reply, ok, State#state{subscribers = Subs}};
        false ->
            Ref = monitor(process, Pid),
            Subs = maps:put(Pid, {Ref, Filter}, State#state.subscribers),
            {reply, ok, State#state{subscribers = Subs}}
    end;
handle_call({unsubscribe, Pid}, _From, State) ->
    case maps:take(Pid, State#state.subscribers) of
        {{Ref, _Filter}, Subs} ->
            demonitor(Ref, [flush]),
            {reply, ok, State#state{subscribers = Subs}};
        error ->
            {reply, ok, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({notify, Event}, State) ->
    maps:foreach(
        fun(Pid, {_Ref, Filter}) ->
            case matches_filter(Event, Filter) of
                true -> Pid ! {mycelium_service_event, Event};
                false -> ok
            end
        end,
        State#state.subscribers
    ),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:get(Pid, State#state.subscribers, undefined) of
        {Ref, _Filter} ->
            Subs = maps:remove(Pid, State#state.subscribers),
            {noreply, State#state{subscribers = Subs}};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

matches_filter(_Event, all) ->
    true;
matches_filter(Event, {name, Name}) ->
    get_event_name(Event) =:= Name;
matches_filter(Event, {pattern, Pattern}) ->
    case get_event_name(Event) of
        undefined ->
            false;
        Name ->
            NameBin = to_binary(Name),
            case re:run(NameBin, Pattern) of
                {match, _} -> true;
                nomatch -> false
            end
    end.

get_event_name({service_registered, Name, _Node}) -> Name;
get_event_name({service_unregistered, Name, _Node}) -> Name;
get_event_name({service_down, Name, _Node, _Reason}) -> Name;
get_event_name(_) -> undefined.

to_binary(Name) when is_atom(Name) -> atom_to_binary(Name, utf8);
to_binary(Name) when is_binary(Name) -> Name;
to_binary(Name) when is_list(Name) -> list_to_binary(Name).
