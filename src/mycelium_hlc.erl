%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_hlc).
-behaviour(gen_server).

%% HLC (Hybrid Logical Clock) wrapper for the hlc library.
%% Provides causal ordering with wall-clock approximation.

-include_lib("hlc/include/hlc.hrl").

%% API
-export([start_link/0]).
-export([now/0, update/1, compare/2]).
-export([to_binary/1, from_binary/1]).
-export([wall_time/1, logical/1]).
-export([pack/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-define(SERVER, ?MODULE).

-type timestamp() :: #timestamp{}.
-export_type([timestamp/0]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Get current HLC timestamp (advances clock)
-spec now() -> timestamp().
now() ->
    gen_server:call(?SERVER, now).

%% Update local clock with remote timestamp (for receiving messages)
-spec update(timestamp()) -> timestamp().
update(Remote) ->
    gen_server:call(?SERVER, {update, Remote}).

%% Compare two HLC timestamps
%% Returns: lt | eq | gt
-spec compare(timestamp(), timestamp()) -> lt | eq | gt.
compare(
    #timestamp{wall_time = WA, logical = LA},
    #timestamp{wall_time = WB, logical = LB}
) ->
    if
        WA =:= WB, LA =:= LB -> eq;
        WA < WB -> lt;
        WA > WB -> gt;
        LA < LB -> lt;
        true -> gt
    end.

%% Serialize HLC timestamp to binary
-spec to_binary(timestamp()) -> binary().
to_binary(#timestamp{wall_time = Wall, logical = Logical}) ->
    <<Wall:64/big, Logical:32/big>>.

%% Deserialize binary to HLC timestamp
-spec from_binary(binary()) -> timestamp().
from_binary(<<Wall:64/big, Logical:32/big>>) ->
    #timestamp{wall_time = Wall, logical = Logical}.

%% Extract wall time from timestamp
-spec wall_time(timestamp()) -> non_neg_integer().
wall_time(#timestamp{wall_time = Wall}) -> Wall.

%% Extract logical clock from timestamp
-spec logical(timestamp()) -> non_neg_integer().
logical(#timestamp{logical = Logical}) -> Logical.

%% Pack a timestamp into a single comparable, monotonic integer. Used
%% as an opaque fencing token / version id that external systems compare
%% with `>'. Higher wall time dominates; logical breaks ties within a
%% wall tick.
-spec pack(timestamp()) -> non_neg_integer().
pack(#timestamp{wall_time = Wall, logical = Logical}) ->
    (Wall bsl 32) bor (Logical band 16#FFFFFFFF).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, Pid} = hlc:start_link(),
    {ok, #{clock => Pid}}.

handle_call(now, _From, #{clock := Clock} = State) ->
    {reply, hlc:now(Clock), State};
handle_call({update, Remote}, _From, #{clock := Clock} = State) ->
    case hlc:update(Clock, Remote) of
        {ok, TS} -> {reply, TS, State};
        {timeahead, TS} -> {reply, TS, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
