%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_bridge).
-behaviour(gen_server).

-export([start_link/0]).
-export([request_connect/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    pending = #{} :: #{node() => connecting}
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec request_connect(node()) -> ok.
request_connect(Node) ->
    gen_server:cast(?SERVER, {request_connect, Node}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    net_kernel:monitor_nodes(true, [{node_type, visible}]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({request_connect, Node}, State) ->
    case maps:is_key(Node, State#state.pending) of
        true ->
            {noreply, State};
        false ->
            case lists:member(Node, nodes()) of
                true ->
                    %% Dist channel is already up (someone called
                    %% `net_kernel:connect_node/1' directly, or OTP
                    %% auto-connected via `Pid ! Msg'). No nodeup will
                    %% fire; resolve HyParView pending immediately.
                    mycelium_hyparview:peer_connected(Node, undefined),
                    {noreply, State};
                false ->
                    Self = self(),
                    spawn_link(fun() ->
                        case net_kernel:connect_node(Node) of
                            true ->
                                %% nodeup will be received
                                ok;
                            false ->
                                Self ! {connect_failed, Node};
                            ignored ->
                                ok
                        end
                    end),
                    Pending = maps:put(Node, connecting, State#state.pending),
                    {noreply, State#state{pending = Pending}}
            end
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node, _Info}, State) ->
    %% Only escalate to HyParView when WE asked for this connection via
    %% `request_connect/1' (i.e. it's part of the gossip topology).
    %% Other dist channels - opened by `net_kernel:connect_node/1' or by
    %% OTP's `Pid ! Msg' auto-connect - stay outside the active view; the
    %% active view tracks the bounded gossip topology, not raw dist.
    case maps:take(Node, State#state.pending) of
        {connecting, NewPending} ->
            mycelium_hyparview:peer_connected(Node, undefined),
            {noreply, State#state{pending = NewPending}};
        error ->
            {noreply, State}
    end;
handle_info({nodedown, Node, _Info}, State) ->
    Pending = maps:remove(Node, State#state.pending),
    mycelium_hyparview:peer_failed(Node, nodedown),
    {noreply, State#state{pending = Pending}};
handle_info({connect_failed, Node}, State) ->
    case maps:is_key(Node, State#state.pending) of
        true ->
            Pending = maps:remove(Node, State#state.pending),
            mycelium_hyparview:peer_failed(Node, connect_failed),
            {noreply, State#state{pending = Pending}};
        false ->
            {noreply, State}
    end;
%% Handle HyParView protocol messages
handle_info({'$mycelium_hyparview', From, Msg}, State) ->
    mycelium_protocol:handle_message({'$mycelium_hyparview', From, Msg}, self()),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
