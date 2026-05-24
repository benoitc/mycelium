%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Seed bootstrap: auto-joins the cluster from the configured
%%% `contact_nodes' (the seeds) at boot, so a node forms a cluster from
%%% config without a manual `mycelium:join/1'. The seed itself (empty
%%% `contact_nodes') needs no bootstrap, so this worker idles there.
%%%
%%% While the node has an empty active view it periodically asks each
%%% contact to let it in (`mycelium:join/1' is a non-blocking request to
%%% connect; the seed's address is resolved through the discovery chain).
%%% It keeps checking every `contact_retry_ms' so a seed that comes up
%%% late, or a node that loses all peers, re-joins on its own. Once the
%%% active view is non-empty the check is a cheap no-op.
-module(mycelium_bootstrap).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_RETRY_MS, 5000).
%% Defer the first attempt so the listener and discovery are up.
-define(INITIAL_DELAY_MS, 1000).

-record(state, {
    contacts :: [node()],
    retry_ms :: pos_integer()
}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    case contacts() of
        [] ->
            %% A seed (or a node configured to be joined to) has nothing to
            %% bootstrap from; do not run.
            ignore;
        Contacts ->
            Retry = application:get_env(mycelium, contact_retry_ms, ?DEFAULT_RETRY_MS),
            erlang:send_after(min(?INITIAL_DELAY_MS, Retry), self(), join_contacts),
            {ok, #state{contacts = Contacts, retry_ms = Retry}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(join_contacts, #state{contacts = Contacts, retry_ms = Retry} = State) ->
    case mycelium:active_view() of
        [_ | _] ->
            %% Already in a cluster; nothing to do this tick.
            ok;
        [] ->
            _ = [mycelium:join(C) || C <- Contacts]
    end,
    erlang:send_after(Retry, self(), join_contacts),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Configured seeds, minus ourselves (a node may ship a cluster-wide
%% contact list that includes its own name).
contacts() ->
    [N || N <- application:get_env(mycelium, contact_nodes, []), N =/= node()].
