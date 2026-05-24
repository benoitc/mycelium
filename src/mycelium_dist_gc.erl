%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Idle dist-channel garbage collector.
%%%
%%% Mycelium decouples Erlang dist channels from the HyParView active
%%% view: `Pid ! Msg' to any cluster node auto-connects on demand.
%%% Without bounded fan-out, every cross-cluster send leaves a channel
%%% open and the cluster drifts toward full mesh. This GC reaps idle
%%% channels so HyParView's O(log n) gossip topology actually bounds
%%% the natural connection count.
%%%
%%% Predicate (a node is reaped when ALL of these hold):
%%%   1. not in `mycelium:active_view()' (HyParView would re-bind it
%%%      otherwise),
%%%   2. `quic_dist:list_streams(Node) =:= []' (no live user stream
%%%      rides this channel),
%%%   3. age >= `dist_gc_min_age_ms' (avoid reaping a channel right
%%%      after a brief send/receive that completed milliseconds ago).
%%%
%%% Always runs. Sweep period and min-age are tunable; the GC itself
%%% has no enable/disable env. Removing it would break the architectural
%%% claim of bounded fan-out, so it's not optional.

-module(mycelium_dist_gc).
-behaviour(gen_server).

-export([start_link/0]).

%% Test/observability hooks
-export([sweep_now/0, get_age_ms/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(AGES, mycelium_dist_gc_ages).

-define(DEFAULT_SWEEP_PERIOD_MS, 60000).
-define(DEFAULT_MIN_AGE_MS, 300000).

-record(state, {
    sweep_period_ms :: pos_integer(),
    min_age_ms :: pos_integer(),
    timer_ref :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Run a sweep synchronously. Useful for tests that don't want to wait
%% for the timer.
-spec sweep_now() -> ok.
sweep_now() ->
    gen_server:call(?SERVER, sweep_now).

%% Get how long Node has been visible on this dist (ms), or
%% `not_tracked' if we haven't seen a nodeup for it (e.g. the GC
%% started after the connection formed).
-spec get_age_ms(node()) -> pos_integer() | not_tracked.
get_age_ms(Node) ->
    case ets:lookup(?AGES, Node) of
        [{Node, Since}] ->
            erlang:monotonic_time(millisecond) - Since;
        [] ->
            not_tracked
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    SweepPeriod = application:get_env(
        mycelium,
        dist_gc_sweep_period_ms,
        ?DEFAULT_SWEEP_PERIOD_MS
    ),
    MinAge = application:get_env(
        mycelium,
        dist_gc_min_age_ms,
        ?DEFAULT_MIN_AGE_MS
    ),
    ?AGES = ets:new(?AGES, [
        named_table,
        public,
        set,
        {read_concurrency, true}
    ]),
    %% Record any node already connected at boot (we may have started
    %% after the dist channel formed). They get the current time as
    %% their first-seen, which is conservative: they won't be reaped
    %% until they pass the min-age threshold from now.
    Now = erlang:monotonic_time(millisecond),
    [ets:insert(?AGES, {N, Now}) || N <- nodes()],
    net_kernel:monitor_nodes(true, [{node_type, visible}]),
    TRef = erlang:send_after(SweepPeriod, self(), sweep),
    {ok, #state{
        sweep_period_ms = SweepPeriod,
        min_age_ms = MinAge,
        timer_ref = TRef
    }}.

handle_call(sweep_now, _From, State) ->
    do_sweep(State),
    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node, _Info}, State) ->
    ets:insert_new(?AGES, {Node, erlang:monotonic_time(millisecond)}),
    {noreply, State};
handle_info({nodedown, Node, _Info}, State) ->
    ets:delete(?AGES, Node),
    {noreply, State};
handle_info(sweep, State = #state{sweep_period_ms = Period}) ->
    do_sweep(State),
    TRef = erlang:send_after(Period, self(), sweep),
    {noreply, State#state{timer_ref = TRef}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    catch net_kernel:monitor_nodes(false, [{node_type, visible}]),
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_sweep(#state{min_age_ms = MinAge}) ->
    Active = active_view_safe(),
    Now = erlang:monotonic_time(millisecond),
    [maybe_reap(N, Active, MinAge, Now) || N <- nodes()],
    ok.

maybe_reap(Node, Active, MinAge, Now) ->
    case lists:member(Node, Active) of
        true ->
            ok;
        false ->
            case has_live_streams(Node) of
                true ->
                    ok;
                false ->
                    case is_old_enough(Node, MinAge, Now) of
                        false ->
                            ok;
                        true ->
                            _ = erlang:disconnect_node(Node),
                            ets:delete(?AGES, Node),
                            mycelium_metrics:gc_reap(Node),
                            ok
                    end
            end
    end.

active_view_safe() ->
    %% mycelium app may not be fully started yet during early-boot
    %% sweeps; treat as empty active view rather than crash.
    try
        mycelium:active_view()
    catch
        _:_ -> []
    end.

has_live_streams(Node) ->
    try quic_dist:list_streams(Node) of
        [] -> false;
        L when is_list(L) -> true
    catch
        _:_ ->
            %% If we cannot ask, be conservative and keep the channel.
            true
    end.

is_old_enough(Node, MinAge, Now) ->
    case ets:lookup(?AGES, Node) of
        [{Node, Since}] -> (Now - Since) >= MinAge;
        [] -> false
    end.
