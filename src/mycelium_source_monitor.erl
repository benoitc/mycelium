%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Keep a gen_server's event subscriptions alive across restarts of the
%%% SOURCE process it subscribes to.
%%%
%%% Mycelium's event sources (`mycelium_plumtree', `mycelium_hyparview_events',
%%% `mycelium_shard', `mycelium_service_events') keep their subscriber list in
%%% ephemeral gen_server state. A subscriber that registers once in `init/1'
%%% is therefore silently dropped if the source crashes and is restarted by
%%% its supervisor (the source and its subscribers live in different subtrees
%%% under a `one_for_one' top-level supervisor, so the subscriber is not
%%% restarted alongside it).
%%%
%%% This helper runs in the SUBSCRIBER's process. It monitors each source and
%%% re-subscribes when the source goes down and comes back. A subscriber:
%%%
%%%   - calls `start/1' in `init/1' and stores the returned watch state,
%%%   - routes `{'DOWN', Ref, process, _, _}' through `down/2' (falling back to
%%%     its own subscriber-cleanup handling when the ref is not a watched
%%%     source), and
%%%   - handles `{?MODULE, retry, Source}' by calling `retry/2'.
%%%
%%% Subscription is uniform across sources: `Source:subscribe(self())'.
-module(mycelium_source_monitor).

-export([start/1, retry/2, down/2]).
-export_type([watch/0]).

%% How long to wait before retrying a subscribe while the source is down.
-define(RETRY_MS, 250).

%% Source module -> live monitor reference. Keyed by source so the helper
%% is idempotent per source: a stale retry tick for an already-watched
%% source is a no-op, so duplicate monitors cannot accumulate.
-type watch() :: #{atom() => reference()}.

%%====================================================================
%% API
%%====================================================================

-spec start([atom()]) -> watch().
start(Sources) ->
    lists:foldl(fun add/2, #{}, Sources).

%% Handle a `{?MODULE, retry, Source}' tick. Same idempotent path as the
%% initial subscribe.
-spec retry(atom(), watch()) -> watch().
retry(Source, Watch) ->
    add(Source, Watch).

%% Handle a process `'DOWN''. `{down, Source, Watch1}' if `Ref' was a watched
%% source (a retry is scheduled); `ignore' otherwise, so the caller's own
%% subscriber-monitor handling runs.
-spec down(reference(), watch()) -> {down, atom(), watch()} | ignore.
down(Ref, Watch) ->
    case [S || {S, R} <- maps:to_list(Watch), R =:= Ref] of
        [Source] ->
            sched(Source),
            {down, Source, maps:remove(Source, Watch)};
        [] ->
            ignore
    end.

%%====================================================================
%% Internal
%%====================================================================

%% Idempotent on Source: if already watching (a live monitor exists), do
%% nothing.
add(Source, Watch) ->
    case maps:is_key(Source, Watch) of
        true -> Watch;
        false -> do_subscribe(Source, Watch)
    end.

do_subscribe(Source, Watch) ->
    case whereis(Source) of
        undefined ->
            sched(Source),
            Watch;
        Pid ->
            Ref = monitor(process, Pid),
            %% The source may die between whereis/1 and the call: drop the
            %% monitor (flushing its queued DOWN) and retry, rather than
            %% leaving a dangling ref and waiting on the DOWN.
            try Source:subscribe(self()) of
                ok -> Watch#{Source => Ref}
            catch
                _:_ ->
                    demonitor(Ref, [flush]),
                    sched(Source),
                    Watch
            end
    end.

sched(Source) ->
    erlang:send_after(?RETRY_MS, self(), {?MODULE, retry, Source}).
