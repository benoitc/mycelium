%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Smoke tests for mycelium_metrics. The instrument library is real
%%% (not mocked) so these also exercise the lazy persistent_term cache.

-module(mycelium_metrics_tests).

-include_lib("eunit/include/eunit.hrl").

with_instrument(Tests) ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(instrument),
            ok
        end,
        fun(_) -> ok end, Tests}.

emit_test_() ->
    with_instrument([
        ?_assertEqual(ok, mycelium_metrics:hyparview_event({peer_up, 'a@x'})),
        ?_assertEqual(ok, mycelium_metrics:hyparview_event({peer_down, 'a@x', graceful})),
        ?_assertEqual(ok, mycelium_metrics:hyparview_event({peer_down, 'a@x'})),
        ?_assertEqual(ok, mycelium_metrics:hyparview_event(joined)),
        ?_assertEqual(ok, mycelium_metrics:hyparview_event(left)),
        ?_assertEqual(ok, mycelium_metrics:hyparview_event({shuffle, 'b@x'})),
        ?_assertEqual(ok, mycelium_metrics:hyparview_event(unrecognised)),
        ?_assertEqual(ok, mycelium_metrics:auth_attempt(outgoing, ok, 42)),
        ?_assertEqual(ok, mycelium_metrics:auth_attempt(incoming, fail, 0)),
        ?_assertEqual(ok, mycelium_metrics:gossip_sent(0)),
        ?_assertEqual(ok, mycelium_metrics:gossip_sent(3)),
        ?_assertEqual(ok, mycelium_metrics:gossip_received('peer@x')),
        ?_assertEqual(ok, mycelium_metrics:ihave_sent(0)),
        ?_assertEqual(ok, mycelium_metrics:ihave_sent(2)),
        ?_assertEqual(ok, mycelium_metrics:graft_sent('peer@x')),
        ?_assertEqual(ok, mycelium_metrics:prune_sent('peer@x')),
        ?_assertEqual(ok, mycelium_metrics:gc_reap('peer@x')),
        ?_assertEqual(ok, mycelium_metrics:migrate_result('peer@x', ok)),
        ?_assertEqual(ok, mycelium_metrics:migrate_result('peer@x', fail))
    ]).

%% After at least one emit, the persistent_term cache must hold the
%% instrument so subsequent emits skip the create call.
cached_test_() ->
    with_instrument([
        ?_test(begin
            ok = mycelium_metrics:gc_reap('first@x'),
            ok = mycelium_metrics:gc_reap('second@x'),
            Key = {mycelium_metrics, instrument, <<"mycelium.dist_gc.reap">>, counter},
            ?assertNotEqual(undefined, persistent_term:get(Key, undefined))
        end)
    ]).

%% Emits must be no-ops (not crashes) even when the instrument app is
%% not running. Useful during early boot or in offline test contexts.
safe_when_instrument_missing_test_() ->
    {setup,
        fun() ->
            ok = application:stop(instrument),
            ok = clear_metrics_cache(),
            ok
        end,
        fun(_) -> ok end, [?_assertEqual(ok, mycelium_metrics:gc_reap('peer@x'))]}.

clear_metrics_cache() ->
    [
        persistent_term:erase(K)
     || {K, _} <- persistent_term:get(),
        is_tuple(K),
        tuple_size(K) > 0,
        element(1, K) =:= mycelium_metrics
    ],
    ok.
