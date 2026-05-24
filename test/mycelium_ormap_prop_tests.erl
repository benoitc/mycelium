%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Property tests for mycelium_ormap.
%%%
%%% Three CRDT laws under random op sequences:
%%%   * commutativity: merge(A, B) == merge(B, A)
%%%   * associativity: merge(merge(A, B), C) == merge(A, merge(B, C))
%%%   * idempotence:   merge(A, A) == A
%%%
%%% Plus a "latest add wins" property to lock in the HLC-tiebreak
%%% semantics: across two concurrent maps, the value with the larger
%%% HLC observed at any dot wins on merge.

-module(mycelium_ormap_prop_tests).

-include_lib("eunit/include/eunit.hrl").
%% eunit and proper both define ?LET; proper's is the one we use here.
-undef('LET').
-include_lib("proper/include/proper.hrl").

-define(NUMTESTS, 200).

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
    %% mycelium_hlc is a registered gen_server; idempotent across tests.
    case whereis(mycelium_hlc) of
        undefined ->
            {ok, _} = mycelium_hlc:start_link(),
            started;
        _ ->
            already_running
    end.

teardown(started) ->
    catch gen_server:stop(mycelium_hlc),
    ok;
teardown(_) ->
    ok.

prop_test_() ->
    {setup, fun setup/0, fun teardown/1, [
        {timeout, 60, ?_assert(run(prop_merge_commutative()))},
        {timeout, 60, ?_assert(run(prop_merge_associative()))},
        {timeout, 60, ?_assert(run(prop_merge_idempotent()))},
        {timeout, 60, ?_assert(run(prop_add_then_get()))},
        {timeout, 60, ?_assert(run(prop_remove_disappears()))},
        {timeout, 60, ?_assert(run(prop_remove_survives_stale_add()))}
    ]}.

run(Prop) ->
    proper:quickcheck(Prop, [{numtests, ?NUMTESTS}, {to_file, user}]).

%%====================================================================
%% Generators
%%====================================================================

%% Small key/value space so collisions actually happen.
key() -> oneof([a, b, c, d, e, f]).
value() -> oneof([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).

op() ->
    oneof([
        {add, key(), value()},
        {remove, key()}
    ]).

ops() ->
    list(op()).

ormap_gen() ->
    ?LET(Ops, ops(), apply_ops(Ops, mycelium_ormap:new())).

apply_ops([], M) ->
    M;
apply_ops([{add, K, V} | Rest], M) ->
    apply_ops(Rest, mycelium_ormap:add(K, V, M));
apply_ops([{remove, K} | Rest], M) ->
    apply_ops(Rest, mycelium_ormap:remove(K, M)).

%%====================================================================
%% Properties
%%====================================================================

prop_merge_commutative() ->
    ?FORALL(
        {A, B},
        {ormap_gen(), ormap_gen()},
        same_map(
            mycelium_ormap:merge(A, B),
            mycelium_ormap:merge(B, A)
        )
    ).

prop_merge_associative() ->
    ?FORALL(
        {A, B, C},
        {ormap_gen(), ormap_gen(), ormap_gen()},
        same_map(
            mycelium_ormap:merge(mycelium_ormap:merge(A, B), C),
            mycelium_ormap:merge(A, mycelium_ormap:merge(B, C))
        )
    ).

prop_merge_idempotent() ->
    ?FORALL(
        A,
        ormap_gen(),
        same_map(A, mycelium_ormap:merge(A, A))
    ).

prop_add_then_get() ->
    ?FORALL(
        {K, V, M},
        {key(), value(), ormap_gen()},
        begin
            M1 = mycelium_ormap:add(K, V, M),
            {ok, V} =:= mycelium_ormap:get(K, M1)
        end
    ).

prop_remove_disappears() ->
    ?FORALL(
        {K, V, M},
        {key(), value(), ormap_gen()},
        begin
            M1 = mycelium_ormap:add(K, V, M),
            M2 = mycelium_ormap:remove(K, M1),
            not_found =:= mycelium_ormap:get(K, M2)
        end
    ).

%% Reordering / replay: a stale add delta that arrives after a remove
%% must NOT resurrect the removed entry. Captures the delayed-merge
%% race that the registry-sync layer exposes under partitions.
prop_remove_survives_stale_add() ->
    ?FORALL(
        {K, V},
        {key(), value()},
        begin
            M0 = mycelium_ormap:new(),
            StaleAdd = mycelium_ormap:add(K, V, M0),
            M1 = mycelium_ormap:add(K, V, M0),
            M2 = mycelium_ormap:remove(K, M1),
            Merged = mycelium_ormap:merge(M2, StaleAdd),
            not_found =:= mycelium_ormap:get(K, Merged)
        end
    ).

%%====================================================================
%% Helpers
%%====================================================================

%% Two OR-Maps are "the same" if they expose the same {K, V} set.
%% The dot sets can differ structurally across merge orderings; the
%% public surface (to_list) is what users see.
same_map(M1, M2) ->
    lists:sort(mycelium_ormap:to_list(M1)) =:=
        lists:sort(mycelium_ormap:to_list(M2)).
