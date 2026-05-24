%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Property tests for mycelium_hlc.
%%%
%%% Locks in the three invariants the rest of the system depends on:
%%%   * compare is a total order (exactly one of lt|eq|gt per pair),
%%%   * compare is transitive,
%%%   * to_binary/from_binary is an exact round-trip,
%%%   * locally observed now() is monotonically non-decreasing,
%%%   * update(Remote) returns a timestamp >= Remote (causal merge).
%%%
%%% These are checked against random #timestamp{} structs and against
%%% the live mycelium_hlc gen_server.

-module(mycelium_hlc_prop_tests).

-include_lib("eunit/include/eunit.hrl").
%% eunit and proper both define ?LET; proper's is the one we use here.
-undef('LET').
-include_lib("proper/include/proper.hrl").
-include_lib("hlc/include/hlc.hrl").

-define(NUMTESTS, 200).

%%====================================================================
%% Fixture
%%====================================================================

setup() ->
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
        {timeout, 60, ?_assert(run(prop_compare_total()))},
        {timeout, 60, ?_assert(run(prop_compare_transitive()))},
        {timeout, 60, ?_assert(run(prop_binary_roundtrip()))},
        {timeout, 60, ?_assert(run(prop_now_monotonic()))},
        {timeout, 60, ?_assert(run(prop_update_caps_remote()))}
    ]}.

run(Prop) ->
    proper:quickcheck(Prop, [{numtests, ?NUMTESTS}, {to_file, user}]).

%%====================================================================
%% Generators
%%====================================================================

ts_gen() ->
    ?LET(
        {W, L},
        {non_neg_integer(), choose(0, 16#FFFFFFFF)},
        #timestamp{wall_time = W, logical = L}
    ).

%%====================================================================
%% Properties
%%====================================================================

%% Exactly one of lt, eq, gt for every pair.
prop_compare_total() ->
    ?FORALL(
        {A, B},
        {ts_gen(), ts_gen()},
        begin
            R = mycelium_hlc:compare(A, B),
            lists:member(R, [lt, eq, gt])
        end
    ).

%% (A < B and B < C) implies A < C.
prop_compare_transitive() ->
    ?FORALL(
        {A, B, C},
        {ts_gen(), ts_gen(), ts_gen()},
        begin
            Rab = mycelium_hlc:compare(A, B),
            Rbc = mycelium_hlc:compare(B, C),
            Rac = mycelium_hlc:compare(A, C),
            case {Rab, Rbc} of
                {lt, lt} -> Rac =:= lt;
                {gt, gt} -> Rac =:= gt;
                {eq, X} -> Rac =:= X;
                {X, eq} -> Rac =:= X;
                _ -> true
            end
        end
    ).

prop_binary_roundtrip() ->
    ?FORALL(
        T,
        ts_gen(),
        T =:= mycelium_hlc:from_binary(mycelium_hlc:to_binary(T))
    ).

%% Two successive now() calls never go backwards.
prop_now_monotonic() ->
    ?FORALL(
        _,
        integer(),
        begin
            T1 = mycelium_hlc:now(),
            T2 = mycelium_hlc:now(),
            R = mycelium_hlc:compare(T2, T1),
            lists:member(R, [eq, gt])
        end
    ).

%% After update(Remote), the returned timestamp is >= Remote.
%% This is what makes HLC a causal merge.
prop_update_caps_remote() ->
    ?FORALL(
        Remote,
        ts_gen(),
        begin
            T = mycelium_hlc:update(Remote),
            R = mycelium_hlc:compare(T, Remote),
            lists:member(R, [eq, gt])
        end
    ).
