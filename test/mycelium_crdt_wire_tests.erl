%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Unit tests for mycelium_crdt_wire - the safe gossip-ingest surface.
%%% Pure (no HLC server): covers wrapper validation, the leaf hook, and the
%%% non-map guards. Merge behaviour with live entries is covered in
%%% mycelium_map_SUITE (which has the HLC server up).
-module(mycelium_crdt_wire_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("hlc/include/hlc.hrl").

ts() -> #timestamp{wall_time = erlang:system_time(millisecond), logical = 0}.
dots() -> #{{node(), ts()} => true}.

valid_value_test() ->
    ?assert(mycelium_crdt_wire:valid_entry({value, anything, dots()})).

valid_tombstone_test() ->
    ?assert(mycelium_crdt_wire:valid_entry({tombstone, ts()})).

rejects_empty_dot_map_test() ->
    ?assertNot(mycelium_crdt_wire:valid_entry({value, v, #{}})).

rejects_non_map_dots_test() ->
    ?assertNot(mycelium_crdt_wire:valid_entry({value, v, not_a_map})).

rejects_malformed_dot_key_test() ->
    ?assertNot(mycelium_crdt_wire:valid_entry({value, v, #{bad_key => true}})).

rejects_malformed_tombstone_test() ->
    ?assertNot(mycelium_crdt_wire:valid_entry({tombstone, not_a_timestamp})).

rejects_garbage_test() ->
    ?assertNot(mycelium_crdt_wire:valid_entry(garbage)),
    ?assertNot(mycelium_crdt_wire:valid_entry({value, v})),
    ?assertNot(mycelium_crdt_wire:valid_entry(42)).

leaf_validator_test() ->
    IsInt = fun erlang:is_integer/1,
    ?assert(mycelium_crdt_wire:valid_entry({value, 7, dots()}, IsInt)),
    ?assertNot(mycelium_crdt_wire:valid_entry({value, notint, dots()}, IsInt)).

leaf_validator_that_throws_rejects_test() ->
    Boom = fun(_) -> error(boom) end,
    ?assertNot(mycelium_crdt_wire:valid_entry({value, x, dots()}, Boom)).

accept_filters_invalid_test() ->
    Map = #{a => {value, 1, dots()},
            b => {tombstone, ts()},
            c => garbage,
            d => {value, 2, #{}}},
    Acc = mycelium_crdt_wire:accept(Map, fun(_) -> true end),
    ?assertEqual([a, b], lists:sort(maps:keys(Acc))).

accept_non_map_returns_empty_test() ->
    ?assertEqual(#{}, mycelium_crdt_wire:accept(not_a_map, fun(_) -> true end)),
    ?assertEqual(#{}, mycelium_crdt_wire:accept(garbage, fun(_) -> true end)).

ingest_non_map_is_noop_test() ->
    Local = #{x => {value, 1, dots()}},
    %% Accepted is empty -> absorb_clock(#{}) needs no HLC server, and
    %% Merged is Local unchanged.
    ?assertEqual({Local, #{}},
                 mycelium_crdt_wire:ingest(Local, not_a_map, fun(_) -> true end)).
