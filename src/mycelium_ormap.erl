%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_ormap).

%% A last-write-wins map with HLC-tagged tombstones, suitable for
%% replicating the service registry.
%%
%% Each key holds either a `value' entry (a payload plus the set of
%% dots that have written it) or a `tombstone' entry (an HLC marking
%% the time of removal). Merge keeps the entry with the greater
%% HLC (ties between concurrent values broken deterministically by the
%% node atom, so the merge is commutative and replicas never diverge); a
%% tombstone newer than any value wins, and an add newer than
%% any tombstone wins. The two outcomes are symmetric, so delayed
%% gossip cannot resurrect a removed entry, nor can a delayed remove
%% silently drop a fresher add.
%%
%% This is not a strict CRDT-textbook OR-Map (we do not track
%% per-add dot history through removes). For the service-registry
%% use case it gives the property that matters: register/unregister
%% ordering converges under reorder, partition, and replay.

-include_lib("hlc/include/hlc.hrl").

-export([new/0, add/3, remove/2, get/2, keys/1, to_list/1]).
-export([merge/2, is_empty/1]).
-export([get_entry/2]).
-export([absorb_clock/1, gc_tombstones/2]).

-type dot()             :: {node(), mycelium_hlc:timestamp()}.
-type value_entry()     :: {value, term(), #{dot() => true}}.
-type tombstone_entry() :: {tombstone, mycelium_hlc:timestamp()}.
-type entry()           :: value_entry() | tombstone_entry().
-type ormap()           :: #{term() => entry()}.

-export_type([ormap/0, dot/0, entry/0]).

%%====================================================================
%% API
%%====================================================================

%% Create a new empty OR-Map.
-spec new() -> ormap().
new() -> #{}.

%% Add a key-value pair with a fresh dot. If the current entry is a
%% tombstone newer than the add's HLC, the add is silently ignored;
%% the tombstone won.
-spec add(term(), term(), ormap()) -> ormap().
add(Key, Value, Map) ->
    Dot = {node(), mycelium_hlc:now()},
    DotHLC = dot_hlc(Dot),
    case maps:get(Key, Map, undefined) of
        undefined ->
            Map#{Key => {value, Value, #{Dot => true}}};
        {tombstone, T} ->
            case mycelium_hlc:compare(DotHLC, T) of
                gt -> Map#{Key => {value, Value, #{Dot => true}}};
                _  -> Map
            end;
        {value, _OldValue, Dots} ->
            Map#{Key => {value, Value, Dots#{Dot => true}}}
    end.

%% Remove a key by writing a tombstone tagged with the current HLC.
%% A subsequent add with a strictly greater HLC wins; a stale
%% delayed add never resurrects this entry.
-spec remove(term(), ormap()) -> ormap().
remove(Key, Map) ->
    Map#{Key => {tombstone, mycelium_hlc:now()}}.

%% Get the live value for a key. Tombstones return not_found.
-spec get(term(), ormap()) -> {ok, term()} | not_found.
get(Key, Map) ->
    case maps:get(Key, Map, undefined) of
        {value, Value, _Dots} -> {ok, Value};
        _                     -> not_found
    end.

%% Get the full entry (live value or tombstone) for a key. Used by
%% callers that need to inspect dot history; ordinary lookups should
%% use get/2.
-spec get_entry(term(), ormap()) -> {ok, entry()} | not_found.
get_entry(Key, Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> not_found;
        Entry     -> {ok, Entry}
    end.

%% Keys with live entries. Tombstones are not listed.
-spec keys(ormap()) -> [term()].
keys(Map) ->
    [K || {K, {value, _, _}} <- maps:to_list(Map)].

%% Live key-value pairs. Tombstones are skipped.
-spec to_list(ormap()) -> [{term(), term()}].
to_list(Map) ->
    [{K, V} || {K, {value, V, _Dots}} <- maps:to_list(Map)].

%% A map is empty when it has no live entries; tombstones do not
%% count.
-spec is_empty(ormap()) -> boolean().
is_empty(Map) ->
    lists:all(
        fun({_K, {value, _, _}}) -> false;
           (_)                   -> true
        end,
        maps:to_list(Map)
    ).

%% Advance the local HLC from every dot and tombstone in an incoming
%% map, so a value merged from a peer cannot later be out-ranked by a
%% locally generated timestamp that is behind it. Callers that merge a
%% whole received map must call this BEFORE `merge/2'. Callers that
%% reject some entries (e.g. on a freshness/skew check) must filter
%% first and absorb only the accepted sub-map: `mycelium_hlc:update/1'
%% accepts future timestamps, so absorbing a rejected far-future dot
%% would still move the clock forward.
-spec absorb_clock(ormap()) -> ok.
absorb_clock(Map) ->
    maps:foreach(
        fun(_Key, {value, _Val, Dots}) ->
                lists:foreach(
                    fun({_Node, HLC}) -> mycelium_hlc:update(HLC) end,
                    maps:keys(Dots));
           (_Key, {tombstone, HLC}) ->
                mycelium_hlc:update(HLC)
        end,
        Map
    ),
    ok.

%% Drop tombstones whose wall-clock time is older than `CutoffWallMs'.
%% Live value entries are never touched. This bounds the map for
%% high-churn callers (e.g. reminders) where every remove leaves a
%% tombstone. It is a best-effort shrink, not a correctness operation: a
%% re-arriving tombstone is idempotent, and the cutoff must be chosen so
%% no add older than a dropped tombstone can still be in flight.
-spec gc_tombstones(ormap(), non_neg_integer()) -> ormap().
gc_tombstones(Map, CutoffWallMs) ->
    maps:filter(
        fun(_Key, {tombstone, HLC}) ->
                mycelium_hlc:wall_time(HLC) >= CutoffWallMs;
           (_Key, _Value) ->
                true
        end,
        Map
    ).

%% Merge two OR-Maps. Commutative, associative, idempotent.
-spec merge(ormap(), ormap()) -> ormap().
merge(Map1, Map2) ->
    Keys = lists:usort(maps:keys(Map1) ++ maps:keys(Map2)),
    lists:foldl(
        fun(Key, Acc) ->
            Acc#{Key => merge_entry(maps:get(Key, Map1, undefined),
                                    maps:get(Key, Map2, undefined))}
        end,
        #{},
        Keys
    ).

%%====================================================================
%% Internal
%%====================================================================

merge_entry(undefined, E)                    -> E;
merge_entry(E, undefined)                    -> E;
merge_entry({value, V1, D1}, {value, V2, D2}) ->
    MergedDots = maps:merge(D1, D2),
    %% Last-write-wins by the maximum dot. Compare the full dot
    %% ({Node, HLC}), not just the HLC: when two concurrent writes land on
    %% the same HLC (common on a single host, same millisecond) the node
    %% atom breaks the tie. That keeps the merge commutative, so every
    %% replica resolves the conflict to the same value instead of diverging
    %% on argument order.
    case dot_compare(max_dot(D1), max_dot(D2)) of
        gt -> {value, V1, MergedDots};
        _  -> {value, V2, MergedDots}
    end;
merge_entry({tombstone, T1}, {tombstone, T2}) ->
    case mycelium_hlc:compare(T1, T2) of
        gt -> {tombstone, T1};
        _  -> {tombstone, T2}
    end;
merge_entry({tombstone, T} = Tomb, {value, _, D} = V) ->
    case mycelium_hlc:compare(T, max_hlc(D)) of
        gt -> Tomb;
        _  -> V
    end;
merge_entry({value, _, D} = V, {tombstone, T} = Tomb) ->
    case mycelium_hlc:compare(T, max_hlc(D)) of
        gt -> Tomb;
        _  -> V
    end.

dot_hlc({_Node, HLC}) -> HLC.

%% Total order on dots: HLC first, then the node atom as a deterministic
%% tiebreak so two writes with an equal HLC resolve identically on every
%% replica (the merge stays commutative).
dot_compare({Na, Ha}, {Nb, Hb}) ->
    case mycelium_hlc:compare(Ha, Hb) of
        eq    -> compare_node(Na, Nb);
        Other -> Other
    end.

compare_node(N, N)                -> eq;
compare_node(Na, Nb) when Na > Nb -> gt;
compare_node(_, _)                -> lt.

%% The greatest dot in a non-empty dot set, by dot_compare/2.
max_dot(Dots) ->
    [First | Rest] = maps:keys(Dots),
    lists:foldl(
        fun(Dot, Acc) ->
            case dot_compare(Dot, Acc) of gt -> Dot; _ -> Acc end
        end,
        First,
        Rest
    ).

%% Maximum HLC across a non-empty dot set.
max_hlc(Dots) ->
    [First | Rest] = maps:keys(Dots),
    lists:foldl(
        fun({_, HLC}, Acc) ->
            case mycelium_hlc:compare(HLC, Acc) of
                gt -> HLC;
                _  -> Acc
            end
        end,
        dot_hlc(First),
        Rest
    ).
