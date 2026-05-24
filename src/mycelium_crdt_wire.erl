%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Safe ingest of a peer-supplied OR-Map delta or snapshot.
%%%
%%% A `mycelium_replica' instance merges OR-Map entries that arrive over
%%% gossip. Feeding them to `mycelium_ormap:absorb_clock/1' / `merge/2'
%%% raw is unsafe: those walk every dot and HLC and take the max HLC of
%%% the dot set, so a malformed dot, a bad HLC, an empty dot map, or a
%%% non-map payload crashes the merging process (and a bad HLC crashes the
%%% shared `mycelium_hlc' server). This module is the one place that
%%% validates the WHOLE wrapper (entry shape + dot set + HLCs) before
%%% merging, so any callback module - including `mycelium_map' and any
%%% application-supplied one - can ingest gossip without that footgun.
%%%
%%% Wrapper validation is mandatory for safety; an optional leaf-value
%%% function lets the app additionally reject payloads it does not expect.

-module(mycelium_crdt_wire).

-include_lib("hlc/include/hlc.hrl").

-export([valid_entry/1, valid_entry/2, accept/2, ingest/3]).

%% Validates an application leaf value. Must not need to be total - a
%% throwing validator simply rejects the entry.
-type leaf_validator() :: fun((term()) -> boolean()).
-export_type([leaf_validator/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Wrapper-only validity: the entry shape `absorb_clock'/`merge'
%% require (a value with a non-empty dot map keyed by `{node(), HLC}', or
%% a tombstone carrying an HLC). Accepts any leaf value.
-spec valid_entry(term()) -> boolean().
valid_entry(Entry) ->
    valid_entry(Entry, fun(_) -> true end).

%% @doc Wrapper validity plus an application leaf-value check.
-spec valid_entry(term(), leaf_validator()) -> boolean().
valid_entry({value, Value, Dots}, LeafFun)
  when is_map(Dots), map_size(Dots) > 0 ->
    valid_dots(maps:keys(Dots)) andalso safe_bool(LeafFun, Value);
valid_entry({tombstone, #timestamp{}}, _LeafFun) ->
    true;
valid_entry(_Entry, _LeafFun) ->
    false.

%% @doc Keep only the entries that pass `valid_entry/2'. Guards the
%% top-level argument: a non-map payload (a malformed broadcast can
%% deliver any term) returns `#{}' rather than letting `maps:filter'
%% crash. The helper never crashes the caller on any peer-supplied term.
-spec accept(term(), leaf_validator()) -> mycelium_ormap:ormap().
accept(Map, LeafFun) when is_map(Map) ->
    maps:filter(fun(_K, V) -> valid_entry(V, LeafFun) end, Map);
accept(_NotAMap, _LeafFun) ->
    #{}.

%% @doc Validate, absorb the incoming clock, and merge into `Local'.
%% Returns `{Merged, Accepted}': `Merged' is the new local OR-Map and
%% `Accepted' is the validated sub-map of incoming entries - the keys that
%% changed, so the caller can update its projection and emit events
%% without rescanning the whole map. A non-map `Incoming' is a no-op:
%% `{Local, #{}}'. Works identically for a delta and a full-sync snapshot.
-spec ingest(mycelium_ormap:ormap(), term(), leaf_validator()) ->
    {mycelium_ormap:ormap(), mycelium_ormap:ormap()}.
ingest(Local, Incoming, LeafFun) ->
    Accepted = accept(Incoming, LeafFun),
    mycelium_ormap:absorb_clock(Accepted),
    {mycelium_ormap:merge(Local, Accepted), Accepted}.

%%====================================================================
%% Internal
%%====================================================================

valid_dots(Keys) ->
    lists:all(fun({N, #timestamp{}}) when is_atom(N) -> true;
                 (_)                                 -> false
              end, Keys).

%% Run a possibly-app-supplied leaf validator without letting it crash the
%% ingest of a whole delta: a throwing or non-boolean result rejects the
%% one entry.
safe_bool(Fun, Value) ->
    try Fun(Value) =:= true
    catch _:_ -> false
    end.
