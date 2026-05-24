%%% -*- erlang -*-
%%%
%%% Mycelium Path Stats
%%%
%%% Thin accessor over upstream `quic:get_path_stats/1' for the
%%% per-peer dist QUIC connection. Used by `mycelium_router' to
%%% rank candidate next-hops by srtt.
%%%
%%% Copyright (c) 2026 Benoit Chesneau
%%% Apache License 2.0

-module(mycelium_path_stats).

-export([summary/1, srtt/1, connection/1]).

-type summary() :: #{
    srtt => non_neg_integer(),
    latest_rtt => non_neg_integer(),
    min_rtt => non_neg_integer(),
    rtt_var => non_neg_integer(),
    cwnd => non_neg_integer(),
    bytes_in_flight => non_neg_integer(),
    in_recovery => boolean(),
    congested => boolean()
}.

-export_type([summary/0]).

%% @doc Return the full path-stats map for `Node'. Returns
%% `{error, not_connected}' if there is no current dist connection.
%%
%% NB: `quic_dist:get_controller/1' returns the dist controller
%% gen_statem, not the underlying QUIC connection. `quic:get_path_stats/1'
%% expects the connection. Until upstream exposes a get-path-stats
%% wrapper on the dist controller itself, we extract the conn pid by
%% inspecting the controller's gen_statem state. The accessor is
%% structurally defensive: it first tries the known field position,
%% then falls back to probing every pid in the state tuple with a
%% test call to `quic:get_path_stats/1'.
-spec summary(node()) -> {ok, summary()} | {error, term()}.
summary(Node) when is_atom(Node) ->
    case connection(Node) of
        {ok, Conn} -> quic:get_path_stats(Conn);
        Err -> Err
    end.

%% @doc Resolve a peer node to the underlying QUIC connection pid.
%% Used by `summary/1' here and by `mycelium:migrate_peer/1,2' for
%% RFC 9000 §9 path migration. Returns `{error, not_connected}' if
%% there is no current dist channel; `{error, no_conn}' if the
%% controller is alive but the connection extraction fails.
-spec connection(node()) -> {ok, pid()} | {error, term()}.
connection(Node) when is_atom(Node) ->
    case quic_dist:get_controller(Node) of
        {ok, DistCtrl} -> extract_conn(DistCtrl);
        Err -> Err
    end.

%% Reach into the dist controller's gen_statem state and pluck the
%% connection pid. Strategy:
%%
%%   1. Try the known field position (2 in upstream `#state{}' as of
%%      `quic_dist_controller.erl', conn :: pid()).
%%   2. If that does not look like a live QUIC connection, scan every
%%      element of the state tuple for a live pid that
%%      `quic:get_path_stats/1' answers for.
%%
%% Step 2 makes us robust to upstream record reorderings without
%% making mycelium depend on the internal record header.
extract_conn(DistCtrl) ->
    try
        State = sys:get_state(DistCtrl, 1000),
        case tuple_size(State) >= 2 of
            true -> probe_pid(State, element(2, State));
            false -> {error, no_conn}
        end
    catch
        _:_ -> {error, no_conn}
    end.

probe_pid(State, FastPath) ->
    case is_live_pid(FastPath) of
        true ->
            %% Fast path: position-2 is a live pid, trust it. The
            %% process that owns the connection is the only thing
            %% quic:get_path_stats/1 talks to anyway.
            {ok, FastPath};
        false ->
            %% Upstream record may have been reshuffled; scan the
            %% rest of the tuple for a pid that actually answers
            %% get_path_stats.
            scan_state(State)
    end.

scan_state(State) ->
    Size = tuple_size(State),
    %% Position 1 is the record tag, skip it; scan the rest.
    case find_first(2, Size, State) of
        {ok, _Pid} = Ok -> Ok;
        none -> {error, no_conn}
    end.

find_first(Pos, Size, _State) when Pos > Size ->
    none;
find_first(Pos, Size, State) ->
    Candidate = element(Pos, State),
    case answers_get_path_stats(Candidate) of
        true -> {ok, Candidate};
        false -> find_first(Pos + 1, Size, State)
    end.

is_live_pid(P) when is_pid(P) -> erlang:is_process_alive(P);
is_live_pid(_) -> false.

answers_get_path_stats(Pid) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            try quic:get_path_stats(Pid) of
                {ok, _} -> true;
                _ -> false
            catch
                _:_ -> false
            end;
        false ->
            false
    end;
answers_get_path_stats(_) ->
    false.

%% @doc Smoothed RTT in microseconds, or `{error, _}' if unavailable.
-spec srtt(node()) -> {ok, non_neg_integer()} | {error, term()}.
srtt(Node) ->
    case summary(Node) of
        {ok, #{srtt := Us}} -> {ok, Us};
        {error, _} = Err -> Err
    end.
