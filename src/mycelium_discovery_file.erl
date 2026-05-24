%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_discovery_file).
-behaviour(quic_discovery).

%% Filesystem-backed discovery: one file per registered node under
%% the configured `discovery_dir'. Useful for nodes sharing the same
%% host (or filesystem) so they auto-find each other without epmd.
%%
%% On-disk format (`<discovery_dir>/<node>.endpoint'):
%%
%%   {"<host>", <port>}.
%%
%% Read with `file:consult/1', written via tmpfile + `file:rename/2'
%% so concurrent readers never see a half-written file.
%%
%% `discovery_dir' is read from
%%   application:get_env(mycelium, discovery_dir, "data/discovery").
%%
%% Stale entries: when a node restarts the file is rewritten in place,
%% so a stale entry causes at worst one failed connect followed by a
%% successful retry. We don't try to garbage-collect via timestamps;
%% callers can clean up explicitly with `unregister/1' if needed.

-export([init/1, register/3, lookup/2, list_nodes/1]).
-export([unregister/1]).

%%====================================================================
%% quic_discovery callbacks
%%====================================================================

init(_Opts) ->
    Dir = discovery_dir(),
    case filelib:ensure_dir(filename:join(Dir, "dummy")) of
        ok -> {ok, Dir};
        {error, R} -> {error, {discovery_dir_not_writable, Dir, R}}
    end.

register(Node, Port, _State) ->
    Dir = discovery_dir(),
    NodeStr = node_to_string(Node),
    Host = host_of(NodeStr),
    Term = io_lib:format("~p.~n", [{Host, Port}]),
    Path = endpoint_path(Dir, NodeStr),
    Tmp = Path ++ ".tmp",
    ok = filelib:ensure_dir(Path),
    case file:write_file(Tmp, iolist_to_binary(Term)) of
        ok ->
            case file:rename(Tmp, Path) of
                ok ->
                    {ok, Dir};
                Error ->
                    _ = file:delete(Tmp),
                    Error
            end;
        Error ->
            Error
    end.

lookup(Node, _Host) ->
    Path = endpoint_path(discovery_dir(), node_to_string(Node)),
    case file:consult(Path) of
        {ok, [{Host, Port}]} when is_integer(Port) ->
            {ok, {Host, Port}};
        {ok, _} ->
            {error, malformed_endpoint};
        {error, enoent} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

list_nodes(_Host) ->
    Dir = discovery_dir(),
    case file:list_dir(Dir) of
        {ok, Files} ->
            Nodes = lists:filtermap(
                fun(F) -> read_entry(filename:join(Dir, F)) end,
                lists:filter(fun is_endpoint_file/1, Files)
            ),
            {ok, Nodes};
        {error, enoent} ->
            {ok, []};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Public helpers
%%====================================================================

%% @doc Remove this node's endpoint file. Useful in `init:stop'-style
%% shutdown hooks.
-spec unregister(node() | string() | binary()) -> ok | {error, term()}.
unregister(Node) ->
    case file:delete(endpoint_path(discovery_dir(), node_to_string(Node))) of
        ok -> ok;
        {error, enoent} -> ok;
        Err -> Err
    end.

%%====================================================================
%% Internal
%%====================================================================

discovery_dir() ->
    application:get_env(mycelium, discovery_dir, "data/discovery").

%% Normalise atoms, binaries, and strings to a single string form
%% so callers (upstream quic_dist + mycelium_app) can pass either.
node_to_string(Node) when is_atom(Node) -> atom_to_list(Node);
node_to_string(Node) when is_binary(Node) -> binary_to_list(Node);
node_to_string(Node) when is_list(Node) -> Node.

endpoint_path(Dir, NodeStr) when is_list(NodeStr) ->
    filename:join(Dir, NodeStr ++ ".endpoint").

host_of(NodeStr) when is_list(NodeStr) ->
    case string:split(NodeStr, "@") of
        [_, Host] -> Host;
        _ -> "127.0.0.1"
    end.

is_endpoint_file(F) ->
    case lists:reverse(F) of
        "tniopdne." ++ _ -> true;
        _ -> false
    end.

read_entry(Path) ->
    Base = filename:basename(Path, ".endpoint"),
    BaseBin = list_to_binary(Base),
    %% Validate the filename matches a well-formed `name@host' atom
    %% before minting it. discovery_dir is potentially shared or
    %% writable; without this, crafted filenames could exhaust the
    %% atom table. Prefer an existing atom to avoid minting altogether.
    case mycelium_dist_protocol:validate_node_name(BaseBin) of
        ok ->
            case file:consult(Path) of
                {ok, [{_Host, Port}]} when is_integer(Port) ->
                    {true, {materialise_node(Base, BaseBin), Port}};
                _ ->
                    false
            end;
        {error, _} ->
            false
    end.

materialise_node(Base, BaseBin) ->
    try
        list_to_existing_atom(Base)
    catch
        error:badarg ->
            binary_to_atom(BaseBin, utf8)
    end.
