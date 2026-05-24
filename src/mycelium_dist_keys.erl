%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_dist_keys).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    store_key/2,
    store_key_if_new/2,
    lookup_key/1,
    lookup_pin/1,
    delete_key/1,
    is_trusted/2,
    list_trusted/0,
    set_trust_mode/1,
    get_trust_mode/0,
    fingerprint/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("mycelium.hrl").

-define(SERVER, ?MODULE).
-define(TABLE, mycelium_dist_keys_tab).
-define(PUBLIC_KEY_SIZE, 32).

-record(state, {
    trust_mode :: strict | tofu,
    key_dir :: string()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the key storage server
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Store a public key for a node, overwriting any existing pin
%% unconditionally. This is an operator API and is NOT reachable from the
%% wire; the handshake path uses store_key_if_new/2, which refuses to
%% re-pin a different key. To rotate a peer's pin deliberately, delete_key/1
%% then store_key/2.
-spec store_key(node() | term(), binary()) -> ok | {error, term()}.
store_key(Node, PubKey) when byte_size(PubKey) =:= ?PUBLIC_KEY_SIZE ->
    gen_server:call(?SERVER, {store_key, Node, PubKey, permanent});
store_key(_, _) ->
    {error, invalid_key_size}.

%% @doc Store a key if no key exists for this node (TOFU mode)
-spec store_key_if_new(node() | term(), binary()) -> ok | {error, term()}.
store_key_if_new(Node, PubKey) when byte_size(PubKey) =:= ?PUBLIC_KEY_SIZE ->
    gen_server:call(?SERVER, {store_key_if_new, Node, PubKey});
store_key_if_new(_, _) ->
    {error, invalid_key_size}.

%% @doc Lookup the public key for a node
-spec lookup_key(node()) -> {ok, binary()} | {error, not_found}.
lookup_key(Node) ->
    case ets:lookup(?TABLE, Node) of
        [#peer_key{public_key = PubKey}] -> {ok, PubKey};
        [] -> {error, not_found}
    end.

%% @doc Tri-state pin lookup. Distinguishes "no pin recorded" from
%% "pin exists" so callers can refuse re-pin attempts. Accepts a node
%% atom or a (peer-supplied) name binary; a binary resolves through
%% binary_to_existing_atom so a lookup never mints a new atom. An
%% unknown name is `not_pinned'.
-spec lookup_pin(node() | binary() | term()) -> not_pinned | {pinned, binary()}.
lookup_pin(Node) when is_binary(Node) ->
    try binary_to_existing_atom(Node, utf8) of
        Atom -> lookup_pin(Atom)
    catch
        _:_ ->
            not_pinned
    end;
lookup_pin(Node) ->
    case ets:lookup(?TABLE, Node) of
        [#peer_key{public_key = PubKey}] -> {pinned, PubKey};
        [] -> not_pinned
    end.

%% @doc Delete a trusted key
-spec delete_key(node()) -> ok.
delete_key(Node) ->
    gen_server:call(?SERVER, {delete_key, Node}).

%% @doc Check if a node's public key is trusted. Thin wrapper around
%% lookup_pin/1 kept for back-compat with existing boolean callers.
-spec is_trusted(node(), binary()) -> boolean().
is_trusted(Node, PubKey) ->
    case lookup_pin(Node) of
        {pinned, PubKey} -> true;
        _ -> false
    end.

%% @doc List all trusted nodes
-spec list_trusted() -> [#peer_key{}].
list_trusted() ->
    ets:tab2list(?TABLE).

%% @doc Set trust mode (strict or tofu)
-spec set_trust_mode(strict | tofu) -> ok.
set_trust_mode(Mode) when Mode =:= strict; Mode =:= tofu ->
    gen_server:call(?SERVER, {set_trust_mode, Mode}).

%% @doc Get current trust mode
-spec get_trust_mode() -> strict | tofu.
get_trust_mode() ->
    gen_server:call(?SERVER, get_trust_mode).

%% @doc SHA-256 fingerprint of an Ed25519 public key. Pure helper for
%% diagnostics (logs, key-mismatch reports). The store/lookup API is
%% keyed by node atom, not by fingerprint.
-spec fingerprint(binary()) -> binary().
fingerprint(PubKey) when is_binary(PubKey), byte_size(PubKey) =:= ?PUBLIC_KEY_SIZE ->
    crypto:hash(sha256, PubKey).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for fast lookups
    ?TABLE = ets:new(?TABLE, [
        named_table,
        {keypos, #peer_key.node},
        public,
        {read_concurrency, true}
    ]),

    %% Get configuration
    TrustMode = application:get_env(mycelium, auth_trust_mode, tofu),
    KeyDir = application:get_env(mycelium, auth_key_dir, "data/keys"),

    %% Load trusted keys from disk
    load_trusted_keys(KeyDir),

    %% Initialize node keypair
    case mycelium_dist_auth:ensure_keypair() of
        ok -> ok;
        {error, Reason} -> error_logger:warning_msg("Failed to initialize keypair: ~p~n", [Reason])
    end,

    {ok, #state{trust_mode = TrustMode, key_dir = KeyDir}}.

handle_call({store_key, Node, PubKey, TrustLevel}, _From, State) ->
    Now = erlang:system_time(millisecond),
    Record = #peer_key{
        node = Node,
        public_key = PubKey,
        added_at = Now,
        last_seen = Now,
        trust_level = TrustLevel
    },
    true = ets:insert(?TABLE, Record),
    %% Persist to disk if permanent
    case TrustLevel of
        permanent -> save_trusted_key(State#state.key_dir, Node, PubKey);
        _ -> ok
    end,
    {reply, ok, State};
handle_call({store_key_if_new, Node, PubKey}, _From, State) ->
    case ets:lookup(?TABLE, Node) of
        [] ->
            %% No existing key - store as TOFU
            Now = erlang:system_time(millisecond),
            Record = #peer_key{
                node = Node,
                public_key = PubKey,
                added_at = Now,
                last_seen = Now,
                trust_level = tofu
            },
            true = ets:insert(?TABLE, Record),
            %% Persist TOFU keys too
            save_trusted_key(State#state.key_dir, Node, PubKey),
            {reply, ok, State};
        [#peer_key{public_key = PubKey}] ->
            %% Same key - update last_seen
            Now = erlang:system_time(millisecond),
            true = ets:update_element(?TABLE, Node, {#peer_key.last_seen, Now}),
            {reply, ok, State};
        [#peer_key{public_key = _OtherKey}] ->
            %% Different key - possible key rotation or attack
            error_logger:warning_msg(
                "Key mismatch for node ~p - existing key differs from presented key~n",
                [Node]
            ),
            {reply, {error, key_mismatch}, State}
    end;
handle_call({delete_key, Node}, _From, State) ->
    true = ets:delete(?TABLE, Node),
    delete_trusted_key(State#state.key_dir, Node),
    {reply, ok, State};
handle_call({set_trust_mode, Mode}, _From, State) ->
    {reply, ok, State#state{trust_mode = Mode}};
handle_call(get_trust_mode, _From, State) ->
    {reply, State#state.trust_mode, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Load trusted keys from the trusted/ subdirectory
load_trusted_keys(KeyDir) ->
    TrustedDir = filename:join(KeyDir, "trusted"),
    case file:list_dir(TrustedDir) of
        {ok, Files} ->
            lists:foreach(
                fun(File) ->
                    load_trusted_key_file(TrustedDir, File)
                end,
                Files
            );
        {error, enoent} ->
            %% Directory doesn't exist yet, create it
            filelib:ensure_dir(filename:join(TrustedDir, "dummy")),
            ok;
        {error, Reason} ->
            error_logger:warning_msg(
                "Failed to list trusted keys directory ~p: ~p~n",
                [TrustedDir, Reason]
            )
    end.

load_trusted_key_file(Dir, File) ->
    case filename:extension(File) of
        ".pub" ->
            FilePath = filename:join(Dir, File),
            case file:read_file(FilePath) of
                {ok, PubKey} when byte_size(PubKey) =:= ?PUBLIC_KEY_SIZE ->
                    %% Filenames are operator-controlled but apply the
                    %% same shape check the wire path uses, so a stray
                    %% file cannot inject arbitrary atoms at boot.
                    NodeBin = list_to_binary(filename:rootname(File)),
                    case mycelium_dist_protocol:validate_node_name(NodeBin) of
                        ok ->
                            Node = binary_to_atom(NodeBin, utf8),
                            Now = erlang:system_time(millisecond),
                            Record = #peer_key{
                                node = Node,
                                public_key = PubKey,
                                added_at = Now,
                                last_seen = Now,
                                trust_level = permanent
                            },
                            ets:insert(?TABLE, Record);
                        {error, _} ->
                            error_logger:warning_msg(
                                "Invalid node name in key file: ~p~n", [File]
                            )
                    end;
                {ok, _} ->
                    error_logger:warning_msg(
                        "Invalid key size in file: ~p~n", [File]
                    );
                {error, Reason} ->
                    error_logger:warning_msg(
                        "Failed to read key file ~p: ~p~n", [File, Reason]
                    )
            end;
        _ ->
            %% Ignore non-.pub files
            ok
    end.

%% @doc Save a trusted key to disk. Delegates to mycelium_file's
%% atomic+0600 helper.
save_trusted_key(KeyDir, Node, PubKey) ->
    TrustedDir = filename:join(KeyDir, "trusted"),
    case filelib:ensure_dir(filename:join(TrustedDir, "dummy")) of
        ok ->
            FileName = atom_to_list(Node) ++ ".pub",
            FilePath = filename:join(TrustedDir, FileName),
            case mycelium_file:write_secure(FilePath, PubKey) of
                ok ->
                    ok;
                {error, Reason} ->
                    error_logger:warning_msg(
                        "Failed to save trusted key for ~p: ~p~n",
                        [Node, Reason]
                    )
            end;
        {error, Reason} ->
            error_logger:warning_msg(
                "Failed to create trusted keys directory: ~p~n", [Reason]
            )
    end.

%% @doc Delete a trusted key from disk
delete_trusted_key(KeyDir, Node) ->
    TrustedDir = filename:join(KeyDir, "trusted"),
    FileName = atom_to_list(Node) ++ ".pub",
    FilePath = filename:join(TrustedDir, FileName),
    file:delete(FilePath).
