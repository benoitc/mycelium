%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-ifndef(MYCELIUM_HRL).
-define(MYCELIUM_HRL, true).

%% Peer representation (transport-agnostic)
-record(peer, {
    %% Node name
    id :: node(),
    %% IP address (for passive view)
    address :: inet:ip_address() | undefined,
    %% Distribution port
    port :: inet:port_number() | undefined,
    %% QUIC dist UDP port (when known)
    quic_port :: inet:port_number() | undefined,
    %% Currently in active view?
    connected :: boolean(),
    %% For NEIGHBOR protocol
    priority :: high | low,
    %% erlang:monotonic_time()
    last_seen :: integer() | undefined,
    %% Failure tracking for high churn handling

    %% Consecutive failures
    fail_count = 0 :: non_neg_integer(),
    %% erlang:monotonic_time() - skip until this time
    backoff_until :: integer() | undefined
}).

%% HyParView state (application layer)
-record(view_state, {
    %% Parameters

    %% Max active view (log n)
    active_size = 5 :: pos_integer(),
    %% Max passive view (c * log n)
    passive_size = 30 :: pos_integer(),
    %% Active Random Walk Length
    arwl = 6 :: pos_integer(),
    %% Passive Random Walk Length
    prwl = 3 :: pos_integer(),
    %% Nodes per shuffle
    shuffle_length = 8 :: pos_integer(),
    %% Shuffle interval (ms)
    shuffle_period = 10000 :: pos_integer(),

    %% Views

    %% Connected peers
    active_view = #{} :: #{node() => #peer{}},
    %% Known but not connected
    passive_view = #{} :: #{node() => #peer{}},

    %% Pending operations
    pending = #{} :: #{node() => {atom(), reference()}},

    %% Self
    self :: #peer{},

    %% Churn handling parameters

    %% Max failures before removal
    max_fail_count = 5 :: pos_integer(),
    %% Base backoff interval (ms)
    base_backoff_ms = 1000 :: pos_integer(),
    %% Max age for passive entries (5 min)
    passive_max_age_ms = 300000 :: pos_integer(),

    %% Churn tracking for adaptive shuffle

    %% Joins in current window
    recent_joins = 0 :: non_neg_integer(),
    %% Leaves in current window
    recent_leaves = 0 :: non_neg_integer(),
    %% Window start time
    churn_window_start :: integer() | undefined,
    %% Churn tracking window (30s)
    churn_window_ms = 30000 :: pos_integer()
}).

%% Service registry entry
%% Note: version removed - OR-Map CRDT handles conflict resolution via HLC dots
-record(service_entry, {
    name :: atom() | binary(),
    pid :: pid(),
    node :: node(),
    meta = #{} :: map()
}).

%% Routing request for overlay lookup
-record(route_req, {
    %% Service to find
    service_name :: atom() | binary(),
    %% Max hops remaining
    ttl = 5 :: non_neg_integer(),
    %% Node that initiated the request
    origin :: node(),
    %% Nodes already visited
    visited = [] :: [node()]
}).

%% Trusted peer key for Ed25519 authentication.
%% Identity is the public key fingerprint (SHA-256). The optional
%% `node' field is what the dist-key ETS store keys on while we still
%% bridge node-name -> key for the dist handshake.
-record(peer_key, {
    %% Last-seen node name (ETS key)
    node :: node() | undefined,
    %% SHA-256 hash of public key (32 bytes)
    fingerprint :: binary() | undefined,
    %% 32 bytes Ed25519 public key
    public_key :: binary(),
    %% erlang:system_time(millisecond)
    added_at :: integer(),
    %% erlang:system_time(millisecond)
    last_seen :: integer(),
    %% permanent = pre-configured, tofu = trust on first use
    trust_level :: permanent | tofu
}).

%% Crypto session state for encrypted distribution
-record(crypto_session, {
    %% 32 bytes ChaCha20 key for sending
    send_key :: binary(),
    %% 32 bytes ChaCha20 key for receiving
    recv_key :: binary(),
    %% Counter for send nonce
    send_nonce :: non_neg_integer(),
    %% Counter for receive nonce
    recv_nonce :: non_neg_integer()
}).

%% HyParView protocol messages (sent over Erlang distribution)
-type hyparview_msg() ::
    {join, Sender :: #peer{}}
    | {forward_join, NewPeer :: #peer{}, TTL :: integer(), Sender :: #peer{}}
    | {disconnect, Sender :: #peer{}}
    | {neighbor, Priority :: high | low, Sender :: #peer{}}
    | {neighbor_reply, Accept :: boolean(), Sender :: #peer{}}
    | {shuffle, TTL :: integer(), Peers :: [#peer{}], Sender :: #peer{}}
    | {shuffle_reply, Peers :: [#peer{}], Sender :: #peer{}}.

-endif.
