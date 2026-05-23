%%% -*- erlang -*-
%%%
%%% Mycelium Distribution Auth Stream
%%%
%%% Runs the Ed25519 challenge-response identity protocol over a pair
%%% of unidirectional QUIC streams, before the Erlang dist handshake.
%%% Each side opens its own unidirectional stream and writes its half
%%% of the protocol on it; reads are done from the peer's stream.
%%%
%%% Uni stream IDs (2,6,10,... client-init; 3,7,11,... server-init) do
%%% not collide with the bidi stream numbering the controller uses for
%%% the dist control stream (bidi 0, hardcoded server-side).
%%%
%%% Wire format on each stream: each protocol message from
%%% `mycelium_dist_protocol' is preceded by a 2-byte big-endian length
%%% prefix.
%%%
%%% Copyright (c) 2026 Benoit Chesneau
%%% Apache License 2.0
%%%

-module(mycelium_dist_auth_stream).

-export([
    authenticate_outgoing/3,
    authenticate_incoming/2
]).

%% 2-byte big-endian length prefix used during the auth handshake.
-define(LEN_SIZE, 2).

%% The handshake runs in a single linear process (the quic_dist
%% gatekeeper/setup process). The TLS channel binding is constant for
%% the whole handshake, so it is stashed in the process dictionary
%% rather than threaded through every send/recv frame, mirroring the
%% `mycelium_dial_target' stash that mycelium_dist:setup/5 uses.
-define(BINDING_KEY, mycelium_channel_binding).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run the auth protocol as the connection initiator. Returns the
%% node atom claimed by the peer (verified by signature against the
%% trusted key for that atom). The dist handshake that runs immediately
%% after refuses the connection if that name does not match the target.
%%
%% `TargetNode' is the node we dialed. It gates the AUTH_OK
%% short-circuit: a server claiming the cookie-only path is only
%% trusted if `TargetNode' matches the client's own
%% `cookie_only_nodes' whitelist.
-spec authenticate_outgoing(
    Conn :: pid(), TargetNode :: node() | undefined, Timeout :: timeout()
) -> {ok, node() | undefined} | {error, term()}.
authenticate_outgoing(Conn, TargetNode, Timeout) ->
    case auth_enabled() of
        false ->
            {ok, undefined};
        true ->
            Deadline = deadline(Timeout),
            with_metrics(
                outgoing,
                fun() -> run_outgoing(Conn, TargetNode, Deadline) end
            )
    end.

%% @doc Run the auth protocol as the connection responder.
-spec authenticate_incoming(Conn :: pid(), Timeout :: timeout()) ->
    {ok, node() | undefined} | {error, term()}.
authenticate_incoming(Conn, Timeout) ->
    case auth_enabled() of
        false ->
            {ok, undefined};
        true ->
            Deadline = deadline(Timeout),
            with_metrics(incoming, fun() -> run_incoming(Conn, Deadline) end)
    end.

%% Convert a relative timeout to a monotonic deadline. The
%% `infinity' case yields a sentinel that `time_left/1' interprets
%% as no cap.
deadline(infinity) ->
    infinity;
deadline(Timeout) when is_integer(Timeout), Timeout >= 0 ->
    erlang:monotonic_time(millisecond) + Timeout.

%% How long remains on the handshake deadline. Bottoms out at 0 so a
%% blown deadline never produces a negative receive timeout.
time_left(infinity) ->
    infinity;
time_left(Deadline) ->
    max(0, Deadline - erlang:monotonic_time(millisecond)).

%% Time the handshake and emit attempt/duration metrics. Exceptions in
%% the inner function are still propagated; they just record as failures.
with_metrics(Role, F) ->
    Start = erlang:monotonic_time(millisecond),
    Outcome = try F() of
        {ok, _} = Ok -> {ok, Ok};
        Other        -> {fail, Other}
    catch
        Class:Reason:Stack -> {raise, Class, Reason, Stack}
    end,
    Duration = erlang:monotonic_time(millisecond) - Start,
    case Outcome of
        {ok, Result} ->
            mycelium_metrics:auth_attempt(Role, ok, Duration),
            Result;
        {fail, Result} ->
            mycelium_metrics:auth_attempt(Role, fail, Duration),
            Result;
        {raise, C, R, S} ->
            mycelium_metrics:auth_attempt(Role, fail, Duration),
            erlang:raise(C, R, S)
    end.

%%====================================================================
%% Outgoing (client) side
%%
%% Internal helpers below thread `Timeout' as the *monotonic deadline*
%% computed at the public entries, not as a per-call timeout. The
%% three receive sites convert it back to a remaining-ms via
%% `time_left/1', so a slow peer dribbling bytes cannot extend the
%% handshake beyond the operator-configured budget.
%%====================================================================

run_outgoing(Conn, TargetNode, Timeout) ->
    %% Channel binding (H1): the client binds to the server cert it
    %% actually observed on this TLS connection. Fail closed if the peer
    %% cert is unavailable - an unbound handshake is exactly the relay
    %% the binding defends against.
    case client_binding(Conn) of
        {ok, Binding} ->
            set_binding(Binding),
            case quic:open_unidirectional_stream(Conn) of
                {ok, MyStream} ->
                    try
                        do_outgoing(Conn, MyStream, TargetNode, Timeout)
                    after
                        catch quic:send_data(Conn, MyStream, <<>>, true)
                    end;
                {error, Reason} ->
                    {error, {open_auth_stream_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {channel_binding_failed, Reason}}
    end.

do_outgoing(Conn, MyStream, TargetNode, Timeout) ->
    case mycelium_dist_auth:get_public_key() of
        {ok, MyPubKey} ->
            MyNode = node(),
            Hello = mycelium_dist_protocol:encode_hello(MyNode, MyPubKey),
            case stream_send(Conn, MyStream, Hello) of
                ok ->
                    case wait_for_peer_stream(Conn, Timeout) of
                        {ok, PeerStream, Buffer} ->
                            client_recv_hello(
                                Conn, MyStream, PeerStream, Buffer,
                                TargetNode, Timeout
                            );
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, {send_hello_failed, Reason}}
            end;
        Error ->
            Error
    end.

client_recv_hello(Conn, MyStream, PeerStream, Buffer, TargetNode, Timeout) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, HelloBin, Rest} ->
            case mycelium_dist_protocol:decode(HelloBin) of
                {hello, ClaimedNode, PeerPubKey} ->
                    %% Trust check is done before we issue our challenge:
                    %% in strict mode, refuse if the claimed node has no
                    %% pinned key. If a different key is already pinned,
                    %% refuse regardless of mode. The dist handshake that
                    %% follows verifies this atom matches the connect
                    %% target.
                    case trust_check(ClaimedNode, PeerPubKey) of
                        ok ->
                            client_send_challenge(
                                Conn, MyStream, PeerStream, Rest,
                                ClaimedNode, PeerPubKey, Timeout
                            );
                        {error, Reason} ->
                            {error, Reason}
                    end;
                ok ->
                    %% Server signalled the cookie-only short-circuit.
                    %% Trust it only when the node we dialed matches our
                    %% own cookie_only_nodes whitelist; otherwise a
                    %% rogue server reachable through discovery could
                    %% skip Ed25519.
                    case TargetNode =/= undefined
                         andalso mycelium_dist_auth:is_cookie_only_allowed(
                                   TargetNode
                                 ) of
                        true  -> warn_cookie_only(), {ok, undefined};
                        _     -> {error, unexpected_auth_ok}
                    end;
                {fail, Reason} ->
                    {error, {auth_rejected, Reason}};
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_hello_failed, Reason}}
    end.

%% Tri-state trust check. A pinned key that does not match the
%% presented one is always a hard reject (re-pin attempt). Absence of
%% a pin falls through to mode policy.
trust_check(Node, PubKey) ->
    case mycelium_dist_keys:lookup_pin(Node) of
        {pinned, PubKey} ->
            ok;
        {pinned, _Other} ->
            {error, key_mismatch};
        not_pinned ->
            case trust_mode() of
                tofu   -> ok;
                strict -> {error, untrusted_key}
            end
    end.

client_send_challenge(Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout) ->
    {MyNonce, MyTs, MyMono} = mycelium_dist_auth:create_challenge(),
    Msg = mycelium_dist_protocol:encode_challenge(MyNonce, MyTs),
    case stream_send(Conn, MyStream, Msg) of
        ok ->
            client_recv_challenge(
                Conn, MyStream, PeerStream, Buffer,
                PeerNode, PeerPubKey, MyNonce, MyTs, MyMono, Timeout
            );
        {error, Reason} ->
            {error, {send_challenge_failed, Reason}}
    end.

client_recv_challenge(
    Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey,
    MyNonce, MyTs, MyMono, Timeout
) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, Bin, Rest} ->
            case mycelium_dist_protocol:decode(Bin) of
                {challenge, PeerNonce, PeerTs} ->
                    case mycelium_dist_auth:validate_peer_ts(PeerTs) of
                        ok ->
                            client_send_response(
                                Conn, MyStream, PeerStream, Rest,
                                PeerNode, PeerPubKey,
                                MyNonce, MyTs, MyMono,
                                PeerNonce, PeerTs, Timeout
                            );
                        {error, _} = E ->
                            E
                    end;
                {fail, Reason} ->
                    {error, {auth_rejected, Reason}};
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_challenge_failed, Reason}}
    end.

client_send_response(
    Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey,
    MyNonce, MyTs, MyMono, PeerNonce, PeerTs, Timeout
) ->
    case sign_for_peer(PeerNonce, PeerTs, PeerPubKey) of
        {ok, MySig} ->
            Resp = mycelium_dist_protocol:encode_response(MySig),
            case stream_send(Conn, MyStream, Resp) of
                ok ->
                    client_recv_response(
                        Conn, PeerStream, Buffer,
                        PeerNode, PeerPubKey, MyNonce, MyTs, MyMono, Timeout
                    );
                {error, Reason} ->
                    {error, {send_response_failed, Reason}}
            end;
        Error ->
            Error
    end.

client_recv_response(Conn, PeerStream, Buffer, PeerNode, PeerPubKey,
                     MyNonce, MyTs, MyMono, Timeout) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, Bin, Rest} ->
            case mycelium_dist_protocol:decode(Bin) of
                {response, PeerSig} ->
                    case
                        mycelium_dist_auth:verify_response(
                            PeerSig, PeerPubKey, {MyNonce, MyTs, MyMono},
                            binding()
                        )
                    of
                        true ->
                            client_recv_ok(
                                Conn, PeerStream, Rest,
                                PeerNode, PeerPubKey, Timeout
                            );
                        false ->
                            {error, signature_verification_failed}
                    end;
                {fail, Reason} ->
                    {error, {auth_rejected, Reason}};
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_response_failed, Reason}}
    end.

client_recv_ok(Conn, PeerStream, Buffer, PeerNodeBin, PeerPubKey, Timeout) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, Bin, _Rest} ->
            case mycelium_dist_protocol:decode(Bin) of
                ok ->
                    %% Signature verified: now safe to mint the atom.
                    PeerNode = binary_to_atom(PeerNodeBin, utf8),
                    case record_peer(PeerNode, PeerPubKey) of
                        ok                 -> {ok, PeerNode};
                        {error, _} = Error -> Error
                    end;
                {fail, Reason} ->
                    {error, {auth_rejected, Reason}};
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_ok_failed, Reason}}
    end.

%%====================================================================
%% Incoming (server) side
%%====================================================================

run_incoming(Conn, Timeout) ->
    %% Channel binding (H1): the server binds to its own listener cert
    %% (the cert it presented on this TLS connection). Fail closed if it
    %% cannot be resolved.
    case mycelium_dist_auth:server_cert_binding() of
        {ok, Binding} ->
            set_binding(Binding),
            case wait_for_peer_stream(Conn, Timeout) of
                {ok, PeerStream, Buffer} ->
                    do_incoming(Conn, PeerStream, Buffer, Timeout);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {channel_binding_failed, Reason}}
    end.

do_incoming(Conn, PeerStream, Buffer, Timeout) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, HelloBin, Rest} ->
            case mycelium_dist_protocol:decode(HelloBin) of
                {hello, PeerNode, PeerPubKey} ->
                    server_after_hello(
                        Conn, PeerStream, Rest, PeerNode, PeerPubKey, Timeout
                    );
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_hello_failed, Reason}}
    end.

server_after_hello(Conn, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout) ->
    %% cookie_only_nodes is a whitelist of peer-name patterns the
    %% cluster trusts on cookie alone. test_runner-style probes can
    %% be exempted from the full Ed25519 handshake; they still face
    %% the OTP-level dist challenge that comes after.
    case mycelium_dist_auth:is_cookie_only_allowed(PeerNode) of
        true ->
            server_send_skip(Conn);
        false ->
            case mycelium_dist_keys:lookup_pin(PeerNode) of
                {pinned, PeerPubKey} ->
                    server_open_my_stream(
                        Conn, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout
                    );
                {pinned, _Other} ->
                    send_fail_via_uni(Conn, <<"key_mismatch">>),
                    {error, key_mismatch};
                not_pinned ->
                    case trust_mode() of
                        tofu ->
                            server_open_my_stream(
                                Conn, PeerStream, Buffer,
                                PeerNode, PeerPubKey, Timeout
                            );
                        strict ->
                            send_fail_via_uni(Conn, <<"untrusted_key">>),
                            {error, untrusted_key}
                    end
            end
    end.

%% Whitelisted peer: open our uni stream just to send AUTH_OK and
%% finalise. The client recognises the OK frame in client_recv_hello
%% and falls through without challenge-response. No Ed25519 runs, so
%% we never mint an atom for the (peer-claimed) name and return
%% `undefined' - the dist handshake that follows establishes identity.
server_send_skip(Conn) ->
    case quic:open_unidirectional_stream(Conn) of
        {ok, MyStream} ->
            Msg = mycelium_dist_protocol:encode_ok(),
            _ = stream_send(Conn, MyStream, Msg),
            catch quic:send_data(Conn, MyStream, <<>>, true),
            warn_cookie_only(),
            {ok, undefined};
        {error, Reason} ->
            {error, {open_auth_stream_failed, Reason}}
    end.

%% Log once per node when a cookie-only (no-Ed25519) connection is
%% accepted, so the reduced-assurance mode is visible to operators.
warn_cookie_only() ->
    Key = {?MODULE, cookie_only_warned},
    case persistent_term:get(Key, false) of
        true ->
            ok;
        false ->
            persistent_term:put(Key, true),
            logger:warning(
                "mycelium: accepted a cookie-only peer (cookie_only_nodes) - "
                "no Ed25519, not protected against an active MITM.")
    end.

server_open_my_stream(Conn, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout) ->
    case quic:open_unidirectional_stream(Conn) of
        {ok, MyStream} ->
            try
                server_send_hello(
                    Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout
                )
            after
                catch quic:send_data(Conn, MyStream, <<>>, true)
            end;
        {error, Reason} ->
            {error, {open_auth_stream_failed, Reason}}
    end.

server_send_hello(Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout) ->
    case mycelium_dist_auth:get_public_key() of
        {ok, MyPubKey} ->
            MyNode = node(),
            Hello = mycelium_dist_protocol:encode_hello(MyNode, MyPubKey),
            case stream_send(Conn, MyStream, Hello) of
                ok ->
                    server_send_challenge(
                        Conn, MyStream, PeerStream, Buffer,
                        PeerNode, PeerPubKey, Timeout
                    );
                {error, Reason} ->
                    {error, {send_hello_failed, Reason}}
            end;
        Error ->
            Error
    end.

server_send_challenge(Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey, Timeout) ->
    {MyNonce, MyTs, MyMono} = mycelium_dist_auth:create_challenge(),
    Msg = mycelium_dist_protocol:encode_challenge(MyNonce, MyTs),
    case stream_send(Conn, MyStream, Msg) of
        ok ->
            server_recv_challenge(
                Conn, MyStream, PeerStream, Buffer,
                PeerNode, PeerPubKey, MyNonce, MyTs, MyMono, Timeout
            );
        {error, Reason} ->
            {error, {send_challenge_failed, Reason}}
    end.

server_recv_challenge(
    Conn, MyStream, PeerStream, Buffer,
    PeerNode, PeerPubKey, MyNonce, MyTs, MyMono, Timeout
) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, Bin, Rest} ->
            case mycelium_dist_protocol:decode(Bin) of
                {challenge, PeerNonce, PeerTs} ->
                    case mycelium_dist_auth:validate_peer_ts(PeerTs) of
                        ok ->
                            server_send_response(
                                Conn, MyStream, PeerStream, Rest,
                                PeerNode, PeerPubKey,
                                MyNonce, MyTs, MyMono,
                                PeerNonce, PeerTs, Timeout
                            );
                        {error, _} = E ->
                            E
                    end;
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_challenge_failed, Reason}}
    end.

server_send_response(
    Conn, MyStream, PeerStream, Buffer, PeerNode, PeerPubKey,
    MyNonce, MyTs, MyMono, PeerNonce, PeerTs, Timeout
) ->
    case sign_for_peer(PeerNonce, PeerTs, PeerPubKey) of
        {ok, MySig} ->
            Resp = mycelium_dist_protocol:encode_response(MySig),
            case stream_send(Conn, MyStream, Resp) of
                ok ->
                    server_recv_response(
                        Conn, MyStream, PeerStream, Buffer,
                        PeerNode, PeerPubKey, MyNonce, MyTs, MyMono, Timeout
                    );
                {error, Reason} ->
                    {error, {send_response_failed, Reason}}
            end;
        Error ->
            Error
    end.

server_recv_response(
    Conn, MyStream, PeerStream, Buffer,
    PeerNode, PeerPubKey, MyNonce, MyTs, MyMono, Timeout
) ->
    case decode_with_buffer(Conn, PeerStream, Buffer, Timeout) of
        {ok, Bin, _Rest} ->
            case mycelium_dist_protocol:decode(Bin) of
                {response, PeerSig} ->
                    case
                        mycelium_dist_auth:verify_response(
                            PeerSig, PeerPubKey, {MyNonce, MyTs, MyMono},
                            binding()
                        )
                    of
                        true ->
                            server_send_ok(Conn, MyStream, PeerNode, PeerPubKey);
                        false ->
                            send_fail(Conn, MyStream, <<"signature_invalid">>),
                            {error, signature_verification_failed}
                    end;
                Other ->
                    {error, {unexpected_message, Other}}
            end;
        {error, Reason} ->
            {error, {recv_response_failed, Reason}}
    end.

server_send_ok(Conn, MyStream, PeerNodeBin, PeerPubKey) ->
    case stream_send(Conn, MyStream, mycelium_dist_protocol:encode_ok()) of
        ok ->
            %% Signature verified: now safe to mint the atom.
            PeerNode = binary_to_atom(PeerNodeBin, utf8),
            case record_peer(PeerNode, PeerPubKey) of
                ok                 -> {ok, PeerNode};
                {error, _} = Error -> Error
            end;
        {error, Reason} ->
            {error, {send_ok_failed, Reason}}
    end.

%%====================================================================
%% Stream IO with length framing
%%====================================================================

stream_send(Conn, StreamId, Msg) ->
    Len = byte_size(Msg),
    Framed = <<Len:(?LEN_SIZE * 8)/big, Msg/binary>>,
    quic:send_data(Conn, StreamId, Framed, false).

decode_with_buffer(Conn, StreamId, Buffer, Timeout) ->
    case Buffer of
        <<Len:(?LEN_SIZE * 8)/big, Rest/binary>> when byte_size(Rest) >= Len ->
            <<Msg:Len/binary, Tail/binary>> = Rest,
            {ok, Msg, Tail};
        _ ->
            case recv_more(Conn, StreamId, Timeout) of
                {ok, More} ->
                    decode_with_buffer(
                        Conn, StreamId, <<Buffer/binary, More/binary>>, Timeout
                    );
                {error, Reason} ->
                    {error, Reason}
            end
    end.

recv_more(Conn, StreamId, Timeout) ->
    receive
        {quic, Conn, {stream_data, StreamId, Data, _Fin}} ->
            {ok, Data};
        {quic, Conn, {stream_reset, StreamId, Code}} ->
            {error, {stream_reset, Code}};
        {quic, Conn, {closed, Reason}} ->
            {error, {connection_closed, Reason}};
        {quic, Conn, {transport_error, Code, Reason}} ->
            {error, {transport_error, Code, Reason}}
    after time_left(Timeout) ->
        {error, auth_stream_timeout}
    end.

%% Wait for the peer's stream to be opened, returning its id and the
%% first chunk of data. Accept both `stream_opened' notifications and
%% direct `stream_data' arrivals.
wait_for_peer_stream(Conn, Timeout) ->
    receive
        {quic, Conn, {stream_opened, StreamId}} ->
            recv_first_chunk(Conn, StreamId, Timeout);
        {quic, Conn, {stream_data, StreamId, Data, _Fin}} ->
            {ok, StreamId, Data};
        {quic, Conn, {closed, Reason}} ->
            {error, {connection_closed, Reason}};
        {quic, Conn, {transport_error, Code, Reason}} ->
            {error, {transport_error, Code, Reason}}
    after time_left(Timeout) ->
        {error, auth_stream_timeout}
    end.

recv_first_chunk(Conn, StreamId, Timeout) ->
    receive
        {quic, Conn, {stream_data, StreamId, Data, _Fin}} ->
            {ok, StreamId, Data};
        {quic, Conn, {stream_reset, StreamId, Code}} ->
            {error, {stream_reset, Code}};
        {quic, Conn, {closed, Reason}} ->
            {error, {connection_closed, Reason}}
    after time_left(Timeout) ->
        {error, auth_stream_timeout}
    end.

%%====================================================================
%% Helpers
%%====================================================================

sign_for_peer(Nonce, Timestamp, _PeerPubKey) ->
    %% Sign <Nonce | Timestamp | OwnPubKey | Binding>. The peer verifies
    %% via mycelium_dist_auth:verify_response/4, rebuilding the message
    %% with our pubkey (from its perspective) and the same Binding from
    %% its own TLS viewpoint. Both the responder identity and the TLS
    %% channel are bound, so a relayed signature over a different
    %% channel's cert no longer verifies.
    mycelium_dist_auth:sign_challenge(Nonce, Timestamp, binding()).

%% Channel-binding stash (see ?BINDING_KEY).
set_binding(Binding) ->
    erlang:put(?BINDING_KEY, Binding).

binding() ->
    erlang:get(?BINDING_KEY).

%% Client-side binding: SHA-256 of the server cert observed on this
%% connection. {error, _} (incl. no_peercert) makes the caller fail closed.
client_binding(Conn) ->
    case quic:peercert(Conn) of
        {ok, Der} when is_binary(Der) ->
            {ok, crypto:hash(sha256, Der)};
        {error, Reason} ->
            {error, {peercert_unavailable, Reason}}
    end.

send_fail(Conn, StreamId, Reason) ->
    Msg = mycelium_dist_protocol:encode_fail(Reason),
    _ = stream_send(Conn, StreamId, Msg),
    ok.

%% Open a fresh uni stream just to deliver a FAIL message (used when we
%% reject before opening the normal server-side auth stream).
send_fail_via_uni(Conn, Reason) ->
    case quic:open_unidirectional_stream(Conn) of
        {ok, S} ->
            send_fail(Conn, S, Reason),
            catch quic:send_data(Conn, S, <<>>, true),
            ok;
        _ ->
            ok
    end.

record_peer(PeerNode, PeerPubKey) ->
    case trust_mode() of
        strict ->
            ok;
        tofu ->
            mycelium_dist_keys:store_key_if_new(PeerNode, PeerPubKey)
    end.

auth_enabled() ->
    application:get_env(mycelium, auth_enabled, true).

trust_mode() ->
    application:get_env(mycelium, auth_trust_mode, tofu).

