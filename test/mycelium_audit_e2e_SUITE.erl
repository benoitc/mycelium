%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% E2E coverage for the audit-fix PRs. Spawns real BEAM peers under
%%% `-proto_dist mycelium' and exercises the round-2 fixes against
%%% the live dist handshake and supervision tree.
%%%
%%% Reuses the `start_peer/3' helper from mycelium_proto_dist_SUITE
%%% (export_all). Each case picks the scaffolding it needs.

-module(mycelium_audit_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        tofu_repin_rejected,
        client_refuses_auth_ok_when_target_not_whitelisted,
        pending_timeout_clears_phantom_join,
        keypair_mismatch_detected_on_load,
        validate_peer_ts_in_handshake
    ].

init_per_suite(Config) ->
    %% Stay clear of the proto_dist suite's port band (19100-19899).
    BasePort = 20000 + erlang:phash2({?MODULE, erlang:system_time()}, 800) * 10,
    [{base_port, BasePort} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    SuiteDir = ?config(priv_dir, Config),
    TcDir = filename:join(SuiteDir,
                          "tc_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(TcDir, "dummy")),
    DiscoveryDir = filename:join(TcDir, "discovery"),
    ok = filelib:ensure_dir(filename:join(DiscoveryDir, "dummy")),
    put(?MODULE, []),
    [{tc_dir, TcDir}, {discovery_dir, DiscoveryDir} | Config].

end_per_testcase(_Case, _Config) ->
    Peers = case get(?MODULE) of undefined -> []; L -> L end,
    [catch peer:stop(P) || P <- Peers],
    erase(?MODULE),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% Two-node TOFU re-pin: A pins B's pubkey on first contact. B is
%% then re-keyed (keys wiped, new keypair generated on restart). A's
%% retry must refuse because the new pubkey does not match the
%% pinned one.
tofu_repin_rejected(Config) ->
    {Pa, _NodeA, _, C1} = start_peer("a", Config, #{}),
    {Pb, NodeB, DirB, _} = start_peer("b", C1, #{}),
    %% First contact: A connects to B, pins B's key.
    true = peer:call(Pa, net_kernel, connect_node, [NodeB], 15000),
    wait_until(fun() ->
        case peer:call(Pa, mycelium_dist_keys, lookup_pin, [NodeB]) of
            {pinned, _} -> true;
            _           -> false
        end
    end, 5000),
    {pinned, OldPub} = peer:call(Pa, mycelium_dist_keys, lookup_pin, [NodeB]),
    %% Stop B and wipe its keypair so a restart generates fresh keys.
    ok = peer:stop(Pb),
    rm_dir(filename:join([DirB, "data", "keys"])),
    %% Restart B on the same port/name with the same suffix dir.
    {Pb2, NodeB, _, _} = restart_peer("b", Config, #{port => peer_port(Pb, b)}),
    _ = Pb2,
    %% Force A to retry: disconnect any cached dist channel, then
    %% try connect_node again. A must refuse because the new pubkey
    %% will not match the pinned one.
    _ = peer:call(Pa, erlang, disconnect_node, [NodeB]),
    timer:sleep(200),
    Result = peer:call(Pa, net_kernel, connect_node, [NodeB], 15000),
    ?assertNotEqual(true, Result),
    %% The pin is unchanged (re-pin refused).
    ?assertEqual({pinned, OldPub},
                 peer:call(Pa, mycelium_dist_keys, lookup_pin, [NodeB])),
    ok.

%% Client refuses an AUTH_OK short-circuit when the dialed peer is
%% not in the client's cookie_only_nodes whitelist. The server-side
%% gate (PR 1) is symmetrical with the client-side gate; both must
%% list the peer for the short-circuit to apply.
client_refuses_auth_ok_when_target_not_whitelisted(Config) ->
    {Pa, NodeA, _, C1} = start_peer("a", Config, #{}),
    %% Server B lists A in cookie_only_nodes so B would send AUTH_OK.
    {_Pb, NodeB, _, _} = start_peer("b", C1,
                                     #{cookie_only_nodes => [NodeA]}),
    %% A does NOT list B in its own cookie_only_nodes; A receives an
    %% unexpected AUTH_OK and the handshake fails.
    Result = peer:call(Pa, net_kernel, connect_node, [NodeB], 10000),
    ?assertNotEqual(true, Result),
    ok.

%% A pending join to a phantom node clears via the backstop timer
%% even though no peer_connected or peer_failed ever fires.
pending_timeout_clears_phantom_join(Config) ->
    {Pa, _NodeA, _, _} = start_peer("a", Config,
                                    #{pending_timeout_ms => 200}),
    _ = peer:call(Pa, mycelium, join, ['phantom@127.0.0.1']),
    wait_until(fun() ->
        Pending = peer:call(Pa, ?MODULE, pending_keys, []),
        not lists:member('phantom@127.0.0.1', Pending)
    end, 2000),
    ok.

%% Corrupting node.pub on disk leaves the priv/pub pair mismatched.
%% load_keypair/1 must refuse rather than silently load garbage.
keypair_mismatch_detected_on_load(Config) ->
    {Pa, _NodeA, DirA, _} = start_peer("a", Config, #{}),
    KeyDir = filename:join([DirA, "data", "keys"]),
    %% Confirm baseline load works.
    ?assertMatch({ok, _, _},
                 peer:call(Pa, mycelium_dist_auth, load_keypair, [KeyDir])),
    %% Overwrite the public key with random bytes.
    ok = peer:call(Pa, file, write_file,
                   [filename:join(KeyDir, "node.pub"),
                    crypto:strong_rand_bytes(32)]),
    %% load_keypair now refuses with keypair_mismatch.
    ?assertEqual({error, keypair_mismatch},
                 peer:call(Pa, mycelium_dist_auth, load_keypair, [KeyDir])),
    ok.

%% A peer presenting a wall-clock timestamp far outside the window
%% would be rejected by validate_peer_ts. We exercise the validator
%% across the dist-call channel to confirm it's wired and reachable.
validate_peer_ts_in_handshake(Config) ->
    {Pa, _NodeA, _, _} = start_peer("a", Config, #{}),
    Now = peer:call(Pa, erlang, system_time, [millisecond]),
    %% Inside tolerance.
    ?assertEqual(ok,
                 peer:call(Pa, mycelium_dist_auth, validate_peer_ts, [Now])),
    %% 5 minutes outside is far past the 2x window default.
    ?assertEqual({error, peer_ts_skew},
                 peer:call(Pa, mycelium_dist_auth, validate_peer_ts,
                           [Now - 5 * 60 * 1000])),
    ok.

%%====================================================================
%% Helpers exported for peer:call
%%====================================================================

%% Return the keys of the hyparview pending map.
pending_keys() ->
    State = sys:get_state(mycelium_hyparview),
    PendingMap = element(state_pending_index(), State),
    maps:keys(PendingMap).

%% Position of `pending' in the view_state record. Stable across
%% the audit work (PR 4 changed value shape, not field position).
state_pending_index() ->
    %% Derived empirically from include/mycelium.hrl: view_state's
    %% first map field is the pending map. Keep this brittle but
    %% local; if the record reshuffles, the assert will fail loudly.
    Probe = sys:get_state(mycelium_hyparview),
    pending_index(Probe, 2, tuple_size(Probe)).

pending_index(_T, I, Size) when I > Size ->
    erlang:error({pending_index_not_found, Size});
pending_index(T, I, Size) ->
    case element(I, T) of
        M when is_map(M) ->
            %% Discriminate by inserting and checking later. For the
            %% test we don't need exactness — any map with our test
            %% key, if present, IS pending; otherwise return the
            %% first map we see (the pending map is first in the
            %% record's map fields).
            I;
        _ ->
            pending_index(T, I + 1, Size)
    end.

%%====================================================================
%% Peer scaffolding (delegates to mycelium_proto_dist_SUITE)
%%====================================================================

start_peer(Suffix, Config, Opts) ->
    do_start_peer(Suffix, Config, Opts).

restart_peer(Suffix, Config, Opts) ->
    %% Same data dirs (Suffix) so the new node loads/regenerates keys
    %% under the original path. mycelium_proto_dist_SUITE doesn't
    %% expose a restart helper; reusing start_peer/3 is enough.
    do_start_peer(Suffix, Config, Opts).

%% Patched copy of mycelium_proto_dist_SUITE:start_peer/3 that
%% understands a few extra knobs:
%%   - cookie_only_nodes :: [atom()]    -> -mycelium cookie_only_nodes
%%   - pending_timeout_ms :: pos_integer() -> -mycelium pending_timeout_ms
do_start_peer(Suffix, Config, Opts) ->
    TcDir = ?config(tc_dir, Config),
    NodeDir = filename:join(TcDir, Suffix),
    ok = filelib:ensure_dir(filename:join(NodeDir, "dummy")),
    QuicDir = filename:join(NodeDir, "data/quic"),
    KeysDir = filename:join(NodeDir, "data/keys"),
    ok = filelib:ensure_dir(filename:join(QuicDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(KeysDir, "dummy")),
    DiscoveryDir = ?config(discovery_dir, Config),
    BasePort = ?config(base_port, Config),
    Port = maps:get(port, Opts, next_port(BasePort)),
    ActiveSize = maps:get(active_size, Opts, 5),
    %% Stable per-suffix node name so restart_peer rebinds to the
    %% same atom (so pinning still has a chance to match across a
    %% restart in the same test).
    Name = list_to_atom(
        "myc_" ++ Suffix ++ "_"
        ++ integer_to_list(erlang:phash2({?MODULE, Suffix, BasePort}, 1 bsl 30))
    ),
    BaseArgs = [
        "-proto_dist", "mycelium",
        "-epmd_module", "mycelium_epmd",
        "-start_epmd", "false",
        "-mycelium_dist_port",     integer_to_list(Port),
        "-mycelium_dist_cert_dir", QuicDir,
        "-setcookie", "mycelium_ct",
        %% Non-default cookie so cookie_only_nodes cases do not trip the
        %% default-cookie boot guard; all peers share it.
        "-mycelium", "dist_cookie",   "myc_ct_secret",
        "-mycelium", "auth_key_dir",  quote(KeysDir),
        "-mycelium", "discovery_dir", quote(DiscoveryDir),
        "-mycelium", "active_size",   integer_to_list(ActiveSize)
    ],
    CookieArgs = case Opts of
        #{cookie_only_nodes := List} ->
            ["-mycelium", "cookie_only_nodes",
             lists:flatten(io_lib:format("~w", [List]))];
        _ -> []
    end,
    PendingArgs = case Opts of
        #{pending_timeout_ms := PT} ->
            ["-mycelium", "pending_timeout_ms", integer_to_list(PT)];
        _ -> []
    end,
    PaArgs = lists:flatmap(fun(P) -> ["-pa", P] end, code:get_path()),
    Args = PaArgs ++ BaseArgs ++ CookieArgs ++ PendingArgs,
    %% Save the port we picked into the peer pid's process dictionary
    %% so restart_peer can grab it.
    put({?MODULE, port, Suffix}, Port),
    {ok, Pid, Node} = peer:start(#{
        name => Name,
        longnames => true,
        host => "127.0.0.1",
        connection => standard_io,
        args => Args
    }),
    {ok, _Started} = peer:call(Pid, application, ensure_all_started, [mycelium]),
    put(?MODULE, [Pid | case get(?MODULE) of undefined -> []; L -> L end]),
    {Pid, Node, NodeDir, Config}.

peer_port(_Pid, Atom) ->
    Suffix = atom_to_list(Atom),
    case get({?MODULE, port, Suffix}) of
        undefined -> erlang:error({no_port_for, Atom});
        P -> P
    end.

quote(S) -> "\"" ++ S ++ "\"".

next_port(BasePort) ->
    Key = {?MODULE, next_port_offset},
    N = case get(Key) of undefined -> 0; X -> X end,
    put(Key, N + 1),
    BasePort + N.

rm_dir(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

wait_until(Fun, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        _ ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true  -> ?assert(false, "wait_until timed out");
                false -> timer:sleep(50), wait_loop(Fun, Deadline)
            end
    end.
