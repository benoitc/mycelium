%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Multi-node CT helper for `-proto_dist quic' clusters.
%%%
%%% Slave boot pattern modelled on upstream `quic_call_SUITE':
%%%
%%%   - `-epmd_module quic_epmd -start_epmd false' (no real EPMD).
%%%   - Cert/key/port passed as boot args (`-quic_dist_*'), since
%%%     proto_dist starts before sys.config-driven app envs are
%%%     applied.
%%%   - Each slave's `-eval' bootstraps `quic_discovery_static' with
%%%     the full per-cluster node->{host, port} map, then starts
%%%     `mycelium', then writes a `READY' marker.
%%%   - The CT BEAM never starts net_kernel; it drives slaves via
%%%     `quic_call.sh' shelled out through `os:cmd/1'.

-module(mycelium_quic_test_helper).

-export([
    setup_cert/1,
    quic_call_path/0,
    short_name/1,
    long_name/1,
    start_slave/4,
    stop_slave/1,
    qcall/4,
    qcall/5,
    wait_until/2
]).

-define(READY_TIMEOUT_MS, 60000).

%% @doc Generate the QUIC TLS material under `Dir' if missing. Returns
%% a map with the cert/key paths to splice into per-slave sys.config.
-spec setup_cert(file:filename()) -> #{cert => binary(), key => binary()}.
setup_cert(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    application:set_env(mycelium, quic_cert_dir, Dir),
    {ok, _} = application:ensure_all_started(public_key),
    ok = mycelium_quic_cert:ensure_cert(),
    {Cert, Key} = mycelium_quic_cert:get_cert_paths(),
    #{cert => list_to_binary(Cert), key => list_to_binary(Key)}.

%% @doc Locate the upstream `quic_call.sh' script.
-spec quic_call_path() -> file:filename().
quic_call_path() ->
    case code:priv_dir(quic) of
        {error, bad_name} ->
            error({quic_priv_dir_not_found});
        Priv ->
            P = filename:join([Priv, "bin", "quic_call.sh"]),
            true = filelib:is_regular(P),
            P
    end.

%% @doc Build a short name with PID + microsecond suffix for
%% uniqueness across consecutive `rebar3 ct' runs.
short_name(Prefix) ->
    list_to_atom(
        atom_to_list(Prefix) ++ "_" ++
            os:getpid() ++ "_" ++
            integer_to_list(erlang:system_time(microsecond))
    ).

long_name(Short) ->
    list_to_atom(atom_to_list(Short) ++ "@127.0.0.1").

%% @doc Start one slave. `Spec' is a map with:
%%   - name :: long node atom
%%   - port :: integer (unique per slave)
%%   - cookie :: atom
%%   - cert :: binary | string (path)
%%   - key :: binary | string (path)
%%   - nodes :: [{LongName, {Host :: string(), Port :: integer()}}]
%%   - mycelium_env :: proplist (extra `{mycelium, [...]}' env entries)
%%
%% `ParentPriv' is the priv_dir of the suite; the slave's stdout log
%% and READY marker live under `ParentPriv/<Short>/'.
-spec start_slave(atom(), file:filename(), map(), pos_integer()) ->
    {ok, map()}.
start_slave(Short, ParentPriv, Spec, _ReadyTimeoutMs) ->
    Name = maps:get(name, Spec),
    Port = maps:get(port, Spec),
    Cookie = maps:get(cookie, Spec),
    Cert = to_string(maps:get(cert, Spec)),
    Key = to_string(maps:get(key, Spec)),
    Nodes = maps:get(nodes, Spec),
    MyEnv = maps:get(mycelium_env, Spec, []),

    SlaveDir = filename:join(ParentPriv, atom_to_list(Short)),
    ok = filelib:ensure_dir(filename:join(SlaveDir, "dummy")),
    ReadyFile = filename:join(SlaveDir, "READY"),
    LogFile = filename:join(SlaveDir, "slave.log"),
    file:delete(ReadyFile),

    ProbeConfig = render_probe_config(SlaveDir, Cert, Key, Nodes),
    Eval = build_slave_eval(Cert, Key, Nodes, MyEnv, ReadyFile),

    EbinPath = lists:flatten(
        [
            ["-pa ", P, " "]
         || P <- code:get_path(),
            P =/= ".",
            P =/= ""
        ]
    ),

    Args =
        string:tokens(EbinPath, " ") ++
            [
                "-name",
                atom_to_list(Name),
                "-setcookie",
                atom_to_list(Cookie),
                "-proto_dist",
                "quic",
                "-epmd_module",
                "quic_epmd",
                "-start_epmd",
                "false",
                "-quic_dist_port",
                integer_to_list(Port),
                "-quic_dist_cert",
                Cert,
                "-quic_dist_key",
                Key,
                "-quic_dist_verify",
                "verify_none",
                %% Disable global's overlapping-partition prevention; mycelium
                %% controls topology, not full mesh. The kernel reads this at
                %% boot only, so setting via mycelium_app:start is too late.
                "-kernel",
                "prevent_overlapping_partitions",
                "false",
                "-noinput",
                "-eval",
                Eval
            ],

    ErlExe = os:find_executable("erl"),
    case ErlExe of
        false -> error({erl_not_found});
        _ -> ok
    end,

    %% Spawn the slave via a shell so its stdout/stderr go straight
    %% to LogFile -- the OS handles the redirect, no tee process,
    %% no pipe-blocking risk if the test process ever stops draining.
    Cmd = build_shell_cmd(ErlExe, Args, LogFile),
    P = erlang:open_port(
        {spawn, Cmd},
        [{cd, SlaveDir}, binary, exit_status, hide]
    ),

    case wait_for_ready_simple(ReadyFile, P, ?READY_TIMEOUT_MS) of
        ok ->
            %% Detach the port -- the slave keeps running with its
            %% stdout going to the file. We retrieve its os_pid for
            %% kill-based shutdown.
            {os_pid, OsPid} = erlang:port_info(P, os_pid),
            unlink(P),
            erlang:port_close(P),
            Slave = #{
                short => Short,
                name => Name,
                os_pid => OsPid,
                ready_file => ReadyFile,
                log_file => LogFile,
                probe_config => ProbeConfig,
                cert => Cert,
                key => Key,
                cookie => Cookie,
                slave_port => Port
            },
            {ok, Slave};
        {error, Reason} ->
            catch erlang:port_close(P),
            error({slave_ready_timeout, Short, Reason, LogFile})
    end.

%% @doc Stop a slave. Polite halt via quic_call, fallback to SIGKILL.
-spec stop_slave(map()) -> ok.
stop_slave(Slave) ->
    catch qcall(Slave, init, stop, [], 1000),
    timer:sleep(150),
    case maps:get(os_pid, Slave, undefined) of
        undefined ->
            ok;
        OsPid ->
            os:cmd(
                "kill -TERM " ++ integer_to_list(OsPid) ++
                    " 2>/dev/null"
            ),
            timer:sleep(150),
            os:cmd(
                "kill -9 " ++ integer_to_list(OsPid) ++
                    " 2>/dev/null"
            )
    end,
    ok.

%% @doc One-shot RPC into a slave via quic_call.sh. Returns the term
%% the slave returned, or `{error, _}' on script failure.
-spec qcall(map(), module(), atom(), list()) -> term().
qcall(Slave, M, F, A) ->
    qcall(Slave, M, F, A, 10000).

-spec qcall(map(), module(), atom(), list(), pos_integer()) -> term().
qcall(#{name := Name, cookie := Cookie, probe_config := Cfg}, M, F, A, Timeout) ->
    Script = quic_call_path(),
    ArgsTerm = lists:flatten(io_lib:format("~p", [A])),
    Cmd = lists:flatten(
        io_lib:format(
            "~ts -c ~ts -C ~ts -t ~p ~ts ~ts ~ts \"~ts\" 2>&1",
            [Script, Cookie, Cfg, Timeout, Name, M, F, escape(ArgsTerm)]
        )
    ),
    Out = string:trim(os:cmd(Cmd)),
    case parse_term(Out) of
        {error, _} = Err ->
            ct:pal(
                "qcall(~p, ~p, ~p, ~p) failed:~n~ts",
                [Name, M, F, A, Out]
            ),
            Err;
        Term ->
            Term
    end.

escape(S) ->
    %% Backslash-escape any embedded double quotes, and any backslashes.
    Esc = lists:flatten([escape_char(C) || C <- S]),
    Esc.

escape_char($") -> [$\\, $"];
escape_char($\\) -> [$\\, $\\];
escape_char(C) -> [C].

parse_term("") ->
    {error, empty_output};
parse_term(S) ->
    case erl_scan:string(S ++ ".") of
        {ok, Toks, _} ->
            case erl_parse:parse_term(Toks) of
                {ok, T} -> T;
                {error, _} -> {error, {parse, S}}
            end;
        {error, _, _} ->
            {error, {scan, S}}
    end.

%% @doc Poll a 0-arity predicate until it returns true.
-spec wait_until(fun(() -> boolean()), pos_integer()) -> ok | timeout.
wait_until(Pred, TotalMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TotalMs,
    do_wait_until(Pred, Deadline).

do_wait_until(Pred, Deadline) ->
    case (catch Pred()) of
        true ->
            ok;
        _ ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    timeout;
                false ->
                    timer:sleep(100),
                    do_wait_until(Pred, Deadline)
            end
    end.

%%====================================================================
%% Internal: render configs and eval scripts
%%====================================================================

%% Sys.config used by quic_call.sh probes. Just needs cert/key paths
%% in {quic, [{dist, [...]}]} so the script can auto-parse them, plus
%% the static node map so the probe can resolve the target.
render_probe_config(Dir, Cert, Key, Nodes) ->
    Path = filename:join(Dir, "probe.config"),
    NodesTerm = io_lib:format("~p", [Nodes]),
    Content = io_lib:format(
        "%% Auto-generated probe sys.config~n"
        "[~n"
        "  {quic, [~n"
        "     {dist, [~n"
        "        {discovery_module, quic_discovery_static},~n"
        "        {nodes, ~ts},~n"
        "        {cert_file, \"~ts\"},~n"
        "        {key_file, \"~ts\"},~n"
        "        {verify, verify_none}~n"
        "     ]}~n"
        "  ]}~n"
        "].~n",
        [NodesTerm, Cert, Key]
    ),
    ok = file:write_file(Path, Content),
    Path.

%% Inline `-eval' for the slave: load quic, set the static nodes
%% map, set the mycelium env, start mycelium, write READY, sleep.
build_slave_eval(Cert, Key, Nodes, MyEnv, ReadyFile) ->
    NodesTerm = lists:flatten(io_lib:format("~p", [Nodes])),
    MyEnvTerm = lists:flatten(io_lib:format("~p", [MyEnv])),
    lists:flatten(
        io_lib:format(
            "io:format(standard_error, \"slave starting ~~p~~n\", [node()]),"
            "logger:set_primary_config(level, info),"
            "{ok,_}=application:ensure_all_started(quic),"
            "Nodes=~ts,"
            "application:set_env(quic,dist,"
            "[{cert_file,\"~ts\"},{key_file,\"~ts\"},{verify,verify_none},"
            "{discovery_module,quic_discovery_static},{nodes,Nodes}]),"
            "{ok,_}=quic_discovery_static:init([{nodes,Nodes}]),"
            "lists:foreach(fun({K,V}) -> "
            "  application:set_env(mycelium, K, V) end, ~ts),"
            "{ok,_}=application:ensure_all_started(mycelium),"
            "io:format(standard_error, \"slave ready ~~p~~n\", [node()]),"
            "ok=file:write_file(\"~ts\", <<>>),"
            "receive _ -> ok end.",
            [NodesTerm, Cert, Key, MyEnvTerm, ReadyFile]
        )
    ).

%%====================================================================
%% Internal: file polling, port tee
%%====================================================================

wait_for_ready_simple(File, Port, TotalMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TotalMs,
    poll_simple(File, Port, Deadline).

poll_simple(File, Port, Deadline) ->
    case filelib:is_regular(File) of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    {error, deadline};
                false ->
                    receive
                        {Port, {exit_status, S}} ->
                            {error, {slave_exited, S}}
                    after 100 ->
                        poll_simple(File, Port, Deadline)
                    end
            end
    end.

%% Build a shell command that runs `erl' and redirects all its
%% stdout/stderr into LogFile. We use bash's `exec' so the PID we
%% capture is the BEAM, not a shell wrapper. Args are individually
%% shell-quoted via single-quote-and-escape.
build_shell_cmd(ErlExe, Args, LogFile) ->
    QuotedArgs = string:join([shell_quote(A) || A <- Args], " "),
    QuotedLog = shell_quote(LogFile),
    %% Plain redirect (no `exec' prefix; some sh implementations
    %% interpret it as a command rather than a builtin when the
    %% executable path is also present).
    shell_quote(ErlExe) ++ " " ++ QuotedArgs ++
        " > " ++ QuotedLog ++ " 2>&1".

shell_quote(S) when is_list(S) ->
    %% Wrap in single quotes; escape any embedded single quotes by
    %% closing/restarting the quoting (`'\''`).
    "'" ++ lists:flatten(string:replace(S, "'", "'\\''", all)) ++ "'".

%% Legacy tee_loop kept for any future port-based driver; unused
%% now that we redirect via shell.
-compile({nowarn_unused_function, [{tee_loop, 2}]}).
tee_loop(Port, Fd) ->
    receive
        {Port, {data, D}} ->
            file:write(Fd, D),
            tee_loop(Port, Fd);
        {Port, {exit_status, _}} ->
            file:close(Fd),
            ok;
        stop ->
            file:close(Fd),
            ok
    end.

to_string(B) when is_binary(B) -> binary_to_list(B);
to_string(L) when is_list(L) -> L.
