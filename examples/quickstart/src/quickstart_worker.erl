%%% A trivial worker that registers itself in the cluster-wide service
%%% registry on start, so any node can discover and call it. It answers
%%% {work, X} with a reply tagged by the node that handled it, which makes
%%% cross-node routing visible in the two-node demo.
-module(quickstart_worker).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% Trap exits so terminate/2 runs on a graceful shutdown and we
    %% unregister. (The registry also drops the entry automatically when
    %% this process dies, since it monitors the registered pid.)
    process_flag(trap_exit, true),
    %% register_service/2 registers the CALLING process (this gen_server).
    %% A generic name, local-preferred by whereis_service/1:
    ok = mycelium:register_service(quickstart_worker, #{node => node()}),
    %% ...and a per-node name so a specific node can be targeted:
    ok = mycelium:register_service({worker, node()}, #{}),
    io:format("[~p] quickstart_worker registered~n", [node()]),
    {ok, #{}}.

handle_call({work, X}, _From, State) ->
    {reply, {worked_on, node(), X}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    mycelium:unregister_service(quickstart_worker),
    mycelium:unregister_service({worker, node()}),
    ok.
