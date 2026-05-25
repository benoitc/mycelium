%%% Public API for the quickstart example: discover a worker through the
%%% service registry and call it. This is the pattern you reach for in a
%%% real app: look up a service by name, then talk to the real pid over
%%% standard Erlang distribution.
-module(quickstart).

-export([work/1, work_on/2, peers/0, who/0]).

%% Send work to ANY quickstart_worker in the cluster (local preferred).
-spec work(term()) -> {worked_on, node(), term()} | {error, no_worker}.
work(X) ->
    call(quickstart_worker, {work, X}).

%% Send work to the worker on a SPECIFIC node, discovered by name.
-spec work_on(node(), term()) -> {worked_on, node(), term()} | {error, no_worker}.
work_on(Node, X) ->
    call({worker, Node}, {work, X}).

%% The current gossip peers (HyParView active view), not all reachable nodes.
-spec peers() -> [node()].
peers() ->
    mycelium:active_view().

%% This node's name and Ed25519 key fingerprint (what you share out of band).
-spec who() -> {node(), binary()}.
who() ->
    {ok, Pub} = mycelium_dist_auth:get_public_key(),
    {node(), mycelium_dist_keys:fingerprint(Pub)}.

%% whereis_service/1 returns {ok, Pid} for a local service and
%% {ok, Node, Pid} for a remote one; handle both, then send normally.
call(Name, Msg) ->
    case mycelium:whereis_service(Name) of
        {ok, Pid} -> gen_server:call(Pid, Msg);
        {ok, _Node, Pid} -> gen_server:call(Pid, Msg);
        {error, not_found} -> {error, no_worker}
    end.
