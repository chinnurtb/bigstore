-module(bigstore_app).

-behaviour(application).

-export([start/2,
         stop/1]).

-include("bigstore.hrl").

start(_StartType, _StartArgs) ->
    os:cmd("rm -rf " ++ ?TEMPS_PATH ++ "*"),
    case bigstore_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, bigstore_vnode}]),
            ok = riak_core_ring_events:add_guarded_handler(bigstore_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(bigstore_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(bigstore, self()),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
