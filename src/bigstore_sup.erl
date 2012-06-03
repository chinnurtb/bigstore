-module(bigstore_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include("bigstore.hrl").

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    {ok, {{one_for_one, 20, 10}, [?CHILD(?VNODE_MASTER, riak_core_vnode_master, worker, [bigstore_vnode]),
                                 ?CHILD(bigstore_tcpd, bigstore_tcpd, worker, [])]}}.
