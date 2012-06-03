-module(bigstore_tcpd).

-behaviour(gen_tcpd).

-export([start_link/0]).

-export([init/1,
         handle_connection/2,
         handle_info/2,
         terminate/2]).

-include("bigstore.hrl").

start_link() ->
    {ok, TcpPort} = application:get_env(data_port),
    {ok, TcpAcceptors} = application:get_env(data_acceptors),
    gen_tcpd:start_link({local, ?MODULE}, ?MODULE, [], tcp, TcpPort,
                        [{acceptors, TcpAcceptors}, {link_acceptors, false},
                         {socket_options, [binary, {active, false}, {reuseaddr, true}, {keepalive, true},
                                           {recbuf, ?CHUNK_SIZE}, {sndbuf, ?CHUNK_SIZE},
                                           {send_timeout, ?SEND_TIMEOUT}, {send_timeout_close, true}]}]).

init(_Args) ->
    {ok, none}.

handle_connection(Socket, _State) ->
    bigstore_coordinator:start(gen_tcpd:sock(Socket)).

handle_info(_Info, _State) ->
    {stop, not_implemented}.

terminate(_Reason, _State) ->
    ok.
