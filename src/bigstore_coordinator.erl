-module(bigstore_coordinator).

-behaviour(gen_fsm).

-export([start/1]).

-export([init/1,
         prepare/2,
         prepare_replication/2,
         read_blob/2,
         replicate_blob/2,
         write_blob/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         code_change/4,
         terminate/3]).

-include("bigstore.hrl").

-record(state, {socket, partition, blob, size, time, hash, fd, replicas}).

-define(REQ_GET_BLOB,   0).
-define(REQ_PUT_BLOB,   1).
-define(REQ_WRITE_BLOB, 2).

-define(RES_OK,                0).
-define(RES_ERROR_TIMEOUT,     1).
-define(RES_ERROR_REPLICATION, 2).
-define(RES_ERROR_OLD_BLOB,    3).
-define(RES_ERROR_TEMP_EXISTS, 4).

start(Socket) ->
    gen_fsm:enter_loop(?MODULE, [], prepare, Socket, 0).

init(_Args) ->
    {stop, not_implemented}.

prepare(timeout, Socket) ->
    {ok, Type} = gen_tcp:recv(Socket, 1),
    {ok, <<PartLen:64>>} = gen_tcp:recv(Socket, 8),
    {ok, Partition0} = gen_tcp:recv(Socket, PartLen),
    {ok, <<Len:64>>} = gen_tcp:recv(Socket, 8),
    {ok, Blob0} = gen_tcp:recv(Socket, Len),
    {ok, <<Size:64>>} = gen_tcp:recv(Socket, 8),
    Partition = list_to_integer(binary_to_list(Partition0)),
    Blob = binary_to_list(Blob0),
    State0 = #state{socket = Socket, partition = Partition, blob = Blob, size = Size,
                    time = now(), hash = crypto:sha_init()},
    Res = case Type of
        <<?REQ_GET_BLOB>> ->
            file:open(blob_path(State0, Blob), [read, raw, binary]);
        _ ->
            file:open(blob_temp_path(State0, Blob), [write, exclusive, raw])
    end,
    case Res of
        {error, eexist} ->
            {stop, temp_exists, State0};
        {ok, Fd} ->
            State = State0#state{fd = Fd},
            case Type of
                <<?REQ_GET_BLOB>> ->
                    {ok, Hash} = gen_tcp:recv(Socket, 20),
                    case dets:lookup(Partition, Blob) of
                        [{_, _, Hash}] ->
                            ok = file:advise(Fd, 0, Size, sequential),
                            {next_state, read_blob, State, 0};
                        _ ->
                            ok = file:close(Fd),
                            {stop, bad_hash, State#state.socket}
                    end;
                <<?REQ_PUT_BLOB>> ->
                    ok = file:advise(Fd, 0, Size, dont_need),
                    {next_state, prepare_replication, State, 0};
                <<?REQ_WRITE_BLOB>> ->
                    ok = file:advise(Fd, 0, Size, dont_need),
                    inet:setopts(State#state.socket, [{active, once}]),
                    {next_state, write_blob, State, ?RECV_TIMEOUT}
            end
    end.

prepare_replication(timeout, State) ->
    KeyIdx = bigstore_util:chash_key(State#state.blob),
    Replicas0 = bigstore_util:get_preflist(KeyIdx),
    Replicas = lists:filter(fun(R) -> R /= {State#state.partition, node()} end, Replicas0),
    case connect_replicas(State, Replicas) of
        {ok, State2} ->
            inet:setopts(State2#state.socket, [{active, once}]),
            {next_state, replicate_blob, State2, ?RECV_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State}
    end.

read_blob(timeout, State = #state{socket = Socket, fd = Fd}) ->
    case gen_tcp:recv(Socket, 8) of
        {ok, <<Offset:64>>} ->
            {ok, <<Len:64>>} = gen_tcp:recv(Socket, 8),
            read_blob(State, Socket, Fd, Offset, Len);
        {error, closed} ->
            ok = file:close(Fd),
            {stop, normal, State#state{socket = undefined, blob = undefined}}
    end.

read_blob(State, _Socket, _Fd, _Offset, 0) ->
    read_blob(timeout, State);
read_blob(State, Socket, Fd, Offset, Len) ->
    case split_chunk(Offset, Len) of
        {0, _, _} ->
            read_blob(timeout, State);
        {Chunk, Offset2, Len2} ->
            case file:sendfile(Fd, Socket, Offset, Chunk, []) of
                {ok, Chunk} ->
                    read_blob(State, Socket, Fd, Offset2, Len2);
                {error, closed} ->
                    ok = file:close(Fd),
                    {stop, normal, State#state{socket = undefined, blob = undefined}}
            end
    end.

split_chunk(Offset, 0) ->
    {0, Offset, 0};
split_chunk(Offset, Len) when Len > ?CHUNK_SIZE ->
    {?CHUNK_SIZE, Offset + ?CHUNK_SIZE, Len - ?CHUNK_SIZE};
split_chunk(Offset, Len) ->
    {Len, Offset + Len, 0}.

replicate_blob(timeout, State) ->
    {stop, timeout, State}.

write_blob(timeout, State) ->
    {stop, timeout, State}.

handle_event(_Event, _StateName, State) ->
    {stop, not_implemented, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, not_implemented, State}.

handle_info(Msg = {tcp, Socket, Data}, replicate_blob, State = #state{socket = Socket}) ->
    case send_replicas(State, Data) of
        ok ->
            case handle_info(Msg, write_blob, State) of
                {next_state, _, State2, Timeout} ->
                    {next_state, replicate_blob, State2, Timeout};
                {stop, normal, State2} ->
                    case recv_replicas(State2) of
                        ResList when is_list(ResList) ->
                            case lists:dropwhile(fun(R) -> <<?RES_OK>> == R end, ResList) of
                                [] ->
                                    State3 = close_replicas(State2),
                                    {stop, normal, State3};
                                [<<Res>> | _] ->
                                    {stop, response_to_reason(Res), State2}
                            end;
                        {error, Reason} ->
                            {stop, Reason, State2}
                    end;
                {stop, Reason, State2} ->
                    {stop, Reason, State2}
            end;
        {error, Reason} ->
            {stop, Reason, State}
    end;
handle_info({tcp, Socket, Data}, write_blob, State = #state{socket = Socket}) ->
    inet:setopts(Socket, [{active, once}]),
    Hash = crypto:sha_update(State#state.hash, Data),
    ok = file:write(State#state.fd, Data),
    case State#state.size - byte_size(Data) of
        N when N > 0 ->
            {next_state, write_blob, State#state{size = N, hash = Hash}, ?RECV_TIMEOUT};
        0 ->
            State2 = State#state{hash = crypto:sha_final(Hash)},
            ok = file:sync(State2#state.fd),
            ok = file:close(State2#state.fd),
            case insert_blob_index(State2) of
                ok ->
                    ok = file:rename(blob_temp_path(State2, State2#state.blob), blob_path(State2, State2#state.blob)),
                    {stop, normal, State2#state{size = 0, fd = undefined}};
                {error, old_blob} ->
                    {stop, old_blob, State2#state{size = 0, fd = undefined}}
            end;
        _ ->
            {stop, bad_size, State}
    end;
handle_info({tcp_error, _Socket}, replicate_blob, State) ->
    {stop, {replication, tcp_error}, State};
handle_info({tcp_closed, _Socket}, replicate_blob, State) ->
    {stop, {replication, tcp_closed}, State};
handle_info({tcp_error, _Socket}, _StateName, State) ->
    {stop, tcp_error, State};
handle_info({tcp_closed, _Socket}, _StateName, State) ->
    {stop, tcp_closed, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(temp_exists, _StateName, State) ->
    send_result(State, ?RES_ERROR_TEMP_EXISTS),
    gen_tcp:close(State#state.socket);
terminate(old_blob, _StateName, State) ->
    file:delete(blob_temp_path(State, State#state.blob)),
    send_result(State, ?RES_ERROR_OLD_BLOB),
    gen_tcp:close(State#state.socket);
terminate({replication, _Reason}, _StateName, State) ->
    file:delete(blob_temp_path(State, State#state.blob)),
    send_result(State, ?RES_ERROR_REPLICATION),
    gen_tcp:close(State#state.socket);
terminate(timeout, _StateName, State) ->
    file:delete(blob_temp_path(State, State#state.blob)),
    send_result(State, ?RES_ERROR_TIMEOUT),
    gen_tcp:close(State#state.socket);
terminate(normal, _StateName, State) ->
    case State#state.blob of
        undefined ->
            ok;
        _ ->
            file:delete(blob_temp_path(State, State#state.blob))
    end,
    case State#state.socket of
        undefined ->
            ok;
        _ ->
            send_result(State, ?RES_OK),
            gen_tcp:close(State#state.socket)
    end;
terminate(_Reason, _StateName, Socket) when is_port(Socket) ->
    gen_tcp:close(Socket);
terminate(_Reason, _StateName, State) ->
    file:delete(blob_temp_path(State, State#state.blob)),
    gen_tcp:close(State#state.socket).

blobs_path(State) ->
    ?BLOBS_PATH ++ integer_to_list(State#state.partition) ++ "/".

blobs_temp_path(State) ->
    ?TEMPS_PATH ++ integer_to_list(State#state.partition) ++ "/".

blob_path(State, Blob) -> blobs_path(State) ++ Blob.

blob_temp_path(State, Blob) -> blobs_temp_path(State) ++ Blob.

connect_replicas(State, Replicas) ->
    try
        lists:map(fun(R) -> connect_replica(State, R) end, Replicas)
    of
        Replicas2 ->
            {ok, State#state{replicas = Replicas2}}
    catch
        throw:{badmatch, {error, Reason}} ->
            {error, {replication, Reason}}
    end.

connect_replica(State, NodeIdx) ->
    {Host, Port, Partition0} = bigstore:node_data_host_port(NodeIdx),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, false}, {keepalive, true},
                                                {recbuf, ?CHUNK_SIZE}, {sndbuf, ?CHUNK_SIZE},
                                                {send_timeout, ?SEND_TIMEOUT}, {send_timeout_close, true}],
                                   ?CONN_TIMEOUT),
    Partition = list_to_binary(integer_to_list(Partition0)),
    PartLen = byte_size(Partition),
    Blob = list_to_binary(State#state.blob),
    Len = byte_size(Blob),
    Size = State#state.size,
    ok = gen_tcp:send(Socket, <<?REQ_WRITE_BLOB, PartLen:64, Partition/binary,
                                Len:64, Blob/binary, Size:64>>),
    Socket.

send_replicas(State, Data) ->
    try
        Fun = fun(S) -> ok = gen_tcp:send(S, Data) end,
        lists:foreach(Fun, State#state.replicas)
    catch
        error:{badmatch, {error, Reason}} ->
            {error, {replication, Reason}}
    end.

recv_replicas(State) ->
    try
        Fun = fun(S) ->
                {ok, Res} = gen_tcp:recv(S, 1, ?RECV_TIMEOUT),
                Res
        end,
        lists:map(Fun, State#state.replicas)
    catch
        error:{badmatch, {error, Reason}} ->
            {error, {replication, Reason}}
    end.

close_replicas(State) ->
    lists:foreach(fun gen_tcp:close/1, State#state.replicas),
    State#state{replicas = undefined}.

send_result(State, Res) ->
    ok = gen_tcp:send(State#state.socket, <<Res>>).

insert_blob_index(State) ->
    case dets:insert_new(State#state.partition, {State#state.blob, State#state.time, State#state.hash}) of
        true ->
            dets:sync(State#state.partition),
            ok;
        false ->
            case dets:lookup(State#state.partition, State#state.blob) of
                [] ->
                    insert_blob_index(State);
                [Obj = {_, Time, _}] ->
                    case State#state.time > Time of
                        true ->
                            ok = dets:delete_object(State#state.partition, Obj),
                            insert_blob_index(State);
                        false ->
                            {error, old_blob}
                    end
            end
    end.

response_to_reason(?RES_ERROR_TIMEOUT)     -> timeout;
response_to_reason(?RES_ERROR_REPLICATION) -> {replication, none};
response_to_reason(?RES_ERROR_OLD_BLOB)    -> old_blob.
