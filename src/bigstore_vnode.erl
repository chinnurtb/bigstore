-module(bigstore_vnode).

-behaviour(riak_core_vnode).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-include("bigstore.hrl").

-record(state, {partition, handoff_blob}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    State = #state{partition = Partition},
    ok = filelib:ensure_dir(blobs_temp_path(State)),
    ok = filelib:ensure_dir(blobs_path(State)),
    ok = filelib:ensure_dir(index_path(State)),
    os:cmd("rm -rf " ++ blobs_temp_path(State) ++ "*"),
    {ok, Partition} = dets:open_file(Partition, [{file, index_path(State)}]),
    {ok, State}.

handle_command({put_blob, Key, Blob}, _Sender, State) ->
    Res = file:write_file(blob_path(State, Key), Blob),
    {reply, Res, State};
handle_command({get_blob, Key}, _Sender, State) ->
    Res = file:read_file(blob_path(State, Key)),
    {reply, Res, State};
handle_command(get_data_host_port, _Sender, State) ->
    {reply, data_server_info(State), State};
handle_command({has_blob, Ref, From, Key}, _Sender, State) ->
    case dets:lookup(State#state.partition, Key) of
        [{Key, Time, deleted}] ->
            From ! {deleted, Ref, data_server_info(State), Time};
        [{Key, Time, Hash}] ->
            Size = filelib:file_size(blob_path(State, Key)),
            From ! {true, Ref, data_server_info(State), Size, Time, Hash};
        [] ->
            From ! {false, Ref, data_server_info(State)}
    end,
    {noreply, State};
handle_command({delete_blob, Ref, From, Key, Time}, _Sender, State) ->
    case insert_blob_index(State#state.partition, Key, Time, deleted) of
        ok ->
            file:delete(blob_path(State, Key)),
            os:cmd("sync"),
            From ! {ok, Ref};
        {error, already_deleted} ->
            From ! {ok, Ref};
        {error, old_blob} ->
            From ! {error, Ref, old_blob}
    end,
    {noreply, State}.

is_empty(State) ->
    {ok, Blobs} = file:list_dir(blobs_path(State)),
    {Blobs == [], State}.

delete(State) ->
    {ok, Temps0} = file:list_dir(blobs_temp_path(State)),
    Temps = lists:map(fun(T) -> blob_temp_path(State, T) end, Temps0),
    lists:foreach(fun(T) -> ok = file:delete(T) end, Temps),
    ok = file:del_dir(blobs_temp_path(State)),
    ok = file:del_dir(blobs_temp_path_partition(State)),
    {ok, Blobs0} = file:list_dir(blobs_path(State)),
    Blobs = lists:map(fun(B) -> blob_path(State, B) end, Blobs0),
    lists:foreach(fun(B) -> ok = file:delete(B) end, Blobs),
    ok = file:del_dir(blobs_path(State)),
    ok = file:delete(index_path(State)),
    ok = file:del_dir(filename:dirname(index_path(State))),
    os:cmd("sync"),
    {ok, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_command(?FOLD_REQ{foldfun = Fun0, acc0 = Acc0}, _Sender, State0) ->
    Fun = fun(Blob, {State, Acc}) ->
            read_blob_chunks(State, Blob, Fun0, Acc)
    end,
    {ok, Blobs} = file:list_dir(blobs_path(State0)),
    {State, Acc} = lists:foldl(Fun, {State0, Acc0}, Blobs),
    {reply, Acc, State};
handle_handoff_command(_Req, _Sender, State) ->
    {forward, State}.

encode_handoff_item(Blob, Cmd) ->
    term_to_binary({Blob, Cmd}).

handle_handoff_data(BinObj, State) ->
    {Blob, Cmd} = binary_to_term(BinObj),
    State2 = exec_blob_cmd(State, Blob, Cmd),
    {reply, ok, State2}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    dets:close(State#state.partition).

data_server_info(State) ->
    Host = net_adm:localhost(),
    {ok, Port} = application:get_env(bigstore, data_port),
    {Host, Port, State#state.partition}.

index_path(State) ->
    ?INDEXES_PATH ++ integer_to_list(State#state.partition) ++ "/index".

blobs_path(State) ->
    ?BLOBS_PATH ++ integer_to_list(State#state.partition) ++ "/".

blobs_temp_path(State) ->
    ?TEMPS_PATH ++ integer_to_list(State#state.partition) ++ "/handoff/".

blobs_temp_path_partition(State) ->
    ?TEMPS_PATH ++ integer_to_list(State#state.partition) ++ "/".

blob_path(State, Blob) -> blobs_path(State) ++ Blob.

blob_temp_path(State, Blob) -> blobs_temp_path(State) ++ Blob.

read_blob_chunks(State = #state{handoff_blob = undefined}, Blob, Fun, Acc) ->
    {ok, Fd} = file:open(blob_path(State, Blob), [read, raw, binary]),
    Acc2 = Fun(Blob, create, Acc),
    read_blob_chunks(State#state{handoff_blob = {Blob, Fd}}, Fun, Acc2).

read_blob_chunks(State = #state{handoff_blob = {Blob, Fd}}, Fun, Acc) ->
    case file:read(Fd, ?CHUNK_SIZE) of
        {ok, Data} ->
            Acc2 = Fun(Blob, {write, Data}, Acc),
            read_blob_chunks(State, Fun, Acc2);
        eof ->
            [{_, Time, Hash}] = dets:lookup(State#state.partition, Blob),
            ok = file:close(Fd),
            {State#state{handoff_blob = undefined}, Fun(Blob, {close, Time, Hash}, Acc)}
    end.

exec_blob_cmd(State = #state{handoff_blob = undefined}, Blob, create) ->
    {ok, Fd} = file:open(blob_temp_path(State, Blob), [write, exclusive, raw]),
    State#state{handoff_blob = {Blob, Fd}};
exec_blob_cmd(State = #state{handoff_blob = {Blob, Fd}}, Blob, {write, Data}) ->
    ok = file:write(Fd, Data),
    State;
exec_blob_cmd(State = #state{handoff_blob = {Blob, Fd}}, Blob, {close, Time, Hash}) ->
    ok = file:sync(Fd),
    ok = file:close(Fd),
    case insert_blob_index(State, Time, Hash) of
        ok ->
            ok = file:rename(blob_temp_path(State, Blob), blob_path(State, Blob)),
            os:cmd("sync");
        {error, old_blob} ->
            ok = file:delete(blob_temp_path(State, Blob))
    end,
    State#state{handoff_blob = undefined}.

insert_blob_index(#state{partition = Partition, handoff_blob = {Blob, _}}, Time, Hash) ->
    insert_blob_index(Partition, Blob, Time, Hash).

insert_blob_index(Partition, Blob, Time, Hash) ->
    case dets:insert_new(Partition, {Blob, Time, Hash}) of
        true ->
            dets:sync(Partition),
            ok;
        false ->
            case dets:lookup(Partition, Blob) of
                [] ->
                    insert_blob_index(Partition, Blob, Time, Hash);
                [{Blob, Time2, deleted}] when Hash == deleted, Time >= Time2 ->
                    {error, already_deleted};
                [Obj = {Blob, Time2, _}] when Time > Time2 ->
                    ok = dets:delete_object(Partition, Obj),
                    insert_blob_index(Partition, Blob, Time, Hash);
                _ ->
                    {error, old_blob}
            end
    end.
