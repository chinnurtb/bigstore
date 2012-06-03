-module(bigstore).

-export([put_blob/2,
         get_blob/1,
         key_data_host_port/1,
         node_data_host_port/1,
         has_blob/1]).

-include("bigstore.hrl").

-define(RPC_TIMEOUT, timer:seconds(5)).

-ifdef(DEBUG).
-export([put_test_blob/1,
         put_test_blobs/1,
         create_test_blob/1,
         create_test_blobs/1]).
-endif.

put_blob(Key, Value) when is_list(Key) ->
    do_request(Key, {put_blob, Key, Value}).

get_blob(Key) when is_list(Key) ->
    do_request(Key, {get_blob, Key}).

key_data_host_port(Key) when is_list(Key) ->
    do_request(Key, get_data_host_port).

node_data_host_port(NodeIdx) ->
    riak_core_vnode_master:sync_command(NodeIdx, get_data_host_port, ?VNODE_MASTER).

has_blob(Key) when is_list(Key) ->
    Ref = make_ref(),
    do_multi_request(Key, {has_blob, Ref, self(), Key}),
    recv_responses(Ref).

do_request(Key, Msg) ->
    KeyIdx = bigstore_util:chash_key(Key),
    [NodeIdx | _] = bigstore_util:get_preflist(KeyIdx),
    riak_core_vnode_master:sync_command(NodeIdx, Msg, ?VNODE_MASTER).

do_multi_request(Key, Msg) ->
    KeyIdx = bigstore_util:chash_key(Key),
    NodeIdxs = bigstore_util:get_preflist(KeyIdx),
    riak_core_vnode_master:command(NodeIdxs, Msg, ?VNODE_MASTER).

recv_responses(Ref) ->
    erlang:send_after(?RPC_TIMEOUT, self(), {timeout, Ref}),
    {ok, N} = application:get_env(bigstore, replica_size),
    recv_responses2(Ref, [], N).

recv_responses2(_, ResList, 0) ->
    select_recent_blob(ResList);
recv_responses2(Ref, ResList, N) ->
    receive
        {timeout, Ref} ->
            select_recent_blob(ResList);
        {true, Ref, Node, Size, Time, Hash} ->
            recv_responses2(Ref, [{Node, Size, Time, Hash} | ResList], N - 1);
        {false, Ref, _} ->
            recv_responses2(Ref, ResList, N - 1)
    end.

select_recent_blob([]) ->
    {error, none};
select_recent_blob(ResList) ->
    MaxTime = lists:max(lists:map(fun({_, _, T, _}) -> T end, ResList)),
    {_, Size, _, Hash} = lists:keyfind(MaxTime, 3, ResList),
    RecentList = lists:filter(fun({_, _, _, H}) -> H == Hash end, ResList),
    Nodes = lists:map(fun({N, _, _, _}) -> N end, RecentList),
    {ok, Size, Hash, Nodes}.

-ifdef(DEBUG).
put_test_blob(MSize) ->
    {ok, Blob} = file:read_file(io_lib:format("/tmp/blob~B", [MSize])),
    put_blob(integer_to_list(MSize), Blob).

put_test_blobs(N) ->
    lists:foreach(fun put_test_blob/1, lists:seq(1, N)).

create_test_blob(MSize) ->
    Cmd = io_lib:format("dd if=/dev/urandom of=/tmp/blob~B bs=1M count=~B", [MSize, MSize]),
    os:cmd(Cmd).

create_test_blobs(N) ->
    lists:foreach(fun create_test_blob/1, lists:seq(1, N)).
-endif.
