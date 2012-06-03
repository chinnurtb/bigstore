-module(bigstore_util).

-export([chash_key/1,
         get_preflist/1,
         get_preflist/2]).

-define(KEY_BUCKET, bigstore).

chash_key(Key) when is_list(Key) ->
    riak_core_util:chash_key({term_to_binary(?KEY_BUCKET), term_to_binary(Key)});
chash_key(Key) ->
    error(badarg, [Key]).

get_preflist(KeyIdx) ->
    {ok, N} = application:get_env(bigstore, replica_size),
    riak_core_apl:get_apl(KeyIdx, N, ?KEY_BUCKET).

get_preflist(KeyIdx, N) ->
    riak_core_apl:get_apl(KeyIdx, N, ?KEY_BUCKET).
