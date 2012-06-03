-define(DEBUG, true).

-define(CONN_TIMEOUT, timer:seconds(4)).
-define(SEND_TIMEOUT, timer:seconds(20)).
-define(RECV_TIMEOUT, timer:seconds(20)).

-define(VNODE_MASTER, bigstore_vnode_master).

-define(CHUNK_SIZE,   1024 * 16).
-define(TEMPS_PATH,   "data/tmp/").
-define(BLOBS_PATH,   "data/blobs/").
-define(INDEXES_PATH, "data/indexes/").
