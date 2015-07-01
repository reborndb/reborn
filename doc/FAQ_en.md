####What is Reborn?
Reborn is a distributed redis service developed by RebornDB team, reborn can be viewed as an redis server with infinite memory, have the ability of dynamically elastic scaling,  it's more fit for storage business, if you need SUBPUB-like command, Reborn is not supported, always remember Reborn is a distributed storage system.

###Does Reborn support etcd ? 

Yes, please read the tutorial


####Can I use Reborn directly in my existing services?

That depends.  
Two cases:  
1) Twemproxy users:  
Yes, reborn fully support twemproxy commands, further more, using redis-port tool, you can synchronization the data on twemproxy onto your Reborn cluster.

2) Raw redis users:  
That depends, if you use the following commands

KEYS, MOVE, OBJECT, RENAME, RENAMENX, SORT, SCAN, BITOP,MSETNX, BLPOP, BRPOP, BRPOPLPUSH, PSUBSCRIBE，PUBLISH, PUNSUBSCRIBE,  SUBSCRIBE,  UNSUBSCRIBE,  DISCARD, EXEC, MULTI,  UNWATCH,  WATCH, SCRIPT EXISTS, SCRIPT FLUSH, SCRIPT KILL, SCRIPT LOAD, AUTH, ECHO, SELECT, BGREWRITEAOF, BGSAVE, CLIENT KILL, CLIENT LIST, CONFIG GET, CONFIG SET, CONFIG RESETSTAT, DBSIZE, DEBUG OBJECT, DEBUG SEGFAULT, FLUSHALL, FLUSHDB, LASTSAVE, MONITOR, SAVE, SHUTDOWN, SLAVEOF, SLOWLOG, SYNC, TIME

you should modify your code, because Reborn does not support these commands.
