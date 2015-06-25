#Reborn - yet another fast distributed solution for Redis

Reborn is a proxy based high performance Redis cluster solution written in Go/C, an alternative to Twemproxy.

Reborn supports multiple stateless proxy with multiple redis instances.

Reborn is engineered to elastically scale, Easily add or remove redis or proxy instances on-demand/dynamicly.

[![Build Status](https://travis-ci.org/reborndb/reborn.svg)](https://travis-ci.org/reborndb/reborn)

## Features
* Auto rebalance
* Extremely simple to use 
* Support both redis or rocksdb transparently
* GUI dashboard & admin tools 
* Supports most of Redis commands, Fully compatible with twemproxy(https://github.com/twitter/twemproxy)
* Native Redis clients are supported
* Safe and transparent data migration, Easily add or remove nodes on-demand.
* Command-line interface is also provided
* RESTful APIs
* Monitor and Failover

## Build and Install

* Install go(we recommend go1.3.3, go1.4 has some performance issue) & ZooKeeper
* Install godep, `go get github.com/tools/godep` 
* go get github.com/reborndb/reborn
* cd reborn
* make
* make gotest
* cd sample
* follow instructions in usage.md

## Tutorial

[简体中文](https://github.com/reborndb/reborn/blob/master/doc/tutorial_zh.md)

[English](https://github.com/reborndb/reborn/blob/master/doc/tutorial_en.md)

## FAQ

[简体中文](https://github.com/reborndb/reborn/blob/master/doc/FAQ_zh.md)

[English](https://github.com/reborndb/reborn/blob/master/doc/FAQ_en.md)

## Performance (Benchmark)
```
OS:   Ubuntu SMP x86_64 GNU/Linux
CPU:  Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz(8 cores)
Mem:  16G
Disk: 256G SSD
```

+ twemproxy

```
version 0.4.1

alpha:
  listen: 127.0.0.1:22121
  hash: crc32a
  hash_tag: "{}"
  distribution: ketama
  auto_eject_hosts: false
  timeout: 400
  redis: true
  servers:
   - 127.0.0.1:6381:1
   - 127.0.0.1:6382:1
```

+ qdb-server (https://github.com/reborndb/qdb)


####1 twemproxy + 2 reborn-server  
  redis-benchmark -p 22121 -c 500 -n 5000000 -P 100 -r 10000 -t get,set -q

```
SET: 209100.03 requests per second
GET: 212404.41 requests per second
```

####1 reborn-proxy + 2 reborn-server  
  redis-benchmark -p 19000 -c 500 -n 5000000 -P 100 -r 10000 -t get,set -q

```
SET: 410273.22 requests per second
GET: 455913.19 requests per second
```

####1 twemproxy + 2 qdb-server  
  redis-benchmark -p 22121 -c 500 -n 5000000 -P 100 -r 10000 -t get,set -q

```
SET: 133212.55 requests per second
GET: 165584.84 requests per second
```
####1 reborn-proxy + 2 qdb-server  
  redis-benchmark -p 19000 -c 500 -n 5000000 -P 100 -r 10000 -t get,set -q

```
SET: 45909.04 requests per second
GET: 77690.41 requests per second
```

Result:  

![main](doc/bench.png)

## For Java users who want to support HA

[Jodis \(HA Reborn Connection Pool based on Jedis\)] (https://github.com/reborndb/reborn/tree/master/extern/jodis)

## Architecture

![architecture](doc/pictures/architecture.png)

## Snapshots

Dashboard
![main](doc/pictures/snapshot.png)

Migrate
![migrate](doc/pictures/snapshot_migrate.png)

Slots
![slots](doc/pictures/slots.png)

## Authors

* [@goroutine](https://github.com/ngaut)
* [@c4pt0r](https://github.com/c4pt0r)
* [@spinlock9](https://github.com/spinlock)
* [@siddontang](https://github.com/siddontang)
* [@qiuyesuifeng](https://github.com/qiuyesuifeng)

Thanks:

* [@ivanzhaowy](https://github.com/ivanzhaowy)
* [@Apache9](https://github.com/apache9)

## License

Reborn is licensed under MIT， see MIT-LICENSE.txt

-------------
*You are welcome to use Reborn in your product, and feel free to let us know~ :)*
