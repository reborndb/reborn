# Reborn Tutorial

Codes is a distributed Redis solution, there is no obvious difference between connecting to a Reborn proxy and an original Redis server(?), top layer application can connect to Reborn as normal standalone Redis, Reborn will forward low layer requests. Hot data migration and all things in the shadow are transparent to client. Simply treat Coids as a Redis service with unlimited RAM. 

Reborn has four parts:
* Reborn Proxy(proxy)
* Reborn manager(cconfig)
* Reborn Redis
* ZooKeeper

`reborn-proxy` is the proxy service of client connections, `reborn-proxy` is a Redis protocol implementation, perform as an original Redis(just like Twemproxy). You can deploy multiple `reborn-proxy` for one business, `reborn-proxy` is none-stateful.

`reborn-config` is the configuration to for Reborn, support actions like add/remove Redis node, add/remove Proxy node and start data migaration, etc. `reborn-config` has a built-in http server which can start a dashboard for user to monitor the status of Reborn cluster in browser.

`reborn-server` is a branch of Redis maintain by Reborn project, based on 2.8.13, add support for slot and atomic data migration. `reborn-proxy` and `reborn-config` can only work properly with this specific version of Redis.

Reborn depend on ZooKeeper to store data routing table and meta data of `reborn-proxy` node, `reborn-config` actions will go through ZooKeeper, then synchronize up to alive `reborn-proxy`.

Reborn support namespace, configs of products with different name  won’t be conflict.

## Build reborn-proxy & reborn-config

Install Go please check [this document](https://github.com/astaxie/build-web-application-with-golang/blob/master/ebook/01.1.md). Then follow these hints:

```
go get github.com/reborndb/reborn
cd $GOPATH/src/github.com/reborndb/reborn
./bootstrap.sh
make gotest
```

Two executable file `codas-config` and `reborn-proxy` should be generated in `reborn/bin`(`bin/assets` is the resources for `reborn-config` dashboard, should be placed at same directory with `reborn-config`).

```
cd sample

$ ../bin/reborn-config -h                                                                                                                                                                                                                           (master)
usage: reborn-config  [-c <config_file>] [-L <log_file>] [--log-level=<loglevel>]
        <command> [<args>...]
options:
   -c   config file path, default: ./config.ini
   -L   log output path, default: stdout
   --log-level=<loglevel>   (debug < info (default) < warn < error < fatal)

commands:
    server            redis group management
    slot              slot management
    dashboard         start dashboard
    action            action management
    proxy             proxy management
```

```
$ ../bin/reborn-proxy -h

usage: proxy [-c <config_file>] [-L <log_file>] [--log-level=<loglevel>] [--cpu=<cpu_num>] [--addr=<proxy_listen_addr>] [--http-addr=<debug_http_server_addr>]

options:
   -c	set config file
   -L	set output log file, default is stdout
   --log-level=<loglevel>	set log level: info, warn, error, debug [default: info]
   --cpu=<cpu_num>		num of cpu cores that proxy can use
   --addr=<proxy_listen_addr>		proxy listen address, example: 0.0.0.0:9000
   --http-addr=<debug_http_server_addr>		debug vars http server
```

## Deploy

### Configuration file

`reborn-config` and `reborn-proxy` will take `config.ini` in current directory by default without a specific `-c`.

`config.ini`:

```
zk=localhost:2181   <- Location of `zookeeper`, use `zk=hostname1:2181,hostname2:2181,hostname3:2181,hostname4:2181,hostname5:2181` for `zookeeper` clusters.
`zk=http://hostname1:2181,http://hostname2:2181,http://hostname3:2181 for `etcd` clusters.
product=test        <- Product name, also the name of this Coids clusters, can be considered as namespace, Reborn with different names have no intersection. 
proxy_id=proxy_1    <- Proxy will take this as identifier for proxy, multiple proxy can use different `config.ini` with various `proxy_id`.
dashboard_addr=localhost:18087  <- dashboard provides the RESTful API for CLI
coordinator=zookeeper  <-replace zookeeper to etcd if you are using etcd.
```

### Workflow
0. Execute `reborn-config dashboard` , start dashboard.
1. Execute `reborn-config slot init` to initialize slots
2. Starting and compiling a Reborn Redis has no difference from a normal Redis Server
3. Add Redis server group, each server group as a Redis server group, only one master is allowed while could have multiple slaves. Group id only support integer lager than 1.

```
$ ../bin/reborn-config server -h
usage:
    reborn-config server list
    reborn-config server add <group_id> <redis_addr> <role>
    reborn-config server remove <group_id> <redis_addr>
    reborn-config server promote <group_id> <redis_addr>
    reborn-config server add-group <group_id>
    reborn-config server remove-group <group_id>
```

For example: Add two server group with the ids of 1 and 2, each has two Redis instances, a master and a slave.

First, add a group with id of 1 and assign a Redis master to it:

```
$ ./reborn-config server add 1 localhost:6379 master
```

Second, assign a Redis slave to this group:

```
$ ./reborn-config server add 1 localhost:6380 slave
```

Then the group with id of 2:

```
$ ./reborn-config server add 2 localhost:6479 master
$ ./reborn-config server add 2 localhost:6479 slave
```

4. Config slot range of server group

Codes implement data segmentation with Pre-sharding mechanism, 1024 slots will be segmented by default,a single key use following formula to determine which slot to resident, each slot has a server group id represents the server group which will provide service.

```
$ ./reborn-config slot -h                                                                                                                                                                                                                     
usage:
    reborn-config slot init
    reborn-config slot info <slot_id>
    reborn-config slot set <slot_id> <group_id> <status>
    reborn-config slot range-set <slot_from> <slot_to> <group_id> <status>
    reborn-config slot migrate <slot_from> <slot_to> <group_id> [--delay=<delay_time_in_ms>]
```

For exmaple, config server group 1 provide service for slot [0, 511], server group 2 provide service for slot [512, 1023]

```
$ ./reborn-config slot range-set 0 511 1 online
$ ./reborn-config slot range-set 512 1023 2 online
```

5. Start `reborn-proxy`

```
../bin/reborn-proxy -c config.ini -L ./log/proxy.log  --cpu=8 --addr=0.0.0.0:19000 --http-addr=0.0.0.0:11000
```

`codas-proxy`’s status are now `offline`, put it `online` to provide service:

```
 ../bin/reborn-config -c config.ini proxy online <proxy_name>  <---- proxy id, e.g. proxy_1
```

6. Open http://localhost:18087/admin in browser

Now you can achieve operations in browser. Enjoy!

## Data Migration

Reborn offers a reliable and transparent data migration mechanism, also it’s a killer feature which made Reborn distinguished from other static distributed Redis solution, such as Twemproxy.

The minimum data migration unit is `key`, we add some specific actions—such as `SLOTSMGRT`—to Reborn to support migration based on `key`, which will send a random record of a slot to another Reborn Redis instance each time, after the transportation is confirmed the original record will be removed from slot and return slot’s length. The action is atomically.

For example: migrate data in slot with ID from 0 to 511 to server group 2, `--delay` is the sleep duration after each transportation of record, which is used to limit speed, default value is 0. 

```
$ ../bin/reborn-config slot migrate 0 511 2 --delay=10
```

Migration progress is reliable and transparent, data won’t vanish and top layer application won’t terminate service. 

Notice that migration task could be paused, but if there is a paused task, it must be fulfilled before another start(means only one migration task is allowed at the same time). 

## Auto Rebalance

Reborn support dynamic slots migration based on RAM usage to balance data distribution.
 
```
$../bin/reborn-config slot rebalance
```

Requirements:
 * all reborn-server must set maxmemory.
 * All slots’ status should be `online`, namely no transportation task is running. 
 * All server groups must have a master. 
