#!/bin/bash

echo "if you don't install godep, this script can help you install go dependencies"
echo "downloading dependencies, it may take a few minutes..."

# the below import list is generated from Godeps.json
go get -u github.com/BurntSushi/toml
go get -u github.com/c4pt0r/cfg
go get -u github.com/codegangsta/inject
go get -u github.com/codegangsta/martini
go get -u github.com/codegangsta/martini-contrib
go get -u github.com/coreos/etcd/error
go get -u github.com/coreos/go-etcd/etcd
go get -u github.com/cupcake/rdb
go get -u github.com/docopt/docopt-go
go get -u github.com/garyburd/redigo/redis
go get -u github.com/go-martini/martini
go get -u github.com/google/go-snappy/snappy
go get -u github.com/gorilla/context
go get -u github.com/gorilla/mux
go get -u github.com/juju/errors
go get -u github.com/kardianos/osext
go get -u github.com/martini-contrib/cors
go get -u github.com/mitchellh/go-ps
go get -u github.com/ngaut/deadline
go get -u github.com/ngaut/go-zookeeper/zk
go get -u github.com/ngaut/gostats
go get -u github.com/ngaut/log
go get -u github.com/ngaut/pools
go get -u github.com/ngaut/resp
go get -u github.com/ngaut/sync2
go get -u github.com/ngaut/zkhelper
go get -u github.com/nu7hatch/gouuid
go get -u github.com/reborndb/go
go get -u github.com/reborndb/qdb
go get -u github.com/syndtr/goleveldb
go get -u github.com/ugorji/go/codec
go get -u gopkg.in/check.v1
