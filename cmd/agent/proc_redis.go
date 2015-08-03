// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"os"
	"path"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

type redisArgs struct {
	Addr string `json:"addr"`
	// add customized args later
}

// reborn-server [configfile] --port 6380 --bind 127.0.0.1 [other options]
func startRedis(args *redisArgs) (*process, error) {
	p := newDefaultProcess("reborn-server", redisType)

	p.Daemonize = true

	if len(redisConfigFile) > 0 {
		p.addCmdArgs(redisConfigFile)
	}

	seps := strings.Split(args.Addr, ":")

	if len(seps) != 2 {
		return nil, errors.Errorf("redis addr must be ip:port format")
	}

	p.Ctx["addr"] = args.Addr

	// if storeDataPath exists, MkdirAll will do nothing
	storeDataPath := p.storeDataDir(args.Addr)
	os.MkdirAll(storeDataPath, 0755)

	p.addCmdArgs("--bind", seps[0])
	p.addCmdArgs("--port", seps[1])
	p.addCmdArgs("--daemonize", "yes")
	p.addCmdArgs("--logfile", path.Join(p.procLogDir(), "redis.log"))
	p.addCmdArgs("--dir", storeDataPath)
	p.addCmdArgs("--pidfile", p.pidPath())
	p.addCmdArgs("--dbfilename", "dump.rdb")
	p.addCmdArgs("--appendfilename", "appendonly.aof")

	bindRedisProcHandler(p)

	if err := p.start(); err != nil {
		log.Errorf("start redis err %v", err)
		return nil, errors.Trace(err)
	}

	addCheckProc(p)

	return p, nil
}

const (
	connectTimeout = 3 * time.Second
	readTimeout    = 3 * time.Second
	writeTimeout   = 3 * time.Second
)

func newRedisConn(ctx map[string]string) (redis.Conn, error) {
	c, err := redis.DialTimeout("tcp", ctx["addr"], connectTimeout, readTimeout, writeTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}

	auth := globalEnv.StoreAuth()
	if len(auth) > 0 {
		if ok, err := redis.String(c.Do("AUTH", auth)); err != nil {
			c.Close()
			return nil, errors.Trace(err)
		} else if ok != "OK" {
			c.Close()
			return nil, errors.Errorf("auth err, need OK but got %s", ok)
		}
	}

	return c, nil
}

func bindRedisProcHandler(p *process) {
	postStart := func(p *process) error {
		c, err := newRedisConn(p.Ctx)
		if err != nil {
			return errors.Trace(err)
		}

		defer c.Close()

		if _, err := c.Do("PING"); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	stop := func(p *process) error {
		c, err := newRedisConn(p.Ctx)
		if err != nil {
			return errors.Trace(err)
		}
		defer c.Close()
		c.Do("SHUTDOWN")
		return nil
	}

	p.postStartFunc = postStart
	p.stopFunc = stop
}
