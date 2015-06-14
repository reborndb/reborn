// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
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
		return nil, fmt.Errorf("redis addr must be ip:port format")
	}

	p.Ctx["addr"] = args.Addr

	p.addCmdArgs("--bind", seps[0])
	p.addCmdArgs("--port", seps[1])
	p.addCmdArgs("--daemonize", "yes")
	p.addCmdArgs("--logfile", path.Join(p.baseLogDir(), "redis.log"))
	p.addCmdArgs("--dir", p.baseDataDir())
	p.addCmdArgs("--pidfile", p.pidPath())
	p.addCmdArgs("--dbfilename", "dump.rdb")
	p.addCmdArgs("--appendfilename", "appendonly.aof")

	bindRedisProcHandler(p)

	if err := p.start(); err != nil {
		log.Errorf("start redis err %v", err)
		return nil, err
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
		return nil, err
	}

	auth := globalEnv.StoreAuth()
	if len(auth) > 0 {
		if ok, err := redis.String(c.Do("AUTH", auth)); err != nil {
			c.Close()
			return nil, err
		} else if ok != "OK" {
			c.Close()
			return nil, fmt.Errorf("auth err, need OK but got %s", ok)
		}
	}

	return c, nil
}

func bindRedisProcHandler(p *process) {
	postStart := func(p *process) error {
		c, err := newRedisConn(p.Ctx)
		if err != nil {
			return err
		}

		defer c.Close()

		if _, err := c.Do("PING"); err != nil {
			return err
		}
		return nil
	}

	stop := func(p *process) error {
		c, err := newRedisConn(p.Ctx)
		if err != nil {
			return err
		}
		defer c.Close()
		c.Do("SHUTDOWN")
		return nil
	}

	p.postStartFunc = postStart
	p.stopFunc = stop
}
