// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"path"
	"time"

	"github.com/garyburd/redigo/redis"
	log "github.com/ngaut/logging"
)

type redisArgs struct {
	Port     string `json:"port"`
	Password string `json:"password"`
	// add customized args later
}

// reborn-server [configfile] --port 6380 [other options]
func startRedis(args *redisArgs) (*process, error) {
	p := newDefaultProcess("reborn-server", redisType)

	p.Daemonize = true

	if len(redisConfigFile) > 0 {
		p.addCmdArgs(redisConfigFile)
	}

	if len(args.Port) == 0 {
		return nil, fmt.Errorf("redis must have a specail port, not empty")
	}
	p.Ctx["addr"] = fmt.Sprintf(":%s", args.Port)
	p.Ctx["password"] = args.Password

	p.addCmdArgs("--port", args.Port)
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

const connectTimeout = 3 * time.Second
const readTimeout = 3 * time.Second
const writeTimeout = 3 * time.Second

func newRedisConn(ctx map[string]string) (redis.Conn, error) {
	c, err := redis.DialTimeout("tcp", ctx["addr"], connectTimeout, readTimeout, writeTimeout)
	if err != nil {
		return nil, err
	}

	auth := ctx["password"]
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
