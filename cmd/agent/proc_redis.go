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
	Port string `json:"port"`

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
	p.Ctx = args2Map(args)

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

func bindRedisProcHandler(p *process) {
	postStart := func(p *process) error {
		c, err := redis.DialTimeout("tcp", fmt.Sprintf(":%s", p.Ctx["port"]), 3*time.Second, 3*time.Second, 3*time.Second)
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
		c, err := redis.Dial("tcp", fmt.Sprintf(":%s", p.Ctx["port"]))
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
