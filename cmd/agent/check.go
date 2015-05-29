// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/ngaut/logging"
)

var procs map[string]*process = map[string]*process{}
var m sync.Mutex

func checkProcs() {
	restartProcs := []*process{}

	m.Lock()

	for _, p := range procs {
		if b, err := p.checkAlive(); err != nil {
			log.Errorf("check %d (%s) alive err %v, retry later", p.Pid, p.Cmd, err)
		} else if !b {
			needRestart := p.needRestart()
			log.Warningf("%d (%s) is not alive, need restart: %v", p.Pid, p.Cmd, needRestart)
			if needRestart {
				restartProcs = append(restartProcs, p)
			}

			p.clear()
			delete(procs, p.ID)
		}
	}

	m.Unlock()

	for _, p := range restartProcs {
		if err := p.start(); err != nil {
			log.Errorf("restart %s err %v", p.Cmd, err)
		} else {
			addCheckProc(p)
		}
	}
}

func addCheckProc(p *process) {
	if p == nil {
		return
	}

	m.Lock()
	procs[p.ID] = p
	m.Unlock()
}

func removeCheckProc(p *process) {
	m.Lock()
	delete(procs, p.ID)
	m.Unlock()
}

func loadSavedProcs() error {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.dat", dataDir))
	if err != nil {
		return errors.Trace(err)
	}

	for _, f := range files {
		if p, err := loadProcess(f); err != nil {
			log.Errorf("load process data %s err %v, skip", f, err)
		} else if p == nil {
			continue
		} else {
			// todo bind after start func for different type
			if err := bindProcHandler(p); err != nil {
				log.Errorf("bind proc %s err %v, skip", p.Cmd, err)
				continue
			}
			addCheckProc(p)
		}
	}

	//todo remove unnecessary old logs

	return nil
}

func runCheckProcs() {
	for {
		time.Sleep(1 * time.Second)

		checkProcs()
	}
}

func stopCheckProc(id string) error {
	m.Lock()
	defer m.Unlock()

	p := procs[id]
	if p == nil {
		return nil
	}

	delete(procs, id)

	return p.stop()
}

const (
	proxyType     = "proxy"
	dashboardType = "dashboard"
	redisType     = "redis"
	qdbType       = "qdb"
)

func bindProcHandler(p *process) error {
	switch strings.ToLower(p.Type) {
	case proxyType:
	case dashboardType:
	case redisType:
		bindRedisProcHandler(p)
	case qdbType:
		// qdb is same as redis, use ping for start, and use shutdown to stop
		bindRedisProcHandler(p)
	default:
		return fmt.Errorf("unsupport proc type %s", p.Type)
	}
	return nil
}
