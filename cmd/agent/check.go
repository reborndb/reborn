// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
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
		switch strings.ToLower(p.Type) {
		case proxyType:
			// for proxy type, we will use a new id to avoid zk node exists error
			args := new(proxyArgs)
			map2Args(args, p.Ctx)

			// clear old log and data
			p.clear()

			startProxy(args)
		default:
			if err := p.start(); err != nil {
				log.Errorf("restart %s err %v", p.Cmd, err)
			} else {
				addCheckProc(p)
			}
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
	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return errors.Trace(err)
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		baseName := path.Base(f.Name())
		var tp string
		var id string
		if seps := strings.Split(baseName, "_"); len(seps) != 2 {
			continue
		} else {
			tp, id = seps[0], seps[1]
		}

		datFile := path.Join(dataDir, baseName, fmt.Sprintf("%s.dat", tp))

		if p, err := loadProcess(datFile); err != nil {
			log.Warningf("load process data %s err %v, skip", dataDir, err)
			continue
		} else if p == nil {
			log.Infof("proc %s has no need to be reload, skip", id)
			continue
		} else {
			if id != p.ID {
				log.Warningf("we need proc %s, but got %s", id, p.ID)
				continue
			}
			// todo bind after start func for different type
			if err := bindProcHandler(p); err != nil {
				log.Errorf("bind proc %s err %v, skip", p.Cmd, err)
				continue
			}
			addCheckProc(p)
		}
	}

	//todo remove unnecessary old logs
	clearOldProcsFiles(dataDir)
	clearOldProcsFiles(logDir)

	return nil
}

func clearOldProcsFiles(baseDir string) {
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Errorf("read %s err %v", logDir, err)
		return
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		baseName := path.Base(f.Name())
		var id string
		if seps := strings.Split(baseName, "_"); len(seps) != 2 {
			continue
		} else {
			id = seps[1]
		}

		if _, ok := procs[id]; !ok {
			// we need remove unnecessary files
			os.RemoveAll(path.Join(baseDir, baseName))
		}
	}
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
