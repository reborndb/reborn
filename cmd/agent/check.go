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

			// clear old datas
			p.clear()

			// remove from procs
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

		// get path type and values
		baseName := path.Base(f.Name())
		tp, v, ptp := getPathType(baseName)
		if ptp != idPathType {
			continue
		}

		// path format is type_id
		datFile := path.Join(dataDir, baseName, fmt.Sprintf("%s.dat", tp))
		if p, err := loadProcess(datFile); err != nil {
			log.Warningf("load process data %s err %v, skip", dataDir, err)
			continue
		} else if p == nil {
			log.Infof("proc %s has no need to be reload, skip", v)
			continue
		} else {
			if v != p.ID {
				log.Warningf("we need proc %s, but got %s", v, p.ID)
				continue
			}

			// TODO: bind after start func for different type
			if err := bindProcHandler(p); err != nil {
				log.Errorf("bind proc %s err %v, skip", p.Cmd, err)
				continue
			}
			addCheckProc(p)
		}
	}

	// remove unnecessary files
	clearOldProcsFiles(dataDir, dataType)
	clearOldProcsFiles(logDir, logType)

	return nil
}

func clearOldProcsFiles(baseDir string, tp string) {
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Errorf("read %s err %v", logDir, err)
		return
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		// get path type and values
		baseName := path.Base(f.Name())
		tp, v, ptp := getPathType(baseName)
		if ptp != idPathType {
			continue
		}

		// path format is type_id
		if _, ok := procs[v]; !ok {
			// we need remove unnecessary files
			os.RemoveAll(path.Join(baseDir, baseName))

			// if log type, then we just rename dir
			if tp == logType {
				newName := fmt.Sprintf("%s_%s_%d", tp, v, time.Now().Nanosecond())
				os.Rename(path.Join(baseDir, baseName), path.Join(baseDir, newName))
			}
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

	addrPathType  = "addr"
	idPathType    = "id"
	otherPathType = "other"

	logType  = "log"
	dataType = "data"
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

func getPathType(path string) (string, string, string) {
	s := strings.Split(path, "_")
	if len(s) != 2 {
		return "", "", otherPathType
	}

	ss := strings.Split(s[1], ":")
	if len(ss) == 2 {
		return s[0], s[1], addrPathType
	}

	return s[0], s[1], idPathType
}
