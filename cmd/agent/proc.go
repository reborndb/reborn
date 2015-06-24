// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/reborndb/go/io/ioutils"

	"github.com/juju/errors"
	"github.com/mitchellh/go-ps"
	"github.com/ngaut/log"
	"github.com/nu7hatch/gouuid"
)

func genProcID() string {
	u, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("gen uuid err: %v", err)
	}

	return strings.ToLower(hex.EncodeToString(u[0:16]))
}

type process struct {
	// uuid for a process in agent use
	ID string `json:"id"`

	// process type, like proxy, redis, qdb, dashboard
	Type string `json:"type"`

	// Current pid, every process will save it in its own pid file
	// so we don't save it in data file.
	Pid int `json:"-"`

	// for start process, use cmd and args
	Cmd  string   `json:"name"`
	Args []string `json:"args"`

	// special use for different processes
	Ctx map[string]string `json:"ctx"`

	postStartFunc func(p *process) error

	// if not nil, we will use this func to stop process
	stopFunc func(p *process) error

	// some procs like redis support daemonize directly,
	// so we don't use reborn-daemon to start these procs
	Daemonize bool `json:"daemonize"`
}

func (p *process) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[process](%+v)", *p)
}

func newDefaultProcess(cmd string, tp string) *process {
	id := genProcID()
	p := new(process)

	p.ID = id
	p.Cmd = cmd
	p.Type = tp
	p.Ctx = make(map[string]string)
	p.Daemonize = false

	return p
}

func loadProcess(dataPath string) (*process, error) {
	p := new(process)

	data, err := ioutil.ReadFile(dataPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = json.Unmarshal(data, &p); err != nil {
		return nil, errors.Trace(err)
	}

	if !isFileExist(p.pidPath()) {
		// pid file is not exists, we should not handle this id anymore
		os.Remove(dataPath)
		log.Infof("pid file %s is not exist, skip", p.pidPath())
		return nil, nil
	}

	if p.Pid, err = p.readPid(); err != nil {
		return nil, errors.Trace(err)
	}

	return p, nil
}

func (p *process) readPid() (int, error) {
	data, err := ioutil.ReadFile(p.pidPath())
	if err != nil {
		return 0, errors.Trace(err)
	}

	return strconv.Atoi(strings.TrimSpace(string(data)))
}

func (p *process) addCmdArgs(args ...string) {
	p.Args = append(p.Args, args...)
}

func (p *process) start() error {
	os.MkdirAll(p.procDataDir(), 0755)
	os.MkdirAll(p.procLogDir(), 0755)

	var c *exec.Cmd
	if p.Daemonize {
		c = exec.Command(p.Cmd, p.Args...)
	} else {
		args := append([]string{p.Cmd}, p.Args...)
		c = exec.Command("reborn-daemon", args...)
	}

	c.Stdout = os.Stdout
	c.Stderr = os.Stderr

	if err := c.Start(); err != nil {
		return errors.Trace(err)
	}

	go func() {
		// use another goroutine to wait process over
		// we don't handle anything here, because we will
		// check process alive in a checker totally.
		c.Wait()
	}()

	log.Infof("wait 1s to let %s start ok", p.Type)
	time.Sleep(time.Second)

	var err error
	for i := 0; i < 5; i++ {
		// we must read pid from pid file
		if p.Pid, err = p.readPid(); err != nil {
			log.Warningf("read pid failed, err %v, wait 1s and retry", err)
			err = errors.Trace(err)
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	if err != nil {
		return errors.Trace(err)
	}

	if b, err := p.checkAlive(); err != nil {
		return errors.Trace(err)
	} else if !b {
		return errors.Errorf("start %d (%s) but it's not alive", p.Pid, p.Type)
	}

	if p.postStartFunc != nil {
		if err := p.postStartFunc(p); err != nil {
			log.Errorf("post start %d (%s) err %v", p.Pid, p.Type, err)
			return errors.Trace(err)
		}
	}

	log.Infof("%s start ok now", p.Type)
	return errors.Trace(p.save())
}

func (p *process) save() error {
	// we only handle data file here, because process itself will handle pid file
	data, err := json.Marshal(p)
	if err != nil {
		return errors.Trace(err)
	}

	err = ioutils.WriteFileAtomic(p.dataPath(), data, 0644)
	return errors.Trace(err)
}

// agent path
// log: logDir/proc/type_id/xxx logDir/trash/xxx logDir/xxx.log
// data: dataDir/proc/type_id/xxx or dataDir/store/type_addr/xxx

func baseProcDataDir() string {
	return path.Join(dataDir, "proc")
}

func baseStoreDataDir() string {
	return path.Join(dataDir, "store")
}

func baseProcLogDir() string {
	return path.Join(logDir, "proc")
}

func baseTrashLogDir() string {
	return path.Join(logDir, "trash")
}

func (p *process) procDataDir() string {
	return path.Join(baseProcDataDir(), fmt.Sprintf("%s_%s", p.Type, p.ID))
}

func (p *process) storeDataDir(id string) string {
	return path.Join(baseStoreDataDir(), fmt.Sprintf("%s_%s", p.Type, id))
}

func (p *process) pidPath() string {
	return path.Join(p.procDataDir(), fmt.Sprintf("%s.pid", p.Type))
}

func (p *process) dataPath() string {
	return path.Join(p.procDataDir(), fmt.Sprintf("%s.dat", p.Type))
}

func (p *process) procLogDir() string {
	return path.Join(baseProcLogDir(), fmt.Sprintf("%s_%s", p.Type, p.ID))
}

func (p *process) checkAlive() (bool, error) {
	proc, err := ps.FindProcess(p.Pid)
	if err != nil {
		return false, errors.Trace(err)
	} else if proc == nil {
		// proc is not alive
		return false, nil
	} else {
		if strings.Contains(proc.Executable(), p.Cmd) {
			return true, nil
		} else {
			log.Warningf("pid %d exits, but exeutable name is %s, not %s", p.Pid, proc.Executable(), p.Cmd)
			return false, nil
		}
	}
}

func isFileExist(name string) bool {
	fi, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}

	if fi.IsDir() {
		return false
	}

	return true
}

func isDirExist(name string) bool {
	fi, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}

	if !fi.IsDir() {
		return false
	}

	return true
}

func (p *process) needRestart() bool {
	// if the process exited but the pid file exists,
	// we may think the process is closed unpredictably,
	// so we need restart it

	return isFileExist(p.pidPath())
}

func (p *process) clear() {
	p.clearData()
	p.clearLog()
}

func (p *process) clearData() {
	// remove base dir
	os.RemoveAll(p.procDataDir())
}

func (p *process) clearLog() {
	// backup log dir
	// 1 if log dir does not exist, there will be an error
	// 2 if newLogDir exists, there will be an error
	// all erroros we ignore

	baseName := path.Base(p.procLogDir())
	newName := fmt.Sprintf("%s_%d", baseName, time.Now().UnixNano())
	os.Rename(p.procLogDir(), path.Join(baseTrashLogDir(), newName))
}

func (p *process) stop() error {
	b, err := p.checkAlive()
	if err != nil {
		return errors.Trace(err)
	}

	defer p.clearLog()

	if !b {
		return nil
	} else {
		if proc, err := os.FindProcess(p.Pid); err != nil {
			return errors.Trace(err)
		} else {
			if p.stopFunc != nil {
				if err := p.stopFunc(p); err != nil {
					log.Errorf("stop %d (%s) err %v, send SIGTERM and Interrupt signal", p.Pid, p.Type, err)
					proc.Signal(syscall.SIGTERM)
					proc.Signal(os.Interrupt)
				}
			} else {
				proc.Signal(syscall.SIGTERM)
				proc.Signal(os.Interrupt)
			}

			ch := make(chan struct{}, 1)
			go func(ch chan struct{}) {
				proc.Wait()
				ch <- struct{}{}
			}(ch)

			select {
			case <-ch:
			case <-time.After(5 * time.Minute):
				proc.Kill()
				log.Errorf("wait %d (%s)stopped timeout, force kill", p.Pid, p.Type)
			}

			return nil
		}
	}
}
