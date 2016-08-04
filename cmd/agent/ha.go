// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
	"github.com/reborndb/go/atomic2"
	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/utils"
)

// HA mechanism
// 1, use cooridnator to elect a leader
// 2, leader check all servers (using ping command) every 3 seconds
// 3, if a server is down, leader will ask other agents to help it ping this server
// 4, if all agents reply this server is down, we will be sure it is down.
// 5, if the server is slave, we will only mark it offline.
// 6, if the server is master, we will do failover.
//
// if any step fails, we will discard current check and do above again.

type haTask struct {
	quited atomic2.Int64

	wg sync.WaitGroup
}

func (t *haTask) Run() error {
	log.Infof("we are leader now, begin check")

	t.quited.Set(0)
	t.wg.Add(1)
	defer t.wg.Done()

	for {
		if t.quited.Get() == 1 {
			break
		}

		if err := t.check(); err != nil {
			log.Errorf("check err %v", errors.ErrorStack(err))
			return errors.Trace(err)
		}

		// check servers every second
		time.Sleep(1 * time.Second)
	}

	return nil
}

func (t *haTask) Stop() {
	t.quited.Set(1)

	t.wg.Wait()

	log.Infof("stop ha check")
}

func (t *haTask) Interrupted() bool {
	return false
}

func (t *haTask) check() error {
	groups, err := models.ServerGroups(globalConn, globalEnv.ProductName())
	if err != nil {
		return errors.Trace(err)
	}

	cnt := 0

	const checkChanSize = 100
	ch := make(chan interface{}, checkChanSize)

	// check all servers in all groups
	for _, group := range groups {
		for _, server := range group.Servers {
			cnt++

			go t.checkGroupServer(server, ch)
		}
	}

	var crashSlaves []*models.Server
	var crashMasters []*models.Server
	var rebirthOfflines []*models.Server
	for i := 0; i < cnt; i++ {
		v := <-ch
		switch s := v.(type) {
		case nil:
		case *models.Server:
			if s.Type == models.SERVER_TYPE_SLAVE {
				crashSlaves = append(crashSlaves, s)
			} else if s.Type == models.SERVER_TYPE_MASTER {
				crashMasters = append(crashMasters, s)
			} else if s.Type == models.SERVER_TYPE_OFFLINE {
				rebirthOfflines = append(rebirthOfflines, s)
			}
		case error:
			err = errors.Trace(s)
		}
	}

	if err != nil {
		return errors.Trace(err)
	}

	for _, s := range rebirthOfflines {
		log.Infof("offline %s in group %d is up, set slave", s.Addr, s.GroupId)
		group := models.NewServerGroup(globalEnv.ProductName(), s.GroupId)

		s.Type = models.SERVER_TYPE_SLAVE

		if err := group.AddServer(globalConn, s, globalEnv.StoreAuth()); err != nil {
			return errors.Trace(err)
		}
	}

	for _, s := range crashSlaves {
		log.Infof("slave %s in group %d is down, set offline", s.Addr, s.GroupId)
		group := models.NewServerGroup(globalEnv.ProductName(), s.GroupId)

		s.Type = models.SERVER_TYPE_OFFLINE

		if err := group.AddServer(globalConn, s, globalEnv.StoreAuth()); err != nil {
			return errors.Trace(err)
		}
	}

	for _, s := range crashMasters {
		log.Infof("master %s in group %d is down, do failover", s.Addr, s.GroupId)

		if err := t.doFailover(s); err != nil {
			log.Errorf("master %s in group %d is down, do failover err: %v", s.Addr, s.GroupId, err)
		}
	}

	return nil
}

func checkOfflineGroupServer(s *models.Server, ch chan<- interface{}) {
	if s.Type != models.SERVER_TYPE_OFFLINE {
		ch <- nil
		return
	}

	var err error
	for i := 0; i < haMaxRetryNum; i++ {
		if err = utils.Ping(s.Addr, globalEnv.StoreAuth()); err == nil {
			break
		}

		err = errors.Trace(err)
		time.Sleep(time.Duration(haRetryDelay) * time.Second)
	}

	if err != nil {
		ch <- nil
		return
	}

	// here means we can ping offline server ok, so we think it is up
	// let other help use to check
	log.Infof("leader check server %s in group %d err %v, let other agents help check", s.Addr, s.GroupId, err)

	// get all agents
	agents, err := getAgents()
	if err != nil {
		log.Errorf("get agents err %v", err)
		ch <- errors.Trace(err)
		return
	}

	reply := make([]interface{}, len(agents))

	var wg sync.WaitGroup
	for i, agent := range agents {
		if agent.ID == agentID {
			// ignore itself
			reply[i] = nil
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			resp, err := http.Get(fmt.Sprintf("http://%s/api/check_store?addr=%s", agent.Addr, s.Addr))
			if err != nil {
				reply[i] = errors.Trace(err)
				return
			}
			defer resp.Body.Close()
			if _, err = ioutil.ReadAll(resp.Body); err != nil {
				reply[i] = errors.Trace(err)
				return
			}

			reply[i] = int(resp.StatusCode)
		}(i)
	}

	wg.Wait()

	for i, r := range reply {
		switch v := r.(type) {
		case nil:
			// itself, ignore
		case error:
			log.Errorf("let agent %s check %s err %v", agents[i].Addr, s.Addr, v)
			err = errors.Trace(v)
		case int:
			if v != http.StatusOK {
				log.Infof("agent %s check %s not ok, maybe it is not alive", agents[i].Addr, s.Addr)
				ch <- nil
				return
			}
			// here mean agent check server failed
		}
	}

	if err != nil {
		// here mean let some agent check err, maybe we cann't connect the agent
		// so return error to retry again
		ch <- errors.Trace(err)
		return
	}

	// if all nodes check the store server is up, we will think it is up
	log.Infof("all agents check server %s is up", s.Addr)
	ch <- s
}

func (t *haTask) checkGroupServer(s *models.Server, ch chan<- interface{}) {
	// we don't check offline server
	if s.Type == models.SERVER_TYPE_OFFLINE {
		checkOfflineGroupServer(s, ch)
		return
	}

	var err error
	for i := 0; i < haMaxRetryNum; i++ {
		if err = utils.Ping(s.Addr, globalEnv.StoreAuth()); err == nil {
			break
		}

		err = errors.Trace(err)
		time.Sleep(time.Duration(haRetryDelay) * time.Second)
	}

	if err == nil {
		ch <- nil
		return
	}

	// here means we cannot ping server ok, so we think it is down
	// let other help use to check
	log.Infof("leader check server %s in group %d err %v, let other agents help check", s.Addr, s.GroupId, err)

	// get all agents
	agents, err := getAgents()
	if err != nil {
		log.Errorf("get agents err %v", err)
		ch <- errors.Trace(err)
		return
	}

	reply := make([]interface{}, len(agents))

	var wg sync.WaitGroup
	for i, agent := range agents {
		if agent.ID == agentID {
			// ignore itself
			reply[i] = nil
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			resp, err := http.Get(fmt.Sprintf("http://%s/api/check_store?addr=%s", agent.Addr, s.Addr))
			if err != nil {
				reply[i] = errors.Trace(err)
				return
			}
			defer resp.Body.Close()
			if _, err = ioutil.ReadAll(resp.Body); err != nil {
				reply[i] = errors.Trace(err)
				return
			}

			reply[i] = int(resp.StatusCode)
		}(i)
	}

	wg.Wait()

	for i, r := range reply {
		switch v := r.(type) {
		case nil:
			// itself, ignore
		case error:
			log.Errorf("let agent %s check %s err %v", agents[i].Addr, s.Addr, v)
			err = errors.Trace(v)
		case int:
			if v == http.StatusOK {
				log.Infof("agent %s check %s ok, maybe it is alive", agents[i].Addr, s.Addr)
				ch <- nil
				return
			}
			// here mean agent check server failed
		}
	}

	if err != nil {
		// here mean let some agent check err, maybe we cann't connect the agent
		// so return error to retry again
		ch <- errors.Trace(err)
		return
	}

	// if all nodes check the store server is down, we will think it is down
	log.Infof("all agents check server %s is down", s.Addr)
	ch <- s
}

func (t *haTask) doFailover(s *models.Server) error {
	// first get all slaves
	group := models.NewServerGroup(globalEnv.ProductName(), s.GroupId)

	var err error
	group.Servers, err = group.GetServers(globalConn)
	if err != nil {
		return errors.Trace(err)
	}

	slaves := make([]*models.Server, 0, len(group.Servers))
	slaveAddrs := make([]string, 0, len(group.Servers))

	for _, s := range group.Servers {
		if s.Type == models.SERVER_TYPE_SLAVE {
			slaves = append(slaves, s)
			slaveAddrs = append(slaveAddrs, s.Addr)
		}
	}

	// elect a new master
	log.Infof("elect a new master in %v", slaveAddrs)
	addr, err := t.electNewMaster(slaves)
	if err != nil {
		return errors.Trace(err)
	}

	// prmote it as new master
	log.Infof("promote %s as the new master", addr)
	if err := group.Promote(globalConn, addr, globalEnv.StoreAuth()); err != nil {
		// should we fatal here and let human intervention ???
		return errors.Trace(err)
	}

	// let other slaves replicate from the new master
	for _, slave := range slaves {
		if slave.Addr == addr {
			continue
		}

		log.Infof("let %s slaveof new master %s", slave.Addr, addr)
		if err := utils.SlaveOf(slave.Addr, addr, globalEnv.StoreAuth()); err != nil {
			// should we fatal here and let human intervention ???
			return errors.Trace(err)
		}
	}

	return nil
}

func (t *haTask) getReplicationInfo(s *models.Server) (map[string]string, error) {
	v, err := utils.GetRedisInfo(s.Addr, "REPLICATION", globalEnv.StoreAuth())
	if err != nil {
		return nil, errors.Trace(err)
	}

	seps := strings.Split(v, "\r\n")
	// skip first line, is # Replication
	seps = seps[1:]

	m := make(map[string]string, len(seps))
	for _, s := range seps {
		kv := strings.SplitN(s, ":", 2)
		if len(kv) == 2 {
			m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return m, nil
}

func (t *haTask) electNewMaster(slaves []*models.Server) (string, error) {
	var addr string
	var checkOffset int64 = 0
	var checkPriority int = 0

	for _, slave := range slaves {
		m, err := t.getReplicationInfo(slave)
		if err != nil {
			return "", errors.Errorf("slave %s get replication info err %v", slave.Addr, err)
		}

		if m["slave"] == "master" {
			return "", errors.Errorf("server %s is not slave now,", slave.Addr)
		}

		if m["master_link_status"] == "up" {
			return "", errors.Errorf("slave %s master_link_status is up, master may be alive", slave.Addr)
		}

		priority, _ := strconv.Atoi(m["slave_priority"])
		replOffset, _ := strconv.ParseInt(m["slave_repl_offset"], 10, 64)

		used := false
		// like redis-sentinel, first check priority, then salve repl offset
		if checkPriority < priority {
			used = true
		} else if checkPriority == priority {
			if checkOffset < replOffset {
				used = true
			}
		}

		if used {
			addr = slave.Addr
			checkPriority = priority
			checkOffset = replOffset
		}
	}

	if len(addr) == 0 {
		return "", errors.Errorf("no proper candidate to be promoted")
	}

	log.Infof("select slave %s as new master, priority:%d, repl_offset:%d", addr, checkPriority, checkOffset)

	return addr, nil
}

func startHA() {
	elector := zkhelper.CreateElection(globalConn, fmt.Sprintf("/zk/reborn/db_%s/ha", globalEnv.ProductName()))
	task := &haTask{}

	err := elector.RunTask(task)
	if err != nil {
		log.Errorf("run elector task err: %v", err)
	}
}
