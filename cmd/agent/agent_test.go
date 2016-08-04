// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/zkhelper"
	. "github.com/reborndb/go/gocheck2"
	"github.com/reborndb/reborn/pkg/env"
	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/utils"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testAgentInfo struct {
	cmd  *exec.Cmd
	addr string
}

func (a testAgentInfo) apiURL(action string, query string) string {
	if len(query) > 0 {
		return fmt.Sprintf("http://%s/api/%s?%s", a.addr, action, query)
	} else {
		return fmt.Sprintf("http://%s/api/%s", a.addr, action)
	}
}

func (a testAgentInfo) httpCall(c *C, objPrt interface{}, action string, query string, method string) {
	err := httpCall(objPrt, a.apiURL(action, query), method, nil)
	c.Assert(err, IsNil)
}

type testAgentSuite struct {
	agentDashboard   testAgentInfo
	agentProxy       testAgentInfo
	agentStoreMaster testAgentInfo
	agentStoreSlave  testAgentInfo
	agentHA          testAgentInfo
}

var _ = Suite(&testAgentSuite{})

func (s *testAgentSuite) SetUpSuite(c *C) {
	// initial whole test environment
	configFile = "config.ini"
	resetAbsPath(&configFile)
	cfg, err := utils.InitConfigFromFile(configFile)
	c.Assert(err, IsNil)

	globalEnv = env.LoadRebornEnv(cfg)
	globalConn, err = globalEnv.NewCoordConn()
	c.Assert(err, IsNil)

	s.testSetExecPath(c)

	// remove all infos in coordinator first
	err = zkhelper.DeleteRecursive(globalConn, fmt.Sprintf("/zk/reborn/db_%s", globalEnv.ProductName()), -1)
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		c.Assert(err, IsNil)
	}

	s.agentDashboard = s.testStartAgent(c, "127.0.0.1:39001", false)
	s.agentProxy = s.testStartAgent(c, "127.0.0.1:39002", false)
	s.agentStoreMaster = s.testStartAgent(c, "127.0.0.1:39003", false)
	s.agentStoreSlave = s.testStartAgent(c, "127.0.0.1:39004", false)

	s.testDashboard(c)

	s.testInitGroup(c)
	s.testInitSlots(c)
	s.testStoreAddServer(c)
}

func (s *testAgentSuite) TearDownSuite(c *C) {
	if globalConn != nil {
		globalConn.Close()
	}

	stopAgent := func(agent testAgentInfo) {
		s.testStopAllProcs(c, agent)
		s.testStopAgent(c, agent)
	}

	stopAgent(s.agentProxy)
	stopAgent(s.agentStoreSlave)
	stopAgent(s.agentStoreMaster)
	stopAgent(s.agentDashboard)
}

func (s *testAgentSuite) testSetExecPath(c *C) {
	// the executer is in ../../bin path, we will add this path in global $PATH
	execPath := "../../bin"
	resetAbsPath(&execPath)
	c.Assert(strings.HasSuffix(execPath, "/src/github.com/reborndb/reborn/bin"), IsTrue)

	os.Setenv("PATH", os.ExpandEnv(fmt.Sprintf("${PATH}:%s", execPath)))

	_, err := exec.LookPath("reborn-config")
	c.Assert(err, IsNil)

	_, err = exec.LookPath("reborn-proxy")
	c.Assert(err, IsNil)

	_, err = exec.LookPath("reborn-server")
	c.Assert(err, IsNil)

	_, err = exec.LookPath("reborn-agent")
	c.Assert(err, IsNil)
}

func (s *testAgentSuite) testStartAgent(c *C, addr string, ha bool) testAgentInfo {
	dataDir := fmt.Sprintf("./var/%s/data", addr)
	logDir := fmt.Sprintf("./var/%s/log", addr)

	args := []string{
		"--http-addr", addr, "--data-dir", dataDir, "--log-dir",
		logDir, "-L", path.Join(logDir, "agent.log")}

	if ha {
		args = append(args, "--ha", "--ha-max-retry-num", "1", "--ha-retry-delay", "1")
	}

	cmd := exec.Command("reborn-agent", args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	c.Assert(err, IsNil)

	var info testAgentInfo
	info.cmd = cmd
	info.addr = addr

	time.Sleep(time.Second)

	return info
}

func (s *testAgentSuite) testStopAgent(c *C, agent testAgentInfo) {
	c.Assert(agent.cmd, NotNil)
	agent.cmd.Process.Signal(os.Interrupt)
	agent.cmd.Wait()
}

func (s *testAgentSuite) testStopAllProcs(c *C, agent testAgentInfo) {
	procs := s.testGetProcs(c, agent)

	for _, proc := range procs {
		agent.httpCall(c, nil, "stop", fmt.Sprintf("id=%s", proc.ID), "POST")
	}
}

func (s *testAgentSuite) testKillAllProcs(c *C, agent testAgentInfo) {
	procs := s.testGetProcs(c, agent)
	s.testKillProcs(c, procs)
}

func (s *testAgentSuite) testKillProcs(c *C, procs []procStatus) {
	for _, proc := range procs {
		p, err := os.FindProcess(proc.Pid)
		c.Assert(err, IsNil)
		p.Kill()
		p.Wait()
	}
}

func (s *testAgentSuite) testGetProcs(c *C, agent testAgentInfo) []procStatus {
	var procs []procStatus
	agent.httpCall(c, &procs, "procs", "", "GET")
	return procs
}

func (s *testAgentSuite) callDashboardAPI(c *C, retVal interface{}, apiPath string, method string, arg interface{}) {
	err := httpCall(retVal, fmt.Sprintf("http://%s%s", globalEnv.DashboardAddr(), apiPath), method, arg)
	c.Assert(err, IsNil)
}

func (s *testAgentSuite) testDashboard(c *C) {
	agent := s.agentDashboard

	agent.httpCall(c, nil, "start_dashboard", "", "POST")

	time.Sleep(2 * time.Second)
	s.callDashboardAPI(c, nil, "/ping", "GET", nil)

	// if we kill dashboard, the restart one will wait a coorindator session timeout time
	// for old session expired, this will be a long time, so we don't test this case here.
}

func (s *testAgentSuite) testStore(c *C, agent testAgentInfo, port int) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	agent.httpCall(c, nil, "start_redis", fmt.Sprintf("addr=%s", url.QueryEscape(addr)), "POST")

	err := utils.Ping(addr, globalEnv.StoreAuth())
	c.Assert(err, IsNil)

	// kill store and then wait 2s for restart
	s.testKillAllProcs(c, agent)

	time.Sleep(2 * time.Second)

	err = utils.Ping(addr, globalEnv.StoreAuth())
	c.Assert(err, IsNil)
}

func (s *testAgentSuite) testStoreAddServer(c *C) {
	s.testStore(c, s.agentStoreMaster, 6381)
	s.testStore(c, s.agentStoreSlave, 6382)

	s.testAddStoreToGroup(c, 6381, models.SERVER_TYPE_MASTER)
	s.testAddStoreToGroup(c, 6382, models.SERVER_TYPE_SLAVE)

	s.checkStoreServerType(c, "127.0.0.1:6381", models.SERVER_TYPE_MASTER)
	s.checkStoreServerType(c, "127.0.0.1:6382", models.SERVER_TYPE_SLAVE)
}

func (s *testAgentSuite) testInitGroup(c *C) {
	serverGroup := models.NewServerGroup(globalEnv.ProductName(), 1)

	s.callDashboardAPI(c, nil, "/api/server_groups", "PUT", serverGroup)
}

func (s *testAgentSuite) testAddStoreToGroup(c *C, port int, role string) {
	server := models.NewServer(role, fmt.Sprintf("127.0.0.1:%d", port))
	s.callDashboardAPI(c, nil, fmt.Sprintf("/api/server_group/%d/addServer", 1), "PUT", server)
}

func (s *testAgentSuite) testInitSlots(c *C) {
	s.callDashboardAPI(c, nil, "/api/slots/init", "POST", nil)

	v := struct {
		FromSlot   int    `json:"from"`
		ToSlot     int    `json:"to"`
		NewGroupID int    `json:"new_group"`
		Status     string `json:"status"`
	}{
		FromSlot:   0,
		ToSlot:     1023,
		NewGroupID: 1,
		Status:     "online",
	}

	s.callDashboardAPI(c, nil, "/api/slot", "POST", v)
}

func (s *testAgentSuite) testProxy(c *C) {
	agent := s.agentProxy
	proxyAddr := "127.0.0.1:19000"
	proxyHTTPAddr := "127.0.0.1:29000"

	args := make(url.Values)
	args.Set("addr", proxyAddr)
	args.Set("http_addr", proxyHTTPAddr)

	agent.httpCall(c, nil, "start_proxy", args.Encode(), "POST")

	// now the proxy will wait 3s for online, this is very long for test
	// maybe later we will change it.
	var err error
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		if err = utils.Ping(proxyAddr, ""); err == nil {
			break
		}
	}
	c.Assert(err, IsNil)

	// kill proxy and then wait for restart
	s.testKillAllProcs(c, agent)

	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		if err = utils.Ping(proxyAddr, ""); err == nil {
			break
		}
	}
	c.Assert(err, IsNil)
}

func (s *testAgentSuite) TestProxy(c *C) {
	s.testProxy(c)
}

func (s *testAgentSuite) checkStoreServerType(c *C, addr string, tp string) {
	group := models.NewServerGroup(globalEnv.ProductName(), 1)
	servers, err := group.GetServers(globalConn)
	c.Assert(err, IsNil)

	for _, server := range servers {
		if server.Addr == addr {
			c.Assert(server.Type, Equals, tp)
			return
		}
	}
	c.Fatalf("addr %s is not in group servers %v", addr, servers)
}

func (s *testAgentSuite) TestHA(c *C) {
	s.agentHA = s.testStartAgent(c, "127.0.0.1:39005", true)
	// defer s.testStopAgent(c, s.agentHA)

	time.Sleep(3 * time.Second)

	// test slave ha
	procs := s.testGetProcs(c, s.agentStoreSlave)

	s.testStopAgent(c, s.agentStoreSlave)
	s.testKillProcs(c, procs)

	time.Sleep(3 * time.Second)

	s.checkStoreServerType(c, "127.0.0.1:6382", models.SERVER_TYPE_OFFLINE)

	// test offline rebirth, stop HA check first
	s.testStopAgent(c, s.agentHA)

	s.agentStoreSlave = s.testStartAgent(c, "127.0.0.1:39004", false)
	time.Sleep(3 * time.Second)

	// now agentStoreSlave restart and redis restart
	role, err := utils.GetRole("127.0.0.1:6382", globalEnv.StoreAuth())
	c.Assert(err, IsNil)
	c.Assert(role, Equals, "master")

	// start HA check
	s.agentHA = s.testStartAgent(c, "127.0.0.1:39005", true)
	time.Sleep(3 * time.Second)

	// when HA is working, offline server is up, automatic to slave
	s.checkStoreServerType(c, "127.0.0.1:6382", models.SERVER_TYPE_SLAVE)

	role, err = utils.GetRole("127.0.0.1:6382", globalEnv.StoreAuth())
	c.Assert(err, IsNil)
	c.Assert(role, Equals, "slave")

	// test master ha
	procs = s.testGetProcs(c, s.agentStoreMaster)

	s.testStopAgent(c, s.agentStoreMaster)
	s.testKillProcs(c, procs)

	time.Sleep(3 * time.Second)

	// now 6382 is slave, and 6381 is offline
	s.checkStoreServerType(c, "127.0.0.1:6382", models.SERVER_TYPE_MASTER)

	role, err = utils.GetRole("127.0.0.1:6382", globalEnv.StoreAuth())
	c.Assert(err, IsNil)
	c.Assert(role, Equals, "master")

	s.checkStoreServerType(c, "127.0.0.1:6381", models.SERVER_TYPE_OFFLINE)

	s.testStopAgent(c, s.agentHA)

	s.agentStoreMaster = s.testStartAgent(c, "127.0.0.1:39003", false)

	time.Sleep(3 * time.Second)

	// now agentStoreMaster restart and redis restart
	role, err = utils.GetRole("127.0.0.1:6381", globalEnv.StoreAuth())
	c.Assert(err, IsNil)
	c.Assert(role, Equals, "master")

	s.agentHA = s.testStartAgent(c, "127.0.0.1:39005", true)
	defer s.testStopAgent(c, s.agentHA)
	time.Sleep(3 * time.Second)

	// when HA is working, offline server is up, automatic to slave
	s.checkStoreServerType(c, "127.0.0.1:6381", models.SERVER_TYPE_SLAVE)

	role, err = utils.GetRole("127.0.0.1:6381", globalEnv.StoreAuth())
	c.Assert(err, IsNil)
	c.Assert(role, Equals, "slave")
}
