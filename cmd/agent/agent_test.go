// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"math/rand"
	"path"
	"testing"

	"github.com/ngaut/zkhelper"

	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testAgentSuite struct {
}

var _ = Suite(&testAgentSuite{})

type testEnv struct {
}

func (env *testEnv) ProductName() string {
	return "test_agent"
}

func (env *testEnv) DashboardAddr() string {
	return "localhost:18087"
}

func (env *testEnv) StoreAuth() string {
	return ""
}

func (env *testEnv) NewCoordConn() (zkhelper.Conn, error) {
	return zkhelper.ConnectToZk("localhost:2181")
}

func (s *testAgentSuite) SetUpSuite(c *C) {
	globalEnv = new(testEnv)
	var err error
	globalConn, err = globalEnv.NewCoordConn()
	c.Assert(err, IsNil)
}

func (s *testAgentSuite) TearDownSuite(c *C) {
	if globalConn != nil {
		globalConn.Close()
	}
}

func (s *testAgentSuite) testAddAgent(c *C, id string, addr string) {
	agent := &agentInfo{
		ID:   id,
		Addr: addr,
		PID:  rand.Int(),
	}

	err := addAgent(agent)
	c.Assert(err, IsNil)
}

func (s *testAgentSuite) TestAgentModel(c *C) {
	s.testAddAgent(c, "1", "127.0.0.1:11000")
	s.testAddAgent(c, "2", "127.0.0.1:12000")

	agents, err := getAgents()
	c.Assert(err, IsNil)
	c.Assert(agents, HasLen, 2)
	c.Assert(agents[0].ID, Equals, "1")
	c.Assert(agents[1].ID, Equals, "2")

	// remove all agents
	for _, agent := range agents {
		err = globalConn.Delete(path.Join(agentPath(), agent.ID), -1)
		c.Assert(err, IsNil)
	}

	agents, err = getAgents()
	c.Assert(err, IsNil)
	c.Assert(agents, HasLen, 0)
}
