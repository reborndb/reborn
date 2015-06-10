// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

func (s *testModelSuite) TestAddSlaveToEmptyGroup(c *C) {
	log.Info("[TestAddSlaveToEmptyGroup][start]")
	fakeCoordConn := zkhelper.NewConn()

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	s1 := NewServer(SERVER_TYPE_SLAVE, s.s1.addr)
	err := g.AddServer(fakeCoordConn, s1)
	c.Assert(err, IsNil)
	c.Assert(g.Servers[0].Type, Equals, SERVER_TYPE_MASTER)

	fakeCoordConn.Close()
	log.Info("[TestAddSlaveToEmptyGroup][end]")
}

func (s *testModelSuite) TestServerGroup(c *C) {
	log.Info("[TestServerGroup][start]")
	fakeCoordConn := zkhelper.NewConn()

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	// test create new group
	groups, err := ServerGroups(fakeCoordConn, productName)
	c.Assert(err, IsNil)
	c.Assert(len(groups), Not(Equals), 0)

	ok, err := g.Exists(fakeCoordConn)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)

	gg, err := GetGroup(fakeCoordConn, productName, 1)
	c.Assert(err, IsNil)
	c.Assert(gg, NotNil)
	c.Assert(gg.Id, Equals, g.Id)

	s1 := NewServer(SERVER_TYPE_MASTER, s.s1.addr)
	s2 := NewServer(SERVER_TYPE_MASTER, s.s2.addr)

	err = g.AddServer(fakeCoordConn, s1)
	c.Assert(err, IsNil)

	servers, err := g.GetServers(fakeCoordConn)
	c.Assert(err, IsNil)
	c.Assert(len(servers), Equals, 1)

	g.AddServer(fakeCoordConn, s2)
	c.Assert(len(g.Servers), Equals, 1)

	s2.Type = SERVER_TYPE_SLAVE
	g.AddServer(fakeCoordConn, s2)
	c.Assert(len(g.Servers), Equals, 2)

	err = g.Promote(fakeCoordConn, s2.Addr)
	c.Assert(err, IsNil)

	m, err := g.Master(fakeCoordConn)
	c.Assert(err, IsNil)
	c.Assert(m.Addr, Equals, s2.Addr)

	fakeCoordConn.Close()
	log.Info("[TestServerGroup][stop]")
}
