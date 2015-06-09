// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"bufio"
	"net"
	"sync"
	"time"

	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

var (
	once sync.Once
	conn zkhelper.Conn
)

func (s *testModelSuite) runFakeRedisSrv(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			continue
		}

		go func(c net.Conn) {
			w := bufio.NewWriter(c)
			w.WriteString("+OK\r\n")
			w.Flush()
		}(c)
	}
}

// TODO
// Use qdb later
func (s *testModelSuite) resetEnv() {
	conn = zkhelper.NewConn()
	once.Do(func() {
		go s.runFakeRedisSrv("127.0.0.1:1111")
		go s.runFakeRedisSrv("127.0.0.1:2222")
		time.Sleep(1 * time.Second)
	})
}

func (s *testModelSuite) TestAddSlaveToEmptyGroup(c *C) {
	log.Info("[TestAddSlaveToEmptyGroup][start]")
	s.resetEnv()

	g := NewServerGroup(productName, 1)
	g.Create(conn)

	s1 := NewServer(SERVER_TYPE_SLAVE, "127.0.0.1:1111")
	err := g.AddServer(conn, s1)
	c.Assert(err, IsNil)
	c.Assert(g.Servers[0].Type, Equals, SERVER_TYPE_MASTER)

	log.Info("[TestAddSlaveToEmptyGroup][end]")
}

func (s *testModelSuite) TestServerGroup(c *C) {
	log.Info("[TestServerGroup][start]")
	s.resetEnv()

	g := NewServerGroup(productName, 1)
	g.Create(conn)

	// test create new group
	groups, err := ServerGroups(conn, productName)
	c.Assert(err, IsNil)
	c.Assert(len(groups), Not(Equals), 0)

	ok, err := g.Exists(conn)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)

	gg, err := GetGroup(conn, productName, 1)
	c.Assert(err, IsNil)
	c.Assert(gg, NotNil)
	c.Assert(gg.Id, Equals, g.Id)

	s1 := NewServer(SERVER_TYPE_MASTER, "127.0.0.1:1111")
	s2 := NewServer(SERVER_TYPE_MASTER, "127.0.0.1:2222")

	err = g.AddServer(conn, s1)
	c.Assert(err, IsNil)

	servers, err := g.GetServers(conn)
	c.Assert(err, IsNil)
	c.Assert(len(servers), Equals, 1)

	g.AddServer(conn, s2)
	c.Assert(len(g.Servers), Equals, 1)

	s2.Type = SERVER_TYPE_SLAVE
	g.AddServer(conn, s2)
	c.Assert(len(g.Servers), Equals, 2)

	err = g.Promote(conn, s2.Addr)
	c.Assert(err, IsNil)

	m, err := g.Master(conn)
	c.Assert(err, IsNil)
	c.Assert(m.Addr, Equals, s2.Addr)

	log.Info("[TestServerGroup][stop]")
}
