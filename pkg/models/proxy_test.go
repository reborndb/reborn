// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/reborndb/go/bytesize"
	"github.com/reborndb/qdb/pkg/engine/goleveldb"
	"github.com/reborndb/qdb/pkg/service"
	"github.com/reborndb/qdb/pkg/store"

	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

var (
	productName = "unit_test"
	auth        = ""
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testModelSuite{})

type testServer struct {
	addr   string
	store  *store.Store
	server *service.Server
}

func (s *testServer) Close() {
	if s.server != nil {
		s.server.Close()
	}
}

type testModelSuite struct {
	s1 *testServer
	s2 *testServer
}

func (s *testModelSuite) SetUpSuite(c *C) {
	s.s1 = s.testCreateServer(c, 26380)
	c.Assert(s.s1, NotNil)

	s.s2 = s.testCreateServer(c, 26381)
	c.Assert(s.s2, NotNil)
}

func (s *testModelSuite) TearDownSuite(c *C) {
	if s.s1 != nil {
		s.s1.Close()
	}

	if s.s2 != nil {
		s.s2.Close()
	}
}

func (s *testModelSuite) testCreateServer(c *C, port int) *testServer {
	base := fmt.Sprintf("/tmp/test_reborn/test_proxy_models/%d", port)
	err := os.RemoveAll(base)
	c.Assert(err, IsNil)

	err = os.MkdirAll(base, 0700)
	c.Assert(err, IsNil)

	conf := goleveldb.NewDefaultConfig()
	testdb, err := goleveldb.Open(path.Join(base, "db"), conf, false)
	c.Assert(err, IsNil)

	cfg := service.NewDefaultConfig()
	cfg.Listen = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.PidFile = fmt.Sprintf(base, "qdb.pid")
	cfg.DumpPath = path.Join(base, "rdb.dump")
	cfg.SyncFilePath = path.Join(base, "sync.pipe")
	cfg.ReplBacklogSize = bytesize.MB

	store := store.New(testdb)
	server, err := service.NewServer(cfg, store)
	c.Assert(err, IsNil)
	go server.Serve()

	ss := new(testServer)
	ss.addr = cfg.Listen
	ss.store = store
	ss.server = server

	return ss
}

func (s *testModelSuite) TestProxy(c *C) {
	log.Info("[TestProxy][start]")
	fakeCoordConn := zkhelper.NewConn()

	path := GetSlotBasePath(productName)
	children, _, _ := fakeCoordConn.Children(path)
	c.Assert(len(children), Equals, 0)

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	// test create new group
	_, err := ServerGroups(fakeCoordConn, productName)
	c.Assert(err, IsNil)

	ok, err := g.Exists(fakeCoordConn)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)

	s1 := NewServer(SERVER_TYPE_MASTER, "localhost:1111")

	g.AddServer(fakeCoordConn, s1, auth)

	err = InitSlotSet(fakeCoordConn, productName, 1024)
	c.Assert(err, IsNil)

	children, _, err = fakeCoordConn.Children(path)
	c.Assert(err, IsNil)
	c.Assert(len(children), Equals, 1024)

	sl, err := GetSlot(fakeCoordConn, productName, 1)
	c.Assert(err, IsNil)
	c.Assert(sl.GroupId, Equals, -1)

	err = SetSlotRange(fakeCoordConn, productName, 0, 1023, 1, SLOT_STATUS_ONLINE)
	c.Assert(err, IsNil)

	pi := &ProxyInfo{
		ID:    "proxy_1",
		Addr:  "localhost:1234",
		State: PROXY_STATE_OFFLINE,
	}

	_, err = CreateProxyInfo(fakeCoordConn, productName, pi)
	c.Assert(err, IsNil)

	ps, err := ProxyList(fakeCoordConn, productName, nil)
	c.Assert(err, IsNil)
	c.Assert(len(ps), Equals, 1)
	c.Assert(ps[0].ID, Equals, "proxy_1")

	err = SetProxyStatus(fakeCoordConn, productName, pi.ID, PROXY_STATE_ONLINE)
	c.Assert(err, IsNil)

	p, err := GetProxyInfo(fakeCoordConn, productName, pi.ID)
	c.Assert(err, IsNil)
	c.Assert(p.State, Equals, PROXY_STATE_ONLINE)

	fakeCoordConn.Close()
	log.Info("[TestProxy][end]")
}
