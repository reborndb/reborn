// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/reborndb/go/bytesize"
	"github.com/reborndb/qdb/pkg/engine/goleveldb"
	"github.com/reborndb/qdb/pkg/service"
	"github.com/reborndb/qdb/pkg/store"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilsSuite{})

type testUtilsSuite struct {
	s *testServer

	auth string
}

type testServer struct {
	addr   string
	store  *store.Store
	server *service.Server
}

func (s *testServer) Close() {
	if s.server != nil {
		s.server.Close()
		s.server = nil
	}
}

func (s *testUtilsSuite) SetUpSuite(c *C) {
	s.auth = "abc"
	s.s = s.testCreateServer(c, 36380, s.auth)
}

func (s *testUtilsSuite) testCreateServer(c *C, port int, auth string) *testServer {
	base := fmt.Sprintf("/tmp/test_reborn/test_proxy_utils/%d", port)

	err := os.MkdirAll(base, 0700)
	c.Assert(err, IsNil)

	conf := goleveldb.NewDefaultConfig()
	testdb, err := goleveldb.Open(path.Join(base, "db"), conf, false)
	c.Assert(err, IsNil)

	cfg := service.NewDefaultConfig()
	cfg.Listen = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.PidFile = fmt.Sprintf(base, "qdb.pid")
	cfg.DumpPath = path.Join(base, "rdb.dump")
	cfg.SyncFilePath = path.Join(base, "sync.pipe")
	cfg.Auth = auth
	cfg.ReplBacklogSize = bytesize.MB

	store := store.New(testdb)
	server, err := service.NewServer(cfg, store)
	c.Assert(err, IsNil)
	go server.Serve()

	ss := new(testServer)
	ss.addr = cfg.Listen
	ss.store = store
	ss.server = server

	c.Assert(ss, NotNil)

	return ss
}

func (s *testUtilsSuite) TearDownSuite(c *C) {
	if s.s != nil {
		s.s.Close()
	}
}

func (s *testUtilsSuite) TestPing(c *C) {
	err := Ping(s.s.addr, s.auth)
	c.Assert(err, IsNil)
}

func (s *testUtilsSuite) TestGetInfo(c *C) {
	_, err := GetRedisInfo(s.s.addr, "", s.auth)
	c.Assert(err, IsNil)
}

func (s *testUtilsSuite) TestGetRole(c *C) {
	role, err := GetRole(s.s.addr, s.auth)
	c.Assert(err, IsNil)
	c.Assert(role, Equals, "master")
}
