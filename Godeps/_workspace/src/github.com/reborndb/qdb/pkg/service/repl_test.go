// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "github.com/reborndb/go/redis/resp"
	. "gopkg.in/check.v1"
)

var _ = Suite(&testReplSuite{})

type testReplSuite struct {
	redisExists bool

	srv1      *testReplSrvNode
	srv2      *testReplSrvNode
	redisNode *testReplRedisNode

	connPools map[int]*testConnPool
}

func (s *testReplSuite) SetUpSuite(c *C) {
	_, err := exec.LookPath("redis-server")
	s.redisExists = (err == nil)

	redisPort := 17777
	if s.redisExists {
		s.startRedis(c, redisPort)
	}

	s.redisNode = &testReplRedisNode{redisPort, s}

	svr1Port := 17778
	svr2Port := 17779

	s.srv1 = &testReplSrvNode{port: svr1Port, s: testCreateServer(c, svr1Port)}
	s.srv2 = &testReplSrvNode{port: svr2Port, s: testCreateServer(c, svr2Port)}

	s.connPools = make(map[int]*testConnPool, 3)
	s.connPools[redisPort] = testCreateConnPool(redisPort)
	s.connPools[svr1Port] = testCreateConnPool(svr1Port)
	s.connPools[svr2Port] = testCreateConnPool(svr2Port)
}

func (s *testReplSuite) TearDownSuite(c *C) {
	for _, p := range s.connPools {
		p.Close()
	}

	if s.redisExists {
		s.stopRedis(c, s.redisNode.Port())
	}

	if s.srv1.s != nil {
		s.srv1.s.Close()
	}

	if s.srv2.s != nil {
		s.srv2.s.Close()
	}
}

type redisChecker struct {
	sync.Mutex
	ok  bool
	buf bytes.Buffer
}

func (r *redisChecker) Write(data []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	r.buf.Write(data)
	if strings.Contains(r.buf.String(), "The server is now ready to accept connections") {
		r.ok = true
	}

	return len(data), nil
}

func (s *testReplSuite) startRedis(c *C, port int) {
	checker := &redisChecker{ok: false}
	// start redis and use memory only
	cmd := exec.Command("redis-server", "--port", fmt.Sprintf("%d", port), "--save", "")
	cmd.Stdout = checker
	cmd.Stderr = checker

	err := cmd.Start()
	c.Assert(err, IsNil)

	for i := 0; i < 20; i++ {
		var ok bool
		checker.Lock()
		ok = checker.ok
		checker.Unlock()

		if ok {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Fatal("redis-server can not start ok after 10s")
}

func (s *testReplSuite) stopRedis(c *C, port int) {
	cmd := exec.Command("redis-cli", "-p", fmt.Sprintf("%d", port), "shutdown", "nosave")
	cmd.Run()
}

func (s *testReplSuite) getConn(c *C, port int) *testPoolConn {
	p, ok := s.connPools[port]
	c.Assert(ok, Equals, true)
	return p.Get(c)
}

func (s *testReplSuite) doCmd(c *C, port int, cmd string, args ...interface{}) redis.Resp {
	nc := s.getConn(c, port)
	defer nc.Recycle()

	return nc.doCmd(c, cmd, args...)
}

func (s *testReplSuite) doCmdMustOK(c *C, port int, cmd string, args ...interface{}) {
	nc := s.getConn(c, port)
	defer nc.Recycle()

	nc.checkOK(c, cmd, args...)
}

func (s *testReplSuite) checkRole(c *C, port int, expect string) {
	r := s.doCmd(c, port, "ROLE")
	resp, ok := r.(*redis.Array)
	c.Assert(ok, Equals, true)
	c.Assert(resp.Value, Not(HasLen), 0)
	role, ok := resp.Value[0].(*redis.BulkBytes)
	c.Assert(ok, Equals, true)
	c.Assert(string(role.Value), Equals, expect)
}

type testReplConn interface {
	Close(c *C)
}

type testReplNode interface {
	Port() int
	Slaveof(c *C, port int) testReplConn
	SyncOffset(c *C) int64
}

type testReplRedisNode struct {
	port int
	s    *testReplSuite
}

func (n *testReplRedisNode) Port() int {
	return n.port
}

type testReplRedisConn struct {
	port int
	s    *testReplSuite
}

func (nc *testReplRedisConn) Close(c *C) {
	nc.s.doCmd(c, nc.port, "CLIENT", "KILL", "addr", fmt.Sprintf("127.0.0.1:%d", nc.port), "type", "slave")
}

func (n *testReplRedisNode) Slaveof(c *C, port int) testReplConn {
	n.s.doCmdMustOK(c, n.port, "SLAVEOF", "127.0.0.1", port)

	return &testReplRedisConn{n.port, n.s}
}

func (n *testReplRedisNode) SyncOffset(c *C) int64 {
	resp := n.s.doCmd(c, n.port, "ROLE")

	// we only care slave replication sync offset

	rsp, ok := resp.(*redis.Array)
	c.Assert(ok, Equals, true)
	c.Assert(rsp.Value, HasLen, 5)

	offset := rsp.Value[4]

	switch t := offset.(type) {
	case *redis.Int:
		return t.Value
	case *redis.BulkBytes:
		n, err := strconv.ParseInt(string(t.Value), 10, 64)
		c.Assert(err, IsNil)
		return n
	default:
		c.Fatalf("invalid resp type %T", offset)
	}

	c.Fatal("can enter here")
	return 0
}

type testReplSrvConn struct {
	nc *conn
}

func (nc *testReplSrvConn) Close(c *C) {
	nc.nc.Close()
}

type testReplSrvNode struct {
	port int
	s    *testServer
}

func (n *testReplSrvNode) Slaveof(c *C, port int) testReplConn {
	nc, err := n.s.h.replicationConnectMaster(fmt.Sprintf("127.0.0.1:%d", port))
	c.Assert(err, IsNil)
	n.s.h.master <- nc
	<-n.s.h.slaveofReply
	return &testReplSrvConn{nc}
}

func (n *testReplSrvNode) Port() int             { return n.port }
func (n *testReplSrvNode) SyncOffset(c *C) int64 { return n.s.h.syncOffset.Get() }

func (s *testReplSuite) waitAndCheckSyncOffset(c *C, node testReplNode, lastSyncOffset int64) {
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)

		if node.SyncOffset(c) > lastSyncOffset {
			break
		}
	}
}

func (s *testReplSuite) testReplication(c *C, master testReplNode, slave testReplNode) {
	// first let both stop replication
	s.doCmdMustOK(c, master.Port(), "SLAVEOF", "NO", "ONE")
	s.doCmdMustOK(c, slave.Port(), "SLAVEOF", "NO", "ONE")

	s.doCmdMustOK(c, master.Port(), "SET", "a", "100")

	offset := int64(-1)
	// slaveof, will do full sync first, must support psync
	nc := slave.Slaveof(c, master.Port())
	defer nc.Close(c)

	s.waitAndCheckSyncOffset(c, slave, offset)

	resp := s.doCmd(c, slave.Port(), "GET", "a")
	c.Assert(slave.SyncOffset(c), Not(Equals), int64(-1))
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

	s.doCmdMustOK(c, master.Port(), "SET", "b", "100")

	time.Sleep(500 * time.Millisecond)
	resp = s.doCmd(c, slave.Port(), "GET", "b")
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("100"))

	s.doCmdMustOK(c, master.Port(), "SET", "c", "")

	time.Sleep(500 * time.Millisecond)
	resp = s.doCmd(c, slave.Port(), "GET", "c")
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString(""))

	offset = slave.SyncOffset(c)
	// now close replication connection
	nc.Close(c)
	s.doCmdMustOK(c, master.Port(), "SET", "b", "1000")

	s.doCmdMustOK(c, master.Port(), "SET", "c", "123")

	s.waitAndCheckSyncOffset(c, slave, offset)

	resp = s.doCmd(c, slave.Port(), "GET", "b")
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("1000"))

	resp = s.doCmd(c, slave.Port(), "GET", "c")
	c.Assert(resp, DeepEquals, redis.NewBulkBytesWithString("123"))

	s.checkRole(c, master.Port(), "master")
	s.checkRole(c, slave.Port(), "slave")

	s.doCmdMustOK(c, slave.Port(), "SLAVEOF", "NO", "ONE")
	s.doCmdMustOK(c, master.Port(), "SLAVEOF", "NO", "ONE")

	s.checkRole(c, master.Port(), "master")
	s.checkRole(c, slave.Port(), "master")
}

func (s *testReplSuite) TestRedisMaster(c *C) {
	if !s.redisExists {
		c.Skip("no redis, skip")
	}
	// redis is master, and svr1 is slave
	s.testReplication(c, s.redisNode, s.srv1)
}

func (s *testReplSuite) TestRedisSlave(c *C) {
	if !s.redisExists {
		c.Skip("no redis, skip")
	}
	// redis is slave, and svr1 is master
	s.testReplication(c, s.srv1, s.redisNode)
}

func (s *testReplSuite) TestReplication(c *C) {
	// svr1 is master, svr2 is slave
	s.testReplication(c, s.srv1, s.srv2)
}
