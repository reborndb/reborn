// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	jerrors "github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/go/bytesize"
	"github.com/reborndb/go/pools"
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/engine/rocksdb"
	"github.com/reborndb/qdb/pkg/store"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testServiceSuite{})

type testServiceSuite struct {
	s        *testServer
	conn     *testConn
	connPool *testConnPool

	slotPort     int
	slotServer   *testServer
	slotConnPool *testConnPool
}

func (s *testServiceSuite) SetUpSuite(c *C) {
	s.s = testCreateServer(c, 16380)
	c.Assert(s.s, NotNil)

	s.conn = testCreateConn(16380)
	c.Assert(s.conn, NotNil)

	s.connPool = testCreateConnPool(16380)
	c.Assert(s.connPool, NotNil)

	s.slotPort = 16381
	s.slotServer = testCreateServer(c, s.slotPort)
	c.Assert(s.slotServer, NotNil)

	s.slotConnPool = testCreateConnPool(s.slotPort)
	c.Assert(s.slotConnPool, NotNil)
}

func (s *testServiceSuite) TearDownSuite(c *C) {
	if s.conn != nil {
		s.conn.Close()
	}

	if s.connPool != nil {
		s.connPool.Close()
	}

	if s.slotConnPool != nil {
		s.slotConnPool.Close()
	}

	if s.s != nil {
		s.s.Close()
	}

	if s.slotServer != nil {
		s.slotServer.Close()
	}
}

type testServer struct {
	s *store.Store
	h *Handler
}

func testCreateServer(c *C, port int) *testServer {
	base := fmt.Sprintf("/tmp/test_qdb/test_service/%d", port)
	err := os.RemoveAll(base)
	c.Assert(err, IsNil)

	err = os.MkdirAll(base, 0700)
	c.Assert(err, IsNil)

	conf := rocksdb.NewDefaultConfig()
	testdb, err := rocksdb.Open(path.Join(base, "db"), conf, false)
	c.Assert(err, IsNil)

	store := store.New(testdb)

	cfg := NewDefaultConfig()
	cfg.Listen = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.DumpPath = path.Join(base, "rdb.dump")
	cfg.SyncFilePath = path.Join(base, "sync.pipe")
	cfg.ReplBacklogSize = bytesize.MB

	h, err := newHandler(cfg, store)
	c.Assert(err, IsNil)
	go h.run()

	s := new(testServer)
	s.s = store
	s.h = h

	return s
}

type testConn struct {
	net.Conn
}

func testCreateConn(port int) *testConn {
	nc, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil
	}
	return &testConn{nc}
}

func (c *testConn) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

type testConnPool struct {
	p *pools.ResourcePool
}

func testCreateConnPool(port int) *testConnPool {
	f := func() (pools.Resource, error) {
		nc, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return nil, err
		}
		return &testPoolConn{Conn: nc, closed: false, p: nil}, nil
	}

	return &testConnPool{pools.NewResourcePool(f, 4, 4, 10*time.Second)}
}

func (p *testConnPool) Close() {
	p.p.Close()
}

func (p *testConnPool) Get(c *C) *testPoolConn {
	nc, err := p.p.Get()
	c.Assert(err, IsNil)

	pc, ok := nc.(*testPoolConn)
	c.Assert(ok, Equals, true)
	pc.p = p.p
	return pc
}

func (s *testServer) Close() {
	if s.s != nil {
		s.s.Close()
		s.s = nil
	}

	if s.h != nil {
		s.h.close()
		s.h = nil
	}
}

type testPoolConn struct {
	net.Conn
	closed bool
	p      *pools.ResourcePool
}

func newTestPoolConn(tc *testConn) *testPoolConn {
	return &testPoolConn{Conn: tc.Conn, closed: false}
}

func (c *testPoolConn) Close() {
	c.Conn.Close()
	c.closed = true
}

func (c *testPoolConn) Recycle() {
	if c.closed {
		return
	}
	c.p.Put(c)
}

func (pc *testPoolConn) doCmd(c *C, cmd string, args ...interface{}) redis.Resp {
	r := bufio.NewReaderSize(pc.Conn, 32)
	w := bufio.NewWriterSize(pc.Conn, 32)

	req := redis.NewRequest(cmd, args...)
	err := redis.Encode(w, req)
	c.Assert(err, IsNil)

	err = w.Flush()
	c.Assert(err, IsNil)

	resp, err := redis.Decode(r)
	c.Assert(err, IsNil)

	return resp
}

func (pc *testPoolConn) checkNil(c *C, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	switch t := resp.(type) {
	case *redis.BulkBytes:
		c.Assert(t.Value, IsNil)
	case *redis.Array:
		c.Assert(t.Value, IsNil)
	default:
		c.Errorf("invalid nil, type is %T", t)
	}
}

func (pc *testPoolConn) checkOK(c *C, cmd string, args ...interface{}) {
	pc.checkString(c, "OK", cmd, args...)
}

func (pc *testPoolConn) checkString(c *C, expect string, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)

	switch x := resp.(type) {
	case *redis.String:
		c.Assert(x.Value, Equals, expect)
	case *redis.BulkBytes:
		c.Assert(string(x.Value), Equals, expect)
	default:
		c.Errorf("invalid type %T", resp)
	}
}

func (pc *testPoolConn) checkError(c *C, expect string, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	switch x := resp.(type) {
	case *redis.Error:
		c.Assert(x.Value, Equals, expect)
	default:
		c.Errorf("invalid type %T", resp)
	}
}

func (pc *testPoolConn) checkContainError(c *C, expect string, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	switch x := resp.(type) {
	case *redis.Error:
		c.Assert(strings.Contains(x.Value, expect), Equals, true)
	default:
		c.Errorf("invalid type %T", resp)
	}
}

func (pc *testPoolConn) checkInt(c *C, expect int64, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	c.Assert(resp, DeepEquals, redis.NewInt(expect))
}

func (pc *testPoolConn) checkIntApprox(c *C, expect, delta int64, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	v, ok := resp.(*redis.Int)
	c.Assert(ok, Equals, true)
	c.Assert(math.Abs(float64(v.Value-expect)) <= float64(delta), Equals, true)
}

func (pc *testPoolConn) checkFloat(c *C, expect float64, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	var v string
	switch x := resp.(type) {
	case *redis.String:
		v = x.Value
	case *redis.BulkBytes:
		v = string(x.Value)
	default:
		c.Errorf("invalid type, type is %T", resp)
	}

	f, err := strconv.ParseFloat(v, 64)
	c.Assert(err, IsNil)
	c.Assert(math.Abs(f-expect) < 1e-10, Equals, true)
}

func (pc *testPoolConn) checkBytes(c *C, expect []byte, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	v, ok := resp.(*redis.BulkBytes)
	c.Assert(ok, Equals, true)
	c.Assert(v.Value, DeepEquals, expect)
}

func (pc *testPoolConn) checkBytesArray(c *C, cmd string, args ...interface{}) [][]byte {
	resp := pc.doCmd(c, cmd, args...)
	v, ok := resp.(*redis.Array)
	c.Assert(ok, Equals, true)
	if v.Value == nil {
		return nil
	}

	ay := make([][]byte, len(v.Value))
	for i, vv := range v.Value {
		b, ok := vv.(*redis.BulkBytes)
		c.Assert(ok, Equals, true)
		ay[i] = b.Value
	}
	return ay
}

func (pc *testPoolConn) checkIntArray(c *C, expect []int64, cmd string, args ...interface{}) {
	resp := pc.doCmd(c, cmd, args...)
	v, ok := resp.(*redis.Array)
	c.Assert(ok, Equals, true)
	c.Assert(v.Value, HasLen, len(expect))

	for i, vv := range v.Value {
		b, ok := vv.(*redis.Int)
		c.Assert(ok, Equals, true)
		c.Assert(b.Value, Equals, expect[i])
	}
}

func (s *testServiceSuite) getConn(c *C) *testPoolConn {
	return s.connPool.Get(c)
}

func (s *testServiceSuite) checkNil(c *C, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkNil(c, cmd, args...)
}

func (s *testServiceSuite) checkDo(c *C, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	resp := nc.doCmd(c, cmd, args...)
	c.Assert(resp, NotNil)
}

func (s *testServiceSuite) checkOK(c *C, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkString(c, "OK", cmd, args...)
}

func (s *testServiceSuite) checkString(c *C, expect string, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkString(c, expect, cmd, args...)
}

func (s *testServiceSuite) checkError(c *C, expect string, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkError(c, expect, cmd, args...)
}

func (s *testServiceSuite) checkContainError(c *C, expect string, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkContainError(c, expect, cmd, args...)
}

func (s *testServiceSuite) checkInt(c *C, expect int64, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkInt(c, expect, cmd, args...)
}

func (s *testServiceSuite) checkIntApprox(c *C, expect, delta int64, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkIntApprox(c, expect, delta, cmd, args...)
}

func (s *testServiceSuite) checkFloat(c *C, expect float64, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkFloat(c, expect, cmd, args...)
}

func (s *testServiceSuite) checkBytes(c *C, expect []byte, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkBytes(c, expect, cmd, args...)
}

func (s *testServiceSuite) checkBytesArray(c *C, cmd string, args ...interface{}) [][]byte {
	nc := s.getConn(c)
	defer nc.Recycle()

	return nc.checkBytesArray(c, cmd, args...)
}

func (s *testServiceSuite) checkIntArray(c *C, expect []int64, cmd string, args ...interface{}) {
	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkIntArray(c, expect, cmd, args...)
}

var (
	keyMarkSet = make(map[string]bool)
)

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

func randomKey(c *C) string {
	for i := 0; ; i++ {
		p := make([]byte, 16)
		for j := 0; j < len(p); j++ {
			p[j] = 'a' + byte(rand.Intn(26))
		}
		s := "key_" + string(p)
		if _, ok := keyMarkSet[s]; !ok {
			keyMarkSet[s] = true
			return s
		}
		c.Assert(i < 32, Equals, true)
	}
}

func (s *testServiceSuite) TestErrorEuqal(c *C) {
	e1 := errors.New("test error")
	c.Assert(e1, NotNil)

	e2 := jerrors.Trace(e1)
	c.Assert(e2, NotNil)

	e3 := jerrors.Trace(e2)
	c.Assert(e3, NotNil)

	c.Assert(jerrors.Cause(e2), Equals, e1)
	c.Assert(jerrors.Cause(e3), Equals, e1)
	c.Assert(jerrors.Cause(e2), Equals, jerrors.Cause(e3))

	e4 := jerrors.New("test error")
	c.Assert(jerrors.Cause(e4), Not(Equals), e1)

	e5 := jerrors.Errorf("test error")
	c.Assert(jerrors.Cause(e5), Not(Equals), e1)
}
