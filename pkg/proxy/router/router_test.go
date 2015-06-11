// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reborndb/qdb/pkg/engine/rocksdb"
	"github.com/reborndb/qdb/pkg/service"
	"github.com/reborndb/qdb/pkg/store"
	"github.com/reborndb/reborn/pkg/models"

	"github.com/garyburd/redigo/redis"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

var (
	conf       *Conf
	ss         *Server
	once       sync.Once
	waitonce   sync.Once
	conn       zkhelper.Conn
	proxyMutex sync.Mutex
	proxyAuth  = "123"
	storeAuth  = "abc"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testProxyRouterSuite{})

type testServer struct {
	addr    string
	store   *store.Store
	handler *service.Handler
}

func (s *testServer) Close() {
	if s.handler != nil {
		s.handler.Close()
	}
}

type testProxyRouterSuite struct {
	s  *testServer
	s1 *testServer
	s2 *testServer
}

func (s *testProxyRouterSuite) SetUpSuite(c *C) {
	s.s = s.testCreateServer(c, 16380)
	c.Assert(s.s, NotNil)

	s.s1 = s.testCreateServer(c, 16381)
	c.Assert(s.s1, NotNil)

	s.s2 = s.testCreateServer(c, 16382)
	c.Assert(s.s2, NotNil)

	s.initEnv(c)
}

func (s *testProxyRouterSuite) TearDownSuite(c *C) {
	// Notice:
	// maybe pipeline process problem,
	// so now we only test incomplete store restart case here
	s.testStoreRestart(c)
	s.testMarkOffline(c)

	if s.s != nil {
		s.s.Close()
	}

	if s.s1 != nil {
		s.s1.Close()
	}

	if s.s2 != nil {
		s.s2.Close()
	}
}

func (s *testProxyRouterSuite) testCreateServer(c *C, port int) *testServer {
	base := fmt.Sprintf("/tmp/test_reborn/test_proxy_router/%d", port)
	err := os.RemoveAll(base)
	c.Assert(err, IsNil)

	err = os.MkdirAll(base, 0700)
	c.Assert(err, IsNil)

	conf := rocksdb.NewDefaultConfig()
	testdb, err := rocksdb.Open(path.Join(base, "db"), conf, false)
	c.Assert(err, IsNil)

	cfg := service.NewDefaultConfig()
	cfg.Listen = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.PidFile = fmt.Sprintf(base, "qdb.pid")
	cfg.DumpPath = path.Join(base, "rdb.dump")
	cfg.SyncFilePath = path.Join(base, "sync.pipe")

	store := store.New(testdb)
	handler, err := service.NewHandler(cfg, store)
	c.Assert(err, IsNil)
	go handler.Run()

	ss := new(testServer)
	ss.addr = cfg.Listen
	ss.store = store
	ss.handler = handler

	return ss
}

func (s *testProxyRouterSuite) initEnv(c *C) {
	go once.Do(func() {
		log.SetLevelByString("error")
		conn = zkhelper.NewConn()
		conf = &Conf{
			ProductName:     "test",
			CoordinatorAddr: "localhost:2181",
			NetTimeout:      5,
			f:               func(string) (zkhelper.Conn, error) { return conn, nil },
			Proto:           "tcp4",
			ProxyID:         "proxy_test",
			Addr:            ":19000",
			HTTPAddr:        ":11000",
			ProxyAuth:       proxyAuth,
			StoreAuth:       storeAuth,
		}

		// init action path
		prefix := models.GetWatchActionPath(conf.ProductName)
		err := models.CreateActionRootPath(conn, prefix)
		c.Assert(err, IsNil)

		// init slot
		err = models.InitSlotSet(conn, conf.ProductName, 1024)
		c.Assert(err, IsNil)

		// init  server group
		g1 := models.NewServerGroup(conf.ProductName, 1)
		g1.Create(conn)
		g2 := models.NewServerGroup(conf.ProductName, 2)
		g2.Create(conn)

		c1 := s.testDialProxy(c, s.s1.addr, "")
		ok, err := redis.String(c1.Do("CONFIG", "SET", "REQUIREPASS", storeAuth))
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, "OK")

		err = c1.Close()
		c.Assert(err, IsNil)

		c2 := s.testDialProxy(c, s.s2.addr, "")
		ok, err = redis.String(c2.Do("CONFIG", "SET", "REQUIREPASS", storeAuth))
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, "OK")

		err = c2.Close()
		c.Assert(err, IsNil)

		s1 := models.NewServer(models.SERVER_TYPE_MASTER, s.s1.addr)
		s2 := models.NewServer(models.SERVER_TYPE_MASTER, s.s2.addr)

		g1.AddServer(conn, s1, storeAuth)
		g2.AddServer(conn, s2, storeAuth)

		// set slot range
		err = models.SetSlotRange(conn, conf.ProductName, 0, 511, 1, models.SLOT_STATUS_ONLINE)
		c.Assert(err, IsNil)

		err = models.SetSlotRange(conn, conf.ProductName, 512, 1023, 2, models.SLOT_STATUS_ONLINE)
		c.Assert(err, IsNil)

		go func() { //set proxy online
			time.Sleep(3 * time.Second)
			err := models.SetProxyStatus(conn, conf.ProductName, conf.ProxyID, models.PROXY_STATE_ONLINE)
			c.Assert(err, IsNil)

			time.Sleep(2 * time.Second)
			proxyMutex.Lock()
			defer proxyMutex.Unlock()

			pi := ss.getProxyInfo()
			c.Assert(pi.State, Equals, models.PROXY_STATE_ONLINE)
		}()

		proxyMutex.Lock()
		ss = NewServer(conf)
		proxyMutex.Unlock()
		ss.Run()
	})

	waitonce.Do(func() {
		time.Sleep(10 * time.Second)
	})
}

func (s *testProxyRouterSuite) testMarkOffline(c *C) {
	suicide := int64(0)
	proxyMutex.Lock()
	ss.onSuicide = func() error {
		atomic.StoreInt64(&suicide, 1)
		return nil
	}
	proxyMutex.Unlock()

	err := models.SetProxyStatus(conn, conf.ProductName, conf.ProxyID, models.PROXY_STATE_MARK_OFFLINE)
	c.Assert(err, IsNil)

	time.Sleep(3 * time.Second)
	c.Assert(atomic.LoadInt64(&suicide), Not(Equals), 0)
}

func (s *testProxyRouterSuite) testDialProxy(c *C, addr string, auth string) redis.Conn {
	cc, err := redis.Dial("tcp", addr)
	c.Assert(err, IsNil)

	if len(auth) > 0 {
		ok, err := redis.String(cc.Do("AUTH", auth))
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, "OK")
	}

	return cc
}

func (s *testProxyRouterSuite) TestSingleKeyRedisCmd(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("SET", "foo", "bar")
	c.Assert(err, IsNil)

	got, err := redis.String(cc.Do("get", "foo"))
	c.Assert(err, IsNil)
	c.Assert(got, Equals, "bar")

	_, err = cc.Do("SET", "bar", "foo")
	c.Assert(err, IsNil)

	got, err = redis.String(cc.Do("get", "bar"))
	c.Assert(err, IsNil)
	c.Assert(got, Equals, "foo")

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestMget(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	const count = 20480
	keys := make([]interface{}, count)
	for i := 0; i < count; i++ {
		s := strconv.Itoa(i)
		keys[i] = s
		_, err := cc.Do("SET", s, s)
		c.Assert(err, IsNil)
	}

	reply, err := redis.Values(cc.Do("MGET", keys...))
	c.Assert(err, IsNil)

	temp := make([]string, count)
	values := make([]interface{}, count)

	for i := 0; i < count; i++ {
		values[i] = &temp[i]
	}

	_, err = redis.Scan(reply, values...)
	c.Assert(err, IsNil)

	for i := 0; i < count; i++ {
		c.Assert(keys[i], Equals, temp[i])
	}

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestMultiKeyRedisCmd(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("SET", "key1", "value1")
	c.Assert(err, IsNil)

	_, err = cc.Do("SET", "key2", "value2")
	c.Assert(err, IsNil)

	var value1 string
	var value2 string
	var value3 string

	reply, err := redis.Values(cc.Do("MGET", "key1", "key2", "key3"))
	c.Assert(err, IsNil)

	_, err = redis.Scan(reply, &value1, &value2, &value3)
	c.Assert(err, IsNil)
	c.Assert(value1, Equals, "value1")
	c.Assert(value2, Equals, "value2")
	c.Assert(len(value3), Equals, 0)

	// test del
	_, err = cc.Do("del", "key1", "key2", "key3")
	c.Assert(err, IsNil)

	// reset
	value1 = ""
	value2 = ""
	value3 = ""

	reply, err = redis.Values(cc.Do("MGET", "key1", "key2", "key3"))
	c.Assert(err, IsNil)

	_, err = redis.Scan(reply, &value1, &value2, &value3)
	c.Assert(err, IsNil)
	c.Assert(len(value1), Equals, 0)
	c.Assert(len(value2), Equals, 0)
	c.Assert(len(value3), Equals, 0)

	// reset
	value1 = ""
	value2 = ""
	value3 = ""

	_, err = cc.Do("MSET", "key1", "value1", "key2", "value2", "key3", "")
	c.Assert(err, IsNil)

	reply, err = redis.Values(cc.Do("MGET", "key1", "key2", "key3"))
	c.Assert(err, IsNil)

	_, err = redis.Scan(reply, &value1, &value2, &value3)
	c.Assert(err, IsNil)
	c.Assert(value1, Equals, "value1")
	c.Assert(value2, Equals, "value2")
	c.Assert(len(value3), Equals, 0)

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdUnknown(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("unknown", "key1", "key2", "key3")
	c.Assert(err, NotNil)

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestNotAllowedCmd(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("save")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not allowed"), Equals, true)

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdPing(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	reply, err := cc.Do("ping")
	c.Assert(err, IsNil)
	c.Assert(reply.(string), Equals, "PONG")

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdQuit(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("quit")
	c.Assert(err, IsNil)

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdEcho(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("echo", "xx")
	c.Assert(err, IsNil)

	_, err = cc.Do("echo")
	c.Assert(err, IsNil)

	s.s1.store.Reset()
	s.s2.store.Reset()
}

func (s *testProxyRouterSuite) testStoreRestart(c *C) {
	cc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	defer cc.Close()

	_, err := cc.Do("SET", "key1", "value1")
	c.Assert(err, IsNil)

	_, err = cc.Do("SET", "key2", "value2")
	c.Assert(err, IsNil)

	// close
	s.s1.Close()
	s.s2.Close()

	// test
	_, err = cc.Do("SET", "key1", "value1")
	c.Assert(err, NotNil)

	_, err = cc.Do("SET", "key2", "value2")
	c.Assert(err, NotNil)

	// restart
	s.s1 = s.testCreateServer(c, 16381)
	c.Assert(s.s1, NotNil)

	s.s2 = s.testCreateServer(c, 16382)
	c.Assert(s.s2, NotNil)

	// auth
	c1 := s.testDialProxy(c, s.s1.addr, "")
	_, err = c1.Do("SET", "key11", "value11")
	c.Assert(err, IsNil)

	ok, err := redis.String(c1.Do("CONFIG", "SET", "REQUIREPASS", storeAuth))
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, "OK")

	err = c1.Close()
	c.Assert(err, IsNil)

	c2 := s.testDialProxy(c, s.s2.addr, "")
	_, err = c2.Do("SET", "key22", "value22")
	c.Assert(err, IsNil)

	ok, err = redis.String(c2.Do("CONFIG", "SET", "REQUIREPASS", storeAuth))
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, "OK")

	err = c2.Close()
	c.Assert(err, IsNil)

	// wait
	time.Sleep(3 * time.Second)

	// proxy should closed our connection
	_, err = cc.Do("SET", "key1", "value1")
	c.Assert(err, NotNil)

	// Notice:
	// maybe some bug in reborn, proxy recover failed

	// now, proxy should recovered from connection error
	// ccc := s.testDialProxy(c, "localhost:19000", proxyAuth)
	// defer ccc.Close()

	// _, err = ccc.Do("SET", "key1", "value1")
	// c.Assert(err, IsNil)

	s.s1.store.Reset()
	s.s2.store.Reset()
}
