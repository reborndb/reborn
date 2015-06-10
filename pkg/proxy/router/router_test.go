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

	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testProxyRouterSuite{})

type testServer struct {
	addr  string
	store *store.Store
}

func (s *testServer) Close() {
	if s.store != nil {
		s.store.Close()
		s.store = nil
	}
}

type testProxyRouterSuite struct {
	s *testServer
}

func (s *testProxyRouterSuite) SetUpSuite(c *C) {
	s.s = s.testCreateServer(c, 16380)
	c.Assert(s.s, NotNil)
}

func (s *testProxyRouterSuite) TearDownSuite(c *C) {
	if s.s != nil {
		s.s.Close()
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
	cfg.DumpPath = path.Join(base, "rdb.dump")
	cfg.SyncFilePath = path.Join(base, "sync.pipe")

	store := store.New(testdb)
	go service.Serve(cfg, store)

	ss := new(testServer)
	ss.addr = cfg.Listen
	ss.store = store

	return ss
}

var (

	conf           *Conf
	ss             *Server
	once           sync.Once
	waitonce       sync.Once
	conn           zkhelper.Conn
	redis1         *miniredis.Miniredis
	redis2         *miniredis.Miniredis
	proxyMutex     sync.Mutex
	proxyPassword  = "123"
	serverPassword = "abc"
)

func (s *testProxyRouterSuite) InitEnv(c *C) {
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

		redis1, _ = miniredis.Run()
		redis2, _ = miniredis.Run()
		redis1.RequireAuth(storeAuth)
		redis2.RequireAuth(storeAuth)

		s1 := models.NewServer(models.SERVER_TYPE_MASTER, redis1.Addr())
		s2 := models.NewServer(models.SERVER_TYPE_MASTER, redis2.Addr())

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

func (s *testProxyRouterSuite) testDialProxy(c *C, addr string) (redis.Conn, error) {
	cc, err := redis.Dial("tcp", addr)
	c.Assert(err, IsNil)

	if len(proxyPassword) > 0 {
		ok, err := redis.String(cc.Do("AUTH", proxyPassword))
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, "OK")
	}

	return cc, nil
}

func (s *testProxyRouterSuite) TestSingleKeyRedisCmd(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("SET", "foo", "bar")
	c.Assert(err, IsNil)

	got, err := redis.String(cc.Do("get", "foo"))
	c.Assert(err, IsNil)
	c.Assert(got, Equals, "bar")

	_, err = cc.Do("SET", "bar", "foo")
	c.Assert(err, IsNil)

	got, err = redis.String(cc.Do("get", "bar"))
	c.Assert(err, IsNil)
	c.Assert(got, Equals, "foo")
}

func (s *testProxyRouterSuite) TestMget(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
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
}

func (s *testProxyRouterSuite) TestMultiKeyRedisCmd(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("SET", "key1", "value1")
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
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdUnknown(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("unknown", "key1", "key2", "key3")
	c.Assert(err, NotNil)
}

func (s *testProxyRouterSuite) TestNotAllowedCmd(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("save")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not allowed"), Equals, true)
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdPing(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	reply, err := cc.Do("ping")
	c.Assert(err, IsNil)
	c.Assert(reply.(string), Equals, "PONG")
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdQuit(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("quit")
	c.Assert(err, IsNil)
}

func (s *testProxyRouterSuite) TestInvalidRedisCmdEcho(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("echo", "xx")
	c.Assert(err, IsNil)

	_, err = cc.Do("echo")
	c.Assert(err, IsNil)
}

// this should be the last test
func (s *testProxyRouterSuite) TestMarkOffline(c *C) {
	s.InitEnv(c)

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

func (s *testProxyRouterSuite) TestRedisRestart(c *C) {
	s.InitEnv(c)

	cc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer cc.Close()

	_, err = cc.Do("SET", "key1", "value1")
	c.Assert(err, IsNil)

	_, err = cc.Do("SET", "key2", "value2")
	c.Assert(err, IsNil)

	// close redis
	redis1.Close()
	redis2.Close()
	_, err = cc.Do("SET", "key1", "value1")
	c.Assert(err, NotNil)

	_, err = cc.Do("SET", "key2", "value2")
	c.Assert(err, NotNil)

	// restart redis
	redis1.Restart()
	redis2.Restart()

	redis1.RequireAuth(serverPassword)
	redis2.RequireAuth(serverPassword)

	time.Sleep(3 * time.Second)

	// proxy should closed our connection
	_, err = cc.Do("SET", "key1", "value1")
	c.Assert(err, NotNil)

	// now, proxy should recovered from connection error
	ccc, err := s.testDialProxy(c, "localhost:19000")
	c.Assert(err, IsNil)
	defer ccc.Close()

	_, err = ccc.Do("SET", "key1", "value1")
	c.Assert(err, IsNil)
}
