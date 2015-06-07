// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reborndb/reborn/pkg/models"

	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
	"github.com/juju/errors"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
)

var (
	conf           *Conf
	s              *Server
	once           sync.Once
	waitonce       sync.Once
	conn           zkhelper.Conn
	redis1         *miniredis.Miniredis
	redis2         *miniredis.Miniredis
	proxyMutex     sync.Mutex
	proxyPassword  = "123"
	serverPassword = "abc"
)

func InitEnv() {
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
			ProxyPassword:   proxyPassword,
			ServerPassword:  serverPassword,
		}

		//init action path
		prefix := models.GetWatchActionPath(conf.ProductName)
		err := models.CreateActionRootPath(conn, prefix)
		if err != nil {
			log.Fatal(err)
		}

		//init slot
		err = models.InitSlotSet(conn, conf.ProductName, 1024)
		if err != nil {
			log.Fatal(err)
		}

		//init  server group
		g1 := models.NewServerGroup(conf.ProductName, 1)
		g1.Create(conn)
		g2 := models.NewServerGroup(conf.ProductName, 2)
		g2.Create(conn)

		redis1, _ = miniredis.Run()
		redis2, _ = miniredis.Run()
		redis1.RequireAuth(conf.ServerPassword)
		redis2.RequireAuth(conf.ServerPassword)

		s1 := models.NewServer(models.SERVER_TYPE_MASTER, redis1.Addr())
		s2 := models.NewServer(models.SERVER_TYPE_MASTER, redis2.Addr())

		g1.AddServer(conn, s1)
		g2.AddServer(conn, s2)

		//set slot range
		err = models.SetSlotRange(conn, conf.ProductName, 0, 511, 1, models.SLOT_STATUS_ONLINE)
		if err != nil {
			log.Fatal(err)
		}

		err = models.SetSlotRange(conn, conf.ProductName, 512, 1023, 2, models.SLOT_STATUS_ONLINE)
		if err != nil {
			log.Fatal(err)
		}

		go func() { //set proxy online
			time.Sleep(3 * time.Second)
			err := models.SetProxyStatus(conn, conf.ProductName, conf.ProxyID, models.PROXY_STATE_ONLINE)
			if err != nil {
				log.Fatal(errors.ErrorStack(err))
			}
			time.Sleep(2 * time.Second)
			proxyMutex.Lock()
			defer proxyMutex.Unlock()
			pi := s.getProxyInfo()
			if pi.State != models.PROXY_STATE_ONLINE {
				log.Fatalf("should be online, we got %s", pi.State)
			}
		}()

		proxyMutex.Lock()
		s = NewServer(conf)
		proxyMutex.Unlock()
		s.Run()
	})

	waitonce.Do(func() {
		time.Sleep(10 * time.Second)
	})
}

func testDialProxy(addr string) (redis.Conn, error) {
	c, err := redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	if len(proxyPassword) > 0 {
		if ok, err := redis.String(c.Do("AUTH", proxyPassword)); err != nil {
			c.Close()
			return nil, errors.Trace(err)
		} else if ok != "OK" {
			c.Close()
			return nil, errors.Errorf("not got ok but %s", ok)
		}
	}

	return c, nil
}

func TestSingleKeyRedisCmd(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("SET", "foo", "bar")
	if err != nil {
		t.Error(err)
	}

	if got, err := redis.String(c.Do("get", "foo")); err != nil || got != "bar" {
		t.Error("'foo' has the wrong value")
	}

	_, err = c.Do("SET", "bar", "foo")
	if err != nil {
		t.Error(err)
	}

	if got, err := redis.String(c.Do("get", "bar")); err != nil || got != "foo" {
		t.Error("'bar' has the wrong value")
	}
}

func TestMget(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const count = 20480
	keys := make([]interface{}, count)
	for i := 0; i < count; i++ {
		s := strconv.Itoa(i)
		keys[i] = s
		_, err := c.Do("SET", s, s)
		if err != nil {
			t.Fatal(err)
		}
	}

	reply, err := redis.Values(c.Do("MGET", keys...))
	if err != nil {
		t.Fatal(err)
	}

	temp := make([]string, count)
	values := make([]interface{}, count)

	for i := 0; i < count; i++ {
		values[i] = &temp[i]
	}
	if _, err := redis.Scan(reply, values...); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i++ {
		if keys[i] != temp[i] {
			t.Fatalf("key, value not match, expect %v, got %v, reply:%+v",
				keys[i], temp[i], reply)
		}
	}
}

func TestMultiKeyRedisCmd(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("SET", "key1", "value1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Do("SET", "key2", "value2")
	if err != nil {
		t.Fatal(err)
	}

	var value1 string
	var value2 string
	var value3 string
	reply, err := redis.Values(c.Do("MGET", "key1", "key2", "key3"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := redis.Scan(reply, &value1, &value2, &value3); err != nil {
		t.Fatal(err)
	}

	if value1 != "value1" || value2 != "value2" || len(value3) != 0 {
		t.Error("value not match")
	}

	//test del
	if _, err := c.Do("del", "key1", "key2", "key3"); err != nil {
		t.Fatal(err)
	}

	//reset
	value1 = ""
	value2 = ""
	value3 = ""
	reply, err = redis.Values(c.Do("MGET", "key1", "key2", "key3"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := redis.Scan(reply, &value1, &value2, &value3); err != nil {
		t.Fatal(err)
	}

	if len(value1) != 0 || len(value2) != 0 || len(value3) != 0 {
		t.Error("value not match", value1, value2, value3)
	}

	//reset
	value1 = ""
	value2 = ""
	value3 = ""

	_, err = c.Do("MSET", "key1", "value1", "key2", "value2", "key3", "")
	if err != nil {
		t.Fatal(err)
	}

	reply, err = redis.Values(c.Do("MGET", "key1", "key2", "key3"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := redis.Scan(reply, &value1, &value2, &value3); err != nil {
		t.Fatal(err)
	}

	if value1 != "value1" || value2 != "value2" || len(value3) != 0 {
		t.Error("value not match", value1, value2, value3)
	}
}

func TestInvalidRedisCmdUnknown(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Do("unknown", "key1", "key2", "key3"); err == nil {
		t.Fatal(err)
	}
}

func TestNotAllowedCmd(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("save")
	if err == nil {
		t.Error("should report error")
	}

	if strings.Index(err.Error(), "not allowed") < 0 {
		t.Error("should report error")
	}
}

func TestInvalidRedisCmdPing(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	reply, err := c.Do("ping")

	if reply.(string) != "PONG" {
		t.Error("should report error", reply)
	}
}

func TestInvalidRedisCmdQuit(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("quit")
	if err != nil {
		t.Fatal(err)
	}
}

func TestInvalidRedisCmdEcho(t *testing.T) {
	InitEnv()
	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("echo", "xx")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Do("echo")
	if err != nil {
		t.Fatal(err)
	}

}

//this should be the last test
func TestMarkOffline(t *testing.T) {
	InitEnv()

	suicide := int64(0)
	proxyMutex.Lock()
	s.onSuicide = func() error {
		atomic.StoreInt64(&suicide, 1)
		return nil
	}
	proxyMutex.Unlock()

	err := models.SetProxyStatus(conn, conf.ProductName, conf.ProxyID, models.PROXY_STATE_MARK_OFFLINE)
	if err != nil {
		t.Fatal(errors.ErrorStack(err))
	}

	time.Sleep(3 * time.Second)

	if atomic.LoadInt64(&suicide) == 0 {
		t.Error("shoud be suicided")
	}
}

func TestRedisRestart(t *testing.T) {
	InitEnv()

	c, err := testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("SET", "key1", "value1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Do("SET", "key2", "value2")
	if err != nil {
		t.Fatal(err)
	}

	//close redis
	redis1.Close()
	redis2.Close()
	_, err = c.Do("SET", "key1", "value1")
	if err == nil {
		t.Fatal("should be error")
	}
	_, err = c.Do("SET", "key2", "value2")
	if err == nil {
		t.Fatal("should be error")
	}

	//restart redis
	redis1.Restart()
	redis2.Restart()
	redis1.RequireAuth(serverPassword)
	redis2.RequireAuth(serverPassword)

	time.Sleep(3 * time.Second)
	//proxy should closed our connection
	_, err = c.Do("SET", "key1", "value1")
	if err == nil {
		t.Error("should be error")
	}

	//now, proxy should recovered from connection error
	c, err = testDialProxy("localhost:19000")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Do("SET", "key1", "value1")
	if err != nil {
		t.Fatal(err)
	}
}
