// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) checkHash(c *C, key string, expect map[string]string) {
	ay := s.checkBytesArray(c, "HGETALL", key)
	if expect == nil {
		c.Assert(ay, IsNil)
	} else {
		c.Assert(ay, HasLen, 2*len(expect))
		n := len(expect)
		for i := 0; i < n; i++ {
			field := string(ay[i*2])
			value := string(ay[i*2+1])
			c.Assert(expect[field], Equals, value)
			delete(expect, field)
		}
		c.Assert(expect, HasLen, 0)
	}
}

func (s *testServiceSuite) TestHDel(c *C) {
	key := randomKey(c)
	s.checkInt(c, 0, "hdel", key, "key1")
	s.checkOK(c, "hmset", key, "key1", "hello1", "key2", "hello2")
	s.checkInt(c, 1, "hdel", key, "key1")
	s.checkInt(c, 0, "hdel", key, "key1")
	s.checkInt(c, 1, "hdel", key, "key2")
	s.checkHash(c, key, nil)
}

func (s *testServiceSuite) TestHSet(c *C) {
	key := randomKey(c)
	s.checkInt(c, 1, "hset", key, "field1", "value")
	s.checkInt(c, 0, "hset", key, "field1", "value2")
	s.checkHash(c, key, map[string]string{"field1": "value2"})
}

func (s *testServiceSuite) TestHGet(c *C) {
	key := randomKey(c)
	s.checkNil(c, "hget", key, "field")
	s.checkInt(c, 1, "hset", key, "field", "value")
	s.checkString(c, "value", "hget", key, "field")
}

func (s *testServiceSuite) TestHLen(c *C) {
	key := randomKey(c)
	s.checkInt(c, 0, "hlen", key)
	s.checkInt(c, 1, "hset", key, "field", "value")
	s.checkInt(c, 1, "hlen", key)
	s.checkInt(c, 1, "hdel", key, "field")
	s.checkInt(c, 0, "hlen", key)
}

func (s *testServiceSuite) TestHExists(c *C) {
	key := randomKey(c)
	s.checkInt(c, 0, "hexists", key, "field")
	s.checkInt(c, 1, "hset", key, "field", "value")
	s.checkInt(c, 1, "hexists", key, "field")
}

func (s *testServiceSuite) TestHMSet(c *C) {
	key := randomKey(c)
	s.checkOK(c, "hmset", key, "key1", "hello1", "key2", "hello2")
	s.checkOK(c, "hmset", key, "key1", "world1")
	s.checkHash(c, key, map[string]string{"key1": "world1", "key2": "hello2"})
}

func (s *testServiceSuite) TestHIncrBy(c *C) {
	key := randomKey(c)
	s.checkInt(c, 1, "hset", key, "key", 5)
	s.checkInt(c, 6, "hincrby", key, "key", 1)
	s.checkInt(c, 5, "hincrby", key, "key", -1)
	s.checkInt(c, -5, "hincrby", key, "key", -10)
	s.checkInt(c, 1, "hincrby", key, "key2", 1)
}

func (s *testServiceSuite) TestHIncrByFloat(c *C) {
	key := randomKey(c)
	s.checkFloat(c, 10.5, "hincrbyfloat", key, "field", 10.5)
	s.checkFloat(c, 10.6, "hincrbyfloat", key, "field", 0.1)
	s.checkInt(c, 1, "hset", key, "field2", 2.0e2)
	s.checkFloat(c, 5200, "hincrbyfloat", key, "field2", 5.0e3)
}

func (s *testServiceSuite) TestHSetNX(c *C) {
	key := randomKey(c)
	s.checkInt(c, 1, "hsetnx", key, "key", "hello")
	s.checkInt(c, 0, "hsetnx", key, "key", "world")
	s.checkHash(c, key, map[string]string{"key": "hello"})
	s.checkInt(c, 1, "del", key)
	s.checkHash(c, key, nil)
}

func (s *testServiceSuite) TestHMGet(c *C) {
	key := randomKey(c)

	a := s.checkBytesArray(c, "hmget", key, "field1", "field2")
	c.Assert(a, HasLen, 2)
	c.Assert(a, DeepEquals, [][]byte{nil, nil})

	s.checkInt(c, 1, "hsetnx", key, "field1", "value")
	a = s.checkBytesArray(c, "hmget", key, "field1", "field2", "field1")
	c.Assert(a, HasLen, 3)
	c.Assert(a, DeepEquals, [][]byte{[]byte("value"), nil, []byte("value")})
}

func (s *testServiceSuite) TestHKeys(c *C) {
	key := randomKey(c)
	a := s.checkBytesArray(c, "hkeys", key)
	c.Assert(a, HasLen, 0)

	s.checkInt(c, 1, "hsetnx", key, "field1", "value")
	a = s.checkBytesArray(c, "hkeys", key)
	c.Assert(a, HasLen, 1)
	c.Assert(a, DeepEquals, [][]byte{[]byte("field1")})
}

func (s *testServiceSuite) TestHVals(c *C) {
	key := randomKey(c)
	a := s.checkBytesArray(c, "hvals", key)
	c.Assert(a, HasLen, 0)

	s.checkInt(c, 1, "hsetnx", key, "field1", "value")
	a = s.checkBytesArray(c, "hvals", key)
	c.Assert(a, HasLen, 1)
	c.Assert(a, DeepEquals, [][]byte{[]byte("value")})
}
