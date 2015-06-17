// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) TestXAppend(c *C) {
	k := randomKey(c)
	s.checkNil(c, "get", k)
	s.checkInt(c, 5, "append", k, "hello")
	s.checkInt(c, 11, "append", k, " world")
	s.checkString(c, "hello world", "get", k)
}

func (s *testServiceSuite) TestXDecr(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, 10)
	s.checkInt(c, 9, "decr", k)
	s.checkOK(c, "set", k, -100)
	s.checkInt(c, -101, "decr", k)
}

func (s *testServiceSuite) TestXDecrBy(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, 10)
	s.checkInt(c, 5, "decrby", k, 5)
	s.checkInt(c, 5, "decrby", k, 0)
}

func (s *testServiceSuite) TestXGet(c *C) {
	k := randomKey(c)
	s.checkNil(c, "get", k)
	s.checkOK(c, "set", k, "hello world")
	s.checkString(c, "hello world", "get", k)
	s.checkOK(c, "set", k, "goodbye")
}

func (s *testServiceSuite) TestXGetSet(c *C) {
	k := randomKey(c)
	s.checkInt(c, 1, "incr", k)
	s.checkString(c, "1", "getset", k, 0)
	s.checkString(c, "0", "get", k)
	s.checkInt(c, 1, "del", k)
	s.checkOK(c, "set", k, "hello")
	s.checkString(c, "hello", "getset", k, "")
	s.checkString(c, "", "getset", k, "hello")
	s.checkString(c, "hello", "get", k)
}

func (s *testServiceSuite) TestXIncr(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, 10)
	s.checkInt(c, 11, "incr", k)
	s.checkString(c, "11", "get", k)
}

func (s *testServiceSuite) TestXIncrBy(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, 10)
	s.checkInt(c, 15, "incrby", k, 5)
}

func (s *testServiceSuite) TestXIncrByFloat(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, 10.50)
	s.checkFloat(c, 10.6, "incrbyfloat", k, 0.1)
	s.checkOK(c, "set", k, "5.0e3")
	s.checkFloat(c, 5200, "incrbyfloat", k, 2.0e2)
	s.checkOK(c, "set", k, "0")
	s.checkFloat(c, 996945661, "incrbyfloat", k, 996945661)
}

func (s *testServiceSuite) TestXSet(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, "hello")
	s.checkString(c, "hello", "get", k)
	s.checkOK(c, "set", k, "")
	s.checkString(c, "", "get", k)
}

func (s *testServiceSuite) TestXPSetEX(c *C) {
	k := randomKey(c)
	s.checkOK(c, "psetex", k, 1000*1000, "hello")
	s.checkOK(c, "psetex", k, 2000*1000, "world")
	s.checkString(c, "world", "get", k)
	s.checkIntApprox(c, 2000, 5, "ttl", k)
	s.checkOK(c, "psetex", k, 1000*1000, "")
	s.checkString(c, "", "get", k)
}

func (s *testServiceSuite) TestXSetEX(c *C) {
	k := randomKey(c)
	s.checkOK(c, "setex", k, 1000, "hello")
	s.checkOK(c, "setex", k, 2000, "world")
	s.checkString(c, "world", "get", k)
	s.checkIntApprox(c, 2000, 5, "ttl", k)
	s.checkOK(c, "setex", k, 1000, "")
	s.checkString(c, "", "get", k)
}

func (s *testServiceSuite) TestXSetNX(c *C) {
	k := randomKey(c)
	s.checkInt(c, 1, "setnx", k, "hello")
	s.checkInt(c, 0, "setnx", k, "world")
	s.checkString(c, "hello", "get", k)
	s.checkInt(c, -1, "ttl", k)
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, 1, "setnx", k, "")
	s.checkString(c, "", "get", k)
}

func (s *testServiceSuite) TestXSetRange(c *C) {
	k := randomKey(c)
	s.checkInt(c, 7, "setrange", k, 2, "redis")
	s.checkString(c, "\x00\x00redis", "get", k)
	s.checkInt(c, 7, "setrange", k, 1, "redis")
	s.checkString(c, "\x00rediss", "get", k)
	s.checkInt(c, 11, "setrange", k, 0, "hello world")
	s.checkString(c, "hello world", "get", k)
}

func (s *testServiceSuite) TestXSetBit(c *C) {
	k := randomKey(c)
	s.checkInt(c, 0, "setbit", k, 3, 1)
	s.checkString(c, "\x08", "get", k)
	s.checkInt(c, 1, "setbit", k, 3, 1)
	s.checkString(c, "\x08", "get", k)
	s.checkInt(c, 1, "setbit", k, 3, 0)
	s.checkString(c, "\x00", "get", k)
	s.checkInt(c, 0, "setbit", k, 8, 1)
	s.checkString(c, "\x00\x01", "get", k)
}

func (s *testServiceSuite) TestXMSet(c *C) {
	k := randomKey(c)
	s.checkOK(c, "mset", k, 0, k+"1", 1, k+"2", 2, k+"3", 3)
	s.checkString(c, "0", "get", k)
	s.checkString(c, "1", "get", k+"1")
	s.checkString(c, "2", "get", k+"2")
	s.checkString(c, "3", "get", k+"3")
	s.checkOK(c, "mset", k, 100, k, 1000)
	s.checkString(c, "1000", "get", k)
	s.checkOK(c, "mset", k+"11", "", k+"12", "", k+"13", "", k+"14", "")
	s.checkString(c, "", "get", k+"11")
	s.checkString(c, "", "get", k+"12")
	s.checkString(c, "", "get", k+"13")
	s.checkString(c, "", "get", k+"14")
}

func (s *testServiceSuite) TestMSetNX(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, "haha")
	s.checkInt(c, 0, "msetnx", k, "1", "1", "1", "2", "2")
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, 1, "msetnx", k, "1", k, "2")
	s.checkString(c, "2", "get", k)
	s.checkInt(c, 1, "msetnx", "3", "", "4", "")
	s.checkString(c, "", "get", "3")
	s.checkString(c, "", "get", "4")
}

func (s *testServiceSuite) TestMGet(c *C) {
	k := randomKey(c)
	s.checkOK(c, "mset", k, 0, k+"1", 1, k+"2", 2, k+"3", 3, k+"4", "")
	a := s.checkBytesArray(c, "mget", k, k+"1", k+"2", k+"3", k+"4")
	c.Assert(a, HasLen, 5)
	c.Assert(a, DeepEquals, [][]byte{[]byte("0"), []byte("1"), []byte("2"), []byte("3"), []byte("")})
}
