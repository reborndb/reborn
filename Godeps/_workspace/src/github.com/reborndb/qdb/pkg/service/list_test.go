// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) checkList(c *C, key string, expect []string) {
	ay := s.checkBytesArray(c, "lrange", key, 0, -1)
	if expect == nil {
		c.Assert(ay, IsNil)
	} else {
		c.Assert(ay, HasLen, len(expect))
		for i := 0; i < len(expect); i++ {
			c.Assert(string(ay[i]), Equals, expect[i])
		}
	}
}

func (s *testServiceSuite) TestLPush(c *C) {
	k := randomKey(c)
	s.checkInt(c, 1, "lpush", k, "key1")
	s.checkInt(c, 2, "lpush", k, "key2")
	s.checkInt(c, 4, "lpush", k, "key3", "key4")
	s.checkList(c, k, []string{"key4", "key3", "key2", "key1"})
}

func (s *testServiceSuite) TestLPushX(c *C) {
	k := randomKey(c)
	s.checkInt(c, 0, "lpushx", k, "key1")
	s.checkList(c, k, nil)
	s.checkInt(c, 1, "lpush", k, "key1")
	s.checkInt(c, 2, "lpushx", k, "key2")
	s.checkList(c, k, []string{"key2", "key1"})
}

func (s *testServiceSuite) TestLPop(c *C) {
	k := randomKey(c)
	s.checkInt(c, 4, "lpush", k, "key1", "key2", "key3", "key4")
	s.checkString(c, "key4", "lpop", k)
	s.checkString(c, "key3", "lpop", k)
	s.checkList(c, k, []string{"key2", "key1"})
	s.checkString(c, "key2", "lpop", k)
	s.checkString(c, "key1", "lpop", k)
	s.checkList(c, k, nil)
	s.checkNil(c, "lpop", k)
}

func (s *testServiceSuite) TestRPush(c *C) {
	k := randomKey(c)
	s.checkInt(c, 1, "rpush", k, "key1")
	s.checkInt(c, 2, "rpush", k, "key2")
	s.checkInt(c, 4, "rpush", k, "key3", "key4")
	s.checkList(c, k, []string{"key1", "key2", "key3", "key4"})
}

func (s *testServiceSuite) TestRPushX(c *C) {
	k := randomKey(c)
	s.checkInt(c, 1, "rpush", k, "key1")
	s.checkInt(c, 2, "rpushx", k, "key2")
	s.checkList(c, k, []string{"key1", "key2"})
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, 0, "rpushx", k, "key3")
	s.checkList(c, k, nil)
}

func (s *testServiceSuite) TestRPop(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "lpush", k, "key1", "key2", "key3")
	s.checkString(c, "key1", "rpop", k)
	s.checkString(c, "key2", "rpop", k)
	s.checkString(c, "key3", "rpop", k)
	s.checkList(c, k, nil)
}

func (s *testServiceSuite) TestLSet(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "lpush", k, "key1", "key2", "key3")
	s.checkOK(c, "lset", k, 0, "one")
	s.checkOK(c, "lset", k, 1, "two")
	s.checkOK(c, "lset", k, 2, "three")
	s.checkList(c, k, []string{"one", "two", "three"})
	s.checkOK(c, "lset", k, -1, "3")
	s.checkOK(c, "lset", k, -2, "2")
	s.checkOK(c, "lset", k, -3, "1")
	s.checkList(c, k, []string{"1", "2", "3"})
}

func (s *testServiceSuite) TestLIndex(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "lpush", k, "key1", "key2", "key3")
	s.checkString(c, "key1", "lindex", k, -1)
	s.checkString(c, "key2", "lindex", k, -2)
	s.checkString(c, "key3", "lindex", k, -3)
}

func (s *testServiceSuite) TestLLen(c *C) {
	k := randomKey(c)
	s.checkInt(c, 0, "llen", k)
	s.checkInt(c, 3, "lpush", k, "key1", "key2", "key3")
	s.checkInt(c, 3, "llen", k)
}

func (s *testServiceSuite) TestTrim(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "lpush", k, "key1", "key2", "key3")
	s.checkOK(c, "ltrim", k, 0, -1)
	s.checkInt(c, 3, "llen", k)
	s.checkOK(c, "ltrim", k, 0, -2)
	s.checkList(c, k, []string{"key3", "key2"})
	s.checkOK(c, "ltrim", k, 0, 0)
	s.checkList(c, k, []string{"key3"})
	s.checkOK(c, "ltrim", k, 1, 0)
	s.checkInt(c, 0, "llen", k)
}
