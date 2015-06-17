// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) checkSet(c *C, key string, expect []string) {
	ay := s.checkBytesArray(c, "smembers", key)
	if expect == nil {
		c.Assert(ay, IsNil)
	} else {
		c.Assert(ay, HasLen, len(expect))
		m := make(map[string]bool)
		for _, s := range expect {
			m[s] = true
		}

		c.Assert(ay, HasLen, len(m))
		for _, v := range ay {
			c.Assert(m[string(v)], Equals, true)
		}
	}
}

func (s *testServiceSuite) TestSAdd(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "sadd", k, "key1", "key2", "key3")
	s.checkInt(c, 1, "sadd", k, "key1", "key2", "key3", "key4", "key4")
	s.checkSet(c, k, []string{"key1", "key2", "key3", "key4"})
	s.checkInt(c, -1, "ttl", k)
}

func (s *testServiceSuite) TestSRem(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "sadd", k, "key1", "key2", "key3")
	s.checkInt(c, 1, "srem", k, "key1", "key4")
	s.checkInt(c, 0, "srem", k, "key1", "key4")
	s.checkSet(c, k, []string{"key2", "key3"})
	s.checkInt(c, 2, "srem", k, "key2", "key3")
	s.checkInt(c, -2, "ttl", k)
	s.checkInt(c, 0, "srem", k, "key1")
	s.checkSet(c, k, nil)
}

func (s *testServiceSuite) TestSCard(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "sadd", k, "key1", "key2", "key3", "key1")
	s.checkInt(c, 3, "scard", k)
	s.checkInt(c, 1, "srem", k, "key1", "key4")
	s.checkInt(c, 2, "scard", k)
	s.checkInt(c, 0, "srem", k, "key1", "key4")
	s.checkInt(c, 2, "scard", k)
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, 0, "scard", k)
}

func (s *testServiceSuite) TestSIsMember(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "sadd", k, "key1", "key2", "key3")
	s.checkInt(c, 1, "sismember", k, "key1")
	s.checkInt(c, 0, "sismember", k, "key0")
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, 0, "sismember", k, "key1")
}

func (s *testServiceSuite) TestSPop(c *C) {
	k := randomKey(c)
	s.checkInt(c, 3, "sadd", k, "key1", "key2", "key3")
	for i := 2; i >= 0; i-- {
		s.checkDo(c, "spop", k)
		s.checkInt(c, int64(i), "scard", k)
	}
}

func (s *testServiceSuite) TestRandMember(c *C) {
	k := randomKey(c)
	var a [][]byte
	s.checkInt(c, 3, "sadd", k, "key1", "key2", "key3")
	a = s.checkBytesArray(c, "srandmember", k, 0)
	c.Assert(a, HasLen, 0)
	a = s.checkBytesArray(c, "srandmember", k, 100)
	c.Assert(a, HasLen, 3)
	m := make(map[string]bool)
	for _, v := range a {
		m[string(v)] = true
	}
	c.Assert(m["key1"], Equals, true)
	c.Assert(m["key2"], Equals, true)
	c.Assert(m["key3"], Equals, true)
}
