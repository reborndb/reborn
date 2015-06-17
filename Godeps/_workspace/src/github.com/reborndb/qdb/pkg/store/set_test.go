// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"strconv"

	"github.com/reborndb/go/redis/rdb"
	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) sdel(c *C, db uint32, key string, expect int64) {
	s.kdel(c, db, expect, key)
}

func (s *testStoreSuite) sdump(c *C, db uint32, key string, expect ...string) {
	s.kexists(c, db, key, 1)

	v, err := s.s.Dump(db, key)
	c.Assert(err, IsNil)
	c.Assert(v, NotNil)

	x, ok := v.(rdb.Set)
	c.Assert(ok, Equals, true)

	m := make(map[string]bool)
	for _, e := range expect {
		m[e] = true
	}
	for _, p := range x {
		c.Assert(m[string(p)], Equals, true)
	}
	s.scard(c, db, key, int64(len(m)))
}

func (s *testStoreSuite) srestore(c *C, db uint32, key string, ttlms int64, expect ...string) {
	var x rdb.Set
	for _, e := range expect {
		x = append(x, []byte(e))
	}
	dump, err := rdb.EncodeDump(x)
	c.Assert(err, IsNil)

	err = s.s.Restore(db, key, ttlms, dump)
	c.Assert(err, IsNil)

	s.sdump(c, db, key, expect...)
	if ttlms == 0 {
		s.kpttl(c, db, key, -1)
	} else {
		s.kpttl(c, db, key, int64(ttlms))
	}
}

func (s *testStoreSuite) sadd(c *C, db uint32, key string, expect int64, members ...string) {
	args := []interface{}{key}
	for _, m := range members {
		args = append(args, m)
	}

	x, err := s.s.SAdd(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	for _, m := range members {
		s.sismember(c, db, key, m, 1)
	}
}

func (s *testStoreSuite) scard(c *C, db uint32, key string, expect int64) {
	x, err := s.s.SCard(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect == 0 {
		s.kexists(c, db, key, 0)
	} else {
		s.kexists(c, db, key, 1)
	}
}

func (s *testStoreSuite) smembers(c *C, db uint32, key string, expect ...string) {
	x, err := s.s.SMembers(db, key)
	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	if len(expect) == 0 {
		s.kexists(c, db, key, 0)
		s.scard(c, db, key, 0)
	} else {
		m := make(map[string]bool)
		for _, e := range expect {
			m[e] = true
		}

		for _, b := range x {
			c.Assert(m[string(b)], Equals, true)
		}
		s.sdump(c, db, key, expect...)
	}
}

func (s *testStoreSuite) sismember(c *C, db uint32, key, member string, expect int64) {
	x, err := s.s.SIsMember(db, key, member)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) spop(c *C, db uint32, key string, expect int64) {
	x, err := s.s.SPop(db, key)
	c.Assert(err, IsNil)

	if expect == 0 {
		c.Assert(x, IsNil)
		s.kexists(c, db, key, 0)
	} else {
		s.sismember(c, db, key, string(x), 0)
	}
}

func (s *testStoreSuite) srandpop(c *C, db uint32, key string, expect int64) {
	x, err := s.s.SRandMember(db, key, 1)
	c.Assert(err, IsNil)

	if expect == 0 {
		c.Assert(len(x), Equals, 0)
		s.kexists(c, db, key, 0)
	} else {
		c.Assert(len(x), Equals, 1)
		member := string(x[0])
		s.sismember(c, db, key, member, 1)
		s.srem(c, db, key, 1, member)
	}
}

func (s *testStoreSuite) srem(c *C, db uint32, key string, expect int64, members ...string) {
	args := []interface{}{key}
	for _, m := range members {
		args = append(args, m)
	}

	x, err := s.s.SRem(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	for _, m := range members {
		s.sismember(c, db, key, m, 0)
	}
}

func (s *testStoreSuite) TestSRestore(c *C) {
	s.srestore(c, 0, "set", 100, "hello", "world")
	s.srestore(c, 0, "set", 0, "hello", "world", "!!")
	s.srestore(c, 0, "set", 100, "z")
	s.scard(c, 0, "set", 1)
	sleepms(200)
	s.scard(c, 0, "set", 0)
	s.kpttl(c, 0, "set", -2)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSAdd(c *C) {
	s.scard(c, 0, "set", 0)
	s.sadd(c, 0, "set", 1, "0")
	s.sadd(c, 0, "set", 1, "1")
	s.sadd(c, 0, "set", 1, "2")
	s.sadd(c, 0, "set", 0, "0", "1", "2")
	s.sdump(c, 0, "set", "1", "2", "0")

	s.kpexpire(c, 0, "set", 1000, 1)
	s.sadd(c, 0, "set", 1, "3", "2", "1", "0")
	s.kpttl(c, 0, "set", 1000)
	s.scard(c, 0, "set", 4)
	s.sdump(c, 0, "set", "0", "1", "2", "3")
	s.sadd(c, 0, "set", 0, "0", "1", "2", "3")
	s.sdel(c, 0, "set", 1)
	s.sdel(c, 0, "set", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSMembers(c *C) {
	s.sadd(c, 0, "set", 3, "0", "1", "2")
	s.smembers(c, 0, "set", "0", "1", "2")
	s.kpexpire(c, 0, "set", 100, 1)
	sleepms(200)
	s.smembers(c, 0, "set")
	s.kpexpire(c, 0, "set", 10, 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSRem(c *C) {
	s.sadd(c, 0, "set", 5, "x", "y", "0", "1", "2")
	s.srem(c, 0, "set", 1, "y", "y", "y")
	s.srem(c, 0, "set", 1, "x")
	s.srem(c, 0, "set", 0, "x")
	s.srem(c, 0, "set", 1, "0", "0", "x")
	s.sdump(c, 0, "set", "1", "2")
	s.srem(c, 0, "set", 2, "1", "2")
	s.scard(c, 0, "set", 0)
	s.kpttl(c, 0, "set", -2)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSPop(c *C) {
	for i := 0; i < 32; i++ {
		s.sadd(c, 0, "set", 1, strconv.Itoa(i))
	}
	for i := 0; i < 32; i++ {
		s.spop(c, 0, "set", 1)
	}
	s.spop(c, 0, "set", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSRandMember(c *C) {
	for i := 0; i < 32; i++ {
		s.sadd(c, 0, "set", 1, strconv.Itoa(i))
	}
	for i := 0; i < 32; i++ {
		s.srandpop(c, 0, "set", 1)
	}
	for i := 0; i < 32; i++ {
		s.sadd(c, 0, "set", 1, strconv.Itoa(i))
		s.srandpop(c, 0, "set", 1)
	}
	s.scard(c, 0, "set", 0)
	s.checkEmpty(c)
}
