// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"math"

	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) kdel(c *C, db uint32, expect int64, keys ...string) {
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	n, err := s.s.Del(db, args...)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, expect)

	for _, key := range keys {
		s.kexists(c, db, key, 0)
	}
}

func (s *testStoreSuite) ktype(c *C, db uint32, key string, expect ObjectCode) {
	x, err := s.s.Type(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect == 0 {
		s.kexists(c, db, key, 0)
	} else {
		s.kexists(c, db, key, 1)
	}
}

func (s *testStoreSuite) kexists(c *C, db uint32, key string, expect int64) {
	x, err := s.s.Exists(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) kttl(c *C, db uint32, key string, expect int64) {
	x, err := s.s.TTL(db, key)
	switch expect {
	case -1, -2, 0:
		c.Assert(err, IsNil)
		c.Assert(x, Equals, expect)
	default:
		c.Assert(err, IsNil)
		c.Assert(math.Abs(float64(expect-x)) < 5, Equals, true)
	}
}

func (s *testStoreSuite) kpttl(c *C, db uint32, key string, expect int64) {
	x, err := s.s.PTTL(db, key)
	switch expect {
	case -1, -2, 0:
		c.Assert(err, IsNil)
		c.Assert(x, Equals, expect)
	default:
		c.Assert(err, IsNil)
		c.Assert(math.Abs(float64(expect-x)) < 50, Equals, true)
	}
}

func (s *testStoreSuite) kpersist(c *C, db uint32, key string, expect int64) {
	x, err := s.s.Persist(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect != 0 {
		s.kpttl(c, db, key, -1)
	}
}

func (s *testStoreSuite) kexpire(c *C, db uint32, key string, ttls uint64, expect int64) {
	x, err := s.s.Expire(db, key, ttls)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect != 0 {
		if ttls == 0 {
			s.kpttl(c, db, key, -2)
		} else {
			s.kpttl(c, db, key, int64(ttls*1e3))
		}
	}
}

func (s *testStoreSuite) kpexpire(c *C, db uint32, key string, ttlms uint64, expect int64) {
	x, err := s.s.PExpire(db, key, ttlms)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect != 0 {
		if ttlms == 0 {
			s.kpttl(c, db, key, -2)
		} else {
			s.kpttl(c, db, key, int64(ttlms))
		}
	}
}

func (s *testStoreSuite) kexpireat(c *C, db uint32, key string, timestamp uint64, expect int64) {
	x, err := s.s.ExpireAt(db, key, timestamp)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect != 0 {
		expireat := timestamp * 1e3
		if now := nowms(); expireat < now {
			s.kpttl(c, db, key, -2)
		} else {
			s.kpttl(c, db, key, int64(expireat-now))
		}
	}
}

func (s *testStoreSuite) kpexpireat(c *C, db uint32, key string, expireat uint64, expect int64) {
	x, err := s.s.PExpireAt(db, key, expireat)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect != 0 {
		if now := nowms(); expireat < now {
			s.kpttl(c, db, key, -2)
		} else {
			s.kpttl(c, db, key, int64(expireat-now))
		}
	}
}

func (s *testStoreSuite) TestDel(c *C) {
	s.kdel(c, 0, 0, "a", "b", "c", "d")
	s.xset(c, 0, "a", "a")
	s.xset(c, 0, "b", "b")
	s.kdel(c, 0, 2, "a", "b", "c", "d")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestExists(c *C) {
	s.kexists(c, 0, "a", 0)
	s.xset(c, 0, "a", "a")
	s.kexists(c, 0, "a", 1)
	s.kdel(c, 0, 1, "a")
	s.kexists(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestTTL(c *C) {
	s.kttl(c, 0, "a", -2)
	s.xset(c, 0, "a", "a")
	s.kttl(c, 0, "a", -1)

	s.kexpireat(c, 0, "a", nowms()/1e3+100, 1)
	s.kttl(c, 0, "a", 100)

	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	s.kttl(c, 0, "a", 0)

	s.kpexpireat(c, 0, "a", nowms()+100000, 1)
	s.kttl(c, 0, "a", 100)

	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	s.kttl(c, 0, "a", 0)
	s.kexists(c, 0, "a", 1)
	sleepms(200)
	s.kttl(c, 0, "a", -2)
	s.kexists(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestPTTL(c *C) {
	s.kpttl(c, 0, "a", -2)
	s.xset(c, 0, "a", "a")
	s.kpttl(c, 0, "a", -1)

	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	s.kpttl(c, 0, "a", 100)

	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	s.kpttl(c, 0, "a", 100)
	s.kexists(c, 0, "a", 1)

	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	sleepms(200)
	s.kpttl(c, 0, "a", -2)
	s.kexists(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestPersist(c *C) {
	s.kpersist(c, 0, "a", 0)
	s.xset(c, 0, "a", "a")
	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	s.kpersist(c, 0, "a", 1)

	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	sleepms(200)
	s.kpersist(c, 0, "a", 0)
	s.kexists(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestExpire(c *C) {
	s.kexpire(c, 0, "a", 10, 0)
	s.xset(c, 0, "a", "a")
	s.kexpire(c, 0, "a", 10, 1)
	s.kttl(c, 0, "a", 10)

	s.kexpire(c, 0, "a", 1, 1)
	s.kpttl(c, 0, "a", 1000)
	s.kpersist(c, 0, "a", 1)

	s.kexpireat(c, 0, "a", 0, 1)
	s.ktype(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestPExpire(c *C) {
	s.kpexpire(c, 0, "a", 10, 0)
	s.xset(c, 0, "a", "a")
	s.kpexpire(c, 0, "a", 100, 1)
	s.kttl(c, 0, "a", 0)
	s.kpttl(c, 0, "a", 100)

	s.kpexpire(c, 0, "a", 100, 1)
	sleepms(200)
	s.ktype(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestExpireAt(c *C) {
	s.kexpireat(c, 0, "a", nowms()/1e3+100, 0)
	s.xset(c, 0, "a", "a")
	s.kexpireat(c, 0, "a", nowms()/1e3+100, 1)
	s.ktype(c, 0, "a", StringCode)

	s.kexpireat(c, 0, "a", nowms()/1e3-20, 1)
	s.kexists(c, 0, "a", 0)

	s.xset(c, 0, "a", "a")
	s.kexpireat(c, 0, "a", 0, 1)
	s.kexists(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestPExpireAt(c *C) {
	s.kpexpireat(c, 0, "a", nowms()+100, 0)
	s.xset(c, 0, "a", "a")
	s.kpexpireat(c, 0, "a", nowms()+100, 1)
	s.ktype(c, 0, "a", StringCode)

	s.kpexpireat(c, 0, "a", nowms()-20, 1)
	s.kexists(c, 0, "a", 0)

	s.xset(c, 0, "a", "a")
	s.kpexpireat(c, 0, "a", 0, 1)
	s.kexists(c, 0, "a", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestRestore(c *C) {
	s.lpush(c, 0, "key", 1, "a")
	s.xrestore(c, 0, "key", 1000, "hello")
	s.hrestore(c, 0, "key", 2000, "a", "b", "b", "a")
	s.zrestore(c, 0, "key", 3000, "z0", 100, "z1", 1000)
	s.lrestore(c, 0, "key", 4000, "l0", "l1", "l2")
	s.srestore(c, 0, "key", 10, "a", "b", "c", "d")
	sleepms(30)
	s.kexists(c, 0, "key", 0)
	s.checkEmpty(c)
}
