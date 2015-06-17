// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"math/rand"
	"strconv"

	"github.com/reborndb/go/redis/rdb"
	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) ldel(c *C, db uint32, key string, expect int64) {
	s.kdel(c, db, expect, key)
}

func (s *testStoreSuite) ldump(c *C, db uint32, key string, expect ...string) {
	s.kexists(c, db, key, 1)

	v, err := s.s.Dump(db, key)
	c.Assert(err, IsNil)
	c.Assert(v, NotNil)

	x, ok := v.(rdb.List)
	c.Assert(ok, Equals, true)
	c.Assert(len(x), Equals, len(expect))

	for i, v := range expect {
		c.Assert(v, Equals, string(x[i]))
	}
	for i, v := range expect {
		s.lindex(c, db, key, i, v)
	}
	s.llen(c, db, key, int64(len(expect)))
}

func (s *testStoreSuite) llen(c *C, db uint32, key string, expect int64) {
	x, err := s.s.LLen(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect == 0 {
		s.kexists(c, db, key, 0)
	} else {
		s.kexists(c, db, key, 1)
	}
}

func (s *testStoreSuite) lindex(c *C, db uint32, key string, index int, expect string) {
	x, err := s.s.LIndex(db, key, index)
	c.Assert(err, IsNil)

	if expect == "" {
		c.Assert(x, IsNil)
	} else {
		c.Assert(string(x), Equals, expect)
	}
}

func (s *testStoreSuite) lrange(c *C, db uint32, key string, beg, end int, expect ...string) {
	x, err := s.s.LRange(db, key, beg, end)
	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	for i, v := range expect {
		c.Assert(v, Equals, string(x[i]))
	}
}

func (s *testStoreSuite) lset(c *C, db uint32, key string, index int, value string) {
	err := s.s.LSet(db, key, index, value)
	c.Assert(err, IsNil)

	s.lrange(c, db, key, index, index, value)
	s.lindex(c, db, key, index, value)
}

func (s *testStoreSuite) ltrim(c *C, db uint32, key string, beg, end int) {
	err := s.s.LTrim(db, key, beg, end)
	c.Assert(err, IsNil)
}

func (s *testStoreSuite) lpop(c *C, db uint32, key string, expect string) {
	x, err := s.s.LPop(db, key)
	c.Assert(err, IsNil)

	if expect == "" {
		c.Assert(x, IsNil)
	} else {
		c.Assert(string(x), Equals, expect)
	}
}

func (s *testStoreSuite) rpop(c *C, db uint32, key string, expect string) {
	x, err := s.s.RPop(db, key)
	c.Assert(err, IsNil)

	if expect == "" {
		c.Assert(x, IsNil)
	} else {
		c.Assert(string(x), Equals, expect)
	}
}

func (s *testStoreSuite) lpush(c *C, db uint32, key string, expect int64, values ...string) {
	args := []interface{}{key}
	for _, v := range values {
		args = append(args, v)
	}

	x, err := s.s.LPush(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.llen(c, db, key, expect)
}

func (s *testStoreSuite) rpush(c *C, db uint32, key string, expect int64, values ...string) {
	args := []interface{}{key}
	for _, v := range values {
		args = append(args, v)
	}

	x, err := s.s.RPush(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.llen(c, db, key, expect)
}

func (s *testStoreSuite) lpushx(c *C, db uint32, key string, value string, expect int64) {
	x, err := s.s.LPushX(db, key, value)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.llen(c, db, key, expect)
}

func (s *testStoreSuite) rpushx(c *C, db uint32, key string, value string, expect int64) {
	x, err := s.s.RPushX(db, key, value)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.llen(c, db, key, expect)
}

func (s *testStoreSuite) lrestore(c *C, db uint32, key string, ttlms int64, expect ...string) {
	var x rdb.List
	for _, s := range expect {
		x = append(x, []byte(s))
	}

	dump, err := rdb.EncodeDump(x)
	c.Assert(err, IsNil)

	err = s.s.Restore(db, key, ttlms, dump)
	c.Assert(err, IsNil)

	s.ldump(c, db, key, expect...)
	if ttlms == 0 {
		s.kpttl(c, db, key, -1)
	} else {
		s.kpttl(c, db, key, int64(ttlms))
	}
}

func (s *testStoreSuite) TestLRestore(c *C) {
	s.lrestore(c, 0, "list", 0, "a", "b", "c")
	s.ldump(c, 0, "list", "a", "b", "c")
	s.kpttl(c, 0, "list", -1)
	s.llen(c, 0, "list", 3)

	s.lrestore(c, 0, "list", 100, "a1", "b1", "c1")
	s.llen(c, 0, "list", 3)
	sleepms(200)
	s.llen(c, 0, "list", 0)
	s.kpttl(c, 0, "list", -2)
	s.llen(c, 0, "list", 0)
	s.ldel(c, 0, "list", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestLIndex(c *C) {
	s.lindex(c, 0, "list", 0, "")
	s.lindex(c, 0, "list", 1, "")
	s.lindex(c, 0, "list", -1, "")
	s.llen(c, 0, "list", 0)

	s.lrestore(c, 0, "list", 0, "a", "b", "c")
	s.llen(c, 0, "list", 3)

	s.lindex(c, 0, "list", 0, "a")
	s.lindex(c, 0, "list", 1, "b")
	s.lindex(c, 0, "list", 2, "c")
	s.lindex(c, 0, "list", 3, "")

	s.lindex(c, 0, "list", -1, "c")
	s.lindex(c, 0, "list", -2, "b")
	s.lindex(c, 0, "list", -3, "a")
	s.lindex(c, 0, "list", -4, "")

	s.ldel(c, 0, "list", 1)
	for i := -4; i <= 4; i++ {
		s.lindex(c, 0, "list", i, "")
	}
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestLRange(c *C) {
	s.lrange(c, 0, "list", 0, 0)
	s.lrange(c, 0, "list", -1, 1)
	s.lrange(c, 0, "list", 1, -1)

	s.lrestore(c, 0, "list", 0, "a", "b", "c", "d")
	s.lrange(c, 0, "list", 0, 0, "a")
	s.lrange(c, 0, "list", 1, 2, "b", "c")
	s.lrange(c, 0, "list", 1, -1, "b", "c", "d")
	s.lrange(c, 0, "list", 2, -2, "c")
	s.lrange(c, 0, "list", -2, -3)
	s.lrange(c, 0, "list", -2, -1, "c", "d")
	s.lrange(c, 0, "list", -100, 2, "a", "b", "c")
	s.lrange(c, 0, "list", -1000, 1000, "a", "b", "c", "d")
	s.llen(c, 0, "list", 4)
	s.ldel(c, 0, "list", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestRPush(c *C) {
	ss := []string{}
	for i := 0; i < 32; i++ {
		sss := strconv.Itoa(i)
		ss = append(ss, sss)
		s.rpush(c, 0, "list", int64(len(ss)), sss)
	}
	for i := 0; i < 32; i++ {
		v := []string{}
		for j := 0; j < 4; j++ {
			v = append(v, strconv.Itoa(rand.Int()))
		}
		ss = append(ss, v...)
		s.rpush(c, 0, "list", int64(len(ss)), v...)
	}
	s.ldump(c, 0, "list", ss...)
	s.lrange(c, 0, "list", 0, -1, ss...)
	s.rpushx(c, 0, "list", "hello", int64(len(ss))+1)
	s.lindex(c, 0, "list", -1, "hello")
	s.ldel(c, 0, "list", 1)
	s.rpushx(c, 0, "list", "world", 0)
	s.llen(c, 0, "list", 0)
	s.kexists(c, 0, "list", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestLPush(c *C) {
	ss := []string{}
	for i := 0; i < 32; i++ {
		sss := strconv.Itoa(i)
		ss = append(ss, sss)
		s.lpush(c, 0, "list", int64(len(ss)), sss)
	}
	for i := 0; i < 32; i++ {
		v := []string{}
		for j := 0; j < 4; j++ {
			v = append(v, strconv.Itoa(rand.Int()))
		}
		ss = append(ss, v...)
		s.lpush(c, 0, "list", int64(len(ss)), v...)
	}
	for i, j := 0, len(ss)-1; i < j; i, j = i+1, j-1 {
		ss[i], ss[j] = ss[j], ss[i]
	}
	s.ldump(c, 0, "list", ss...)
	s.lrange(c, 0, "list", 0, -1, ss...)
	s.lpushx(c, 0, "list", "hello", int64(len(ss))+1)
	s.lindex(c, 0, "list", 0, "hello")
	s.ldel(c, 0, "list", 1)
	s.lpushx(c, 0, "list", "world", 0)
	s.llen(c, 0, "list", 0)
	s.kexists(c, 0, "list", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestLPop(c *C) {
	s.lpop(c, 0, "list", "")
	s.rpush(c, 0, "list", 4, "a", "b", "c", "d")
	s.lpop(c, 0, "list", "a")
	s.lpop(c, 0, "list", "b")
	s.lpop(c, 0, "list", "c")
	s.lpush(c, 0, "list", 2, "x")
	s.lpop(c, 0, "list", "x")
	s.lpop(c, 0, "list", "d")
	s.lpop(c, 0, "list", "")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestRPop(c *C) {
	s.rpop(c, 0, "list", "")
	s.lpush(c, 0, "list", 4, "a", "b", "c", "d")
	s.rpop(c, 0, "list", "a")
	s.rpop(c, 0, "list", "b")
	s.rpop(c, 0, "list", "c")
	s.rpush(c, 0, "list", 2, "x")
	s.rpop(c, 0, "list", "x")
	s.rpop(c, 0, "list", "d")
	s.rpop(c, 0, "list", "")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestLSet(c *C) {
	ss := []string{}
	for i := 0; i < 128; i++ {
		sss := strconv.Itoa(rand.Int())
		ss = append(ss, sss)
		s.rpush(c, 0, "list", int64(len(ss)), sss)
	}
	s.ldump(c, 0, "list", ss...)
	for i := 0; i < 128; i++ {
		ss[i] = strconv.Itoa(i * i)
		s.lset(c, 0, "list", i, ss[i])
	}
	s.ldump(c, 0, "list", ss...)
	s.ltrim(c, 0, "list", 1, 0)
	s.ldel(c, 0, "list", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestLTrim(c *C) {
	ss := []string{"a", "b", "c", "d"}
	s.rpush(c, 0, "list", int64(len(ss)), ss...)
	s.ltrim(c, 0, "list", 0, -1)
	s.ltrim(c, 0, "list", 0, len(ss)-1)
	s.ldump(c, 0, "list", ss...)

	s.ltrim(c, 0, "list", 1, -1)
	s.ldump(c, 0, "list", ss[1:]...)
	s.ltrim(c, 0, "list", 2, 1)
	s.llen(c, 0, "list", 0)

	s.rpush(c, 0, "list", int64(len(ss)), ss...)
	s.ltrim(c, 0, "list", -1, -1)
	s.ldump(c, 0, "list", ss[len(ss)-1:]...)
	s.ltrim(c, 0, "list", 2, 1)
	s.llen(c, 0, "list", 0)

	s.rpush(c, 0, "list", int64(len(ss)), ss...)
	s.ltrim(c, 0, "list", 1, -2)
	s.ldump(c, 0, "list", ss[1:len(ss)-1]...)
	s.ltrim(c, 0, "list", 2, 1)
	s.llen(c, 0, "list", 0)

	s.rpush(c, 0, "list", int64(len(ss)), ss...)
	s.ltrim(c, 0, "list", -100, 1000)
	s.ldump(c, 0, "list", ss...)
	s.ltrim(c, 0, "list", 2, 1)
	s.llen(c, 0, "list", 0)
	s.checkEmpty(c)
}
