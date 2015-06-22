// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"math"
	"math/rand"
	"strconv"

	"github.com/reborndb/go/redis/rdb"
	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) hdelall(c *C, db uint32, key string, expect int64) {
	s.kdel(c, db, expect, key)
}

func (s *testStoreSuite) hdump(c *C, db uint32, key string, expect ...string) {
	s.kexists(c, db, key, 1)

	v, err := s.s.Dump(db, FormatBytes(key))
	c.Assert(err, IsNil)
	c.Assert(v, NotNil)

	x, ok := v.(rdb.Hash)
	c.Assert(ok, Equals, true)
	c.Assert(len(expect)%2, Equals, 0)

	m := make(map[string]string)
	for i := 0; i < len(expect); i += 2 {
		m[expect[i]] = expect[i+1]
	}

	for _, e := range x {
		c.Assert(m[string(e.Field)], Equals, string(e.Value))
	}

	for k, v := range m {
		s.hget(c, db, key, k, v)
	}
	s.hlen(c, db, key, int64(len(m)))
}

func (s *testStoreSuite) hrestore(c *C, db uint32, key string, ttlms int64, expect ...string) {
	c.Assert(len(expect)%2, Equals, 0)

	var x rdb.Hash
	for i := 0; i < len(expect); i += 2 {
		x = append(x, &rdb.HashElement{Field: []byte(expect[i]), Value: []byte(expect[i+1])})
	}

	dump, err := rdb.EncodeDump(x)
	c.Assert(err, IsNil)

	err = s.s.Restore(db, FormatBytes(key, ttlms, dump))
	c.Assert(err, IsNil)

	s.hdump(c, db, key, expect...)
	if ttlms == 0 {
		s.kpttl(c, db, key, -1)
	} else {
		s.kpttl(c, db, key, int64(ttlms))
	}
}

func (s *testStoreSuite) hlen(c *C, db uint32, key string, expect int64) {
	x, err := s.s.HLen(db, FormatBytes(key))
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect == 0 {
		s.kexists(c, db, key, 0)
	} else {
		s.kexists(c, db, key, 1)
	}
}

func (s *testStoreSuite) hdel(c *C, db uint32, key string, expect int64, fields ...string) {
	args := []interface{}{key}
	for _, f := range fields {
		args = append(args, f)
	}

	x, err := s.s.HDel(db, FormatBytes(args...))
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	for _, f := range fields {
		s.hexists(c, db, key, f, 0)
	}
}

func (s *testStoreSuite) hexists(c *C, db uint32, key, field string, expect int64) {
	x, err := s.s.HExists(db, FormatBytes(key, field))
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) hgetall(c *C, db uint32, key string, expect ...string) {
	x, err := s.s.HGetAll(db, FormatBytes(key))
	c.Assert(err, IsNil)

	if len(expect) == 0 {
		c.Assert(len(x), Equals, 0)
		s.kexists(c, db, key, 0)
	} else {
		c.Assert(len(expect)%2, Equals, 0)
		c.Assert(len(x), Equals, len(expect))

		m := make(map[string]string)
		for i := 0; i < len(expect); i += 2 {
			m[expect[i]] = expect[i+1]
		}

		fields, values := []string{}, []string{}
		for i := 0; i < len(x); i += 2 {
			f, v := string(x[i]), string(x[i+1])
			c.Assert(m[f], Equals, v)
			s.hget(c, db, key, f, v)
			fields = append(fields, f)
			values = append(values, v)
		}
		s.hdump(c, db, key, expect...)
		s.hkeys(c, db, key, fields...)
		s.hvals(c, db, key, values...)
	}
}

func (s *testStoreSuite) hget(c *C, db uint32, key, field string, expect string) {
	x, err := s.s.HGet(db, FormatBytes(key, field))
	c.Assert(err, IsNil)

	if expect == "" {
		c.Assert(x, IsNil)
		s.hexists(c, db, key, field, 0)
	} else {
		c.Assert(string(x), Equals, expect)
		s.hexists(c, db, key, field, 1)
	}
}

func (s *testStoreSuite) hkeys(c *C, db uint32, key string, expect ...string) {
	x, err := s.s.HKeys(db, FormatBytes(key))
	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	if len(expect) == 0 {
		s.kexists(c, db, key, 0)
		s.hlen(c, db, key, 0)
	} else {
		m := make(map[string]bool)
		for _, e := range expect {
			m[e] = true
		}

		for _, b := range x {
			c.Assert(m[string(b)], Equals, true)
		}

		for _, e := range expect {
			s.hexists(c, db, key, e, 1)
		}
		s.hlen(c, db, key, int64(len(expect)))
	}
}

func (s *testStoreSuite) hvals(c *C, db uint32, key string, expect ...string) {
	x, err := s.s.HVals(db, FormatBytes(key))
	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	if len(expect) == 0 {
		s.kexists(c, db, key, 0)
		s.hlen(c, db, key, 0)
	} else {
		m1 := make(map[string]int)
		for _, e := range expect {
			m1[e]++
		}
		m2 := make(map[string]int)
		for _, b := range x {
			m2[string(b)]++
		}
		c.Assert(len(m1), Equals, len(m2))

		for k, v := range m2 {
			c.Assert(m1[k], Equals, v)
		}
		s.hlen(c, db, key, int64(len(expect)))
	}
}

func (s *testStoreSuite) hincrby(c *C, db uint32, key, field string, delta int64, expect int64) {
	x, err := s.s.HIncrBy(db, FormatBytes(key, field, delta))
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) hincrbyfloat(c *C, db uint32, key, field string, delta float64, expect float64) {
	x, err := s.s.HIncrByFloat(db, FormatBytes(key, field, delta))
	c.Assert(err, IsNil)
	c.Assert(math.Abs(x-expect) < 1e-9, Equals, true)
}

func (s *testStoreSuite) hset(c *C, db uint32, key, field, value string, expect int64) {
	x, err := s.s.HSet(db, FormatBytes(key, field, value))
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.hget(c, db, key, field, value)
}

func (s *testStoreSuite) hsetnx(c *C, db uint32, key, field, value string, expect int64) {
	x, err := s.s.HSetNX(db, FormatBytes(key, field, value))
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.hexists(c, db, key, field, 1)
	if expect != 0 {
		s.hget(c, db, key, field, value)
	}
}

func (s *testStoreSuite) hmset(c *C, db uint32, key string, pairs ...string) {
	c.Assert(len(pairs)%2, Equals, 0)

	args := []interface{}{key}
	for i := 0; i < len(pairs); i++ {
		args = append(args, pairs[i])
	}

	err := s.s.HMSet(db, FormatBytes(args...))
	c.Assert(err, IsNil)

	for i := 0; i < len(pairs); i += 2 {
		s.hget(c, db, key, pairs[i], pairs[i+1])
	}
}

func (s *testStoreSuite) hmget(c *C, db uint32, key string, pairs ...string) {
	c.Assert(len(pairs)%2, Equals, 0)

	args := []interface{}{key}
	for i := 0; i < len(pairs); i += 2 {
		args = append(args, pairs[i])
	}

	x, err := s.s.HMGet(db, FormatBytes(args...))
	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(pairs)/2)

	for i, b := range x {
		v := pairs[i*2+1]
		if len(v) == 0 {
			c.Assert(b, IsNil)
		} else {
			c.Assert(string(b), Equals, v)
		}
	}
}

func (s *testStoreSuite) TestHSet(c *C) {
	ss := []string{}
	ks := []string{}
	vs := []string{}
	for i := 0; i < 32; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(rand.Int())
		ss = append(ss, k, v)
		ks = append(ks, k)
		vs = append(vs, v)
		s.hset(c, 0, "hash", k, strconv.Itoa(rand.Int()), 1)
		s.hset(c, 0, "hash", k, v, 0)
		s.hget(c, 0, "hash", k, v)
	}
	s.hkeys(c, 0, "hash", ks...)
	s.hvals(c, 0, "hash", vs...)
	s.hgetall(c, 0, "hash", ss...)
	s.hdelall(c, 0, "hash", 1)
	s.hgetall(c, 0, "hash")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestHDel(c *C) {
	ss := []string{}
	for i := 0; i < 32; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(rand.Int())
		ss = append(ss, k, v)
	}
	s.hmset(c, 0, "hash", ss...)
	s.hgetall(c, 0, "hash", ss...)

	s.hdel(c, 0, "hash", 2, "0", "1")
	s.hdel(c, 0, "hash", 1, "2", "2", "2")
	s.hdel(c, 0, "hash", 0, "0", "1", "2", "0", "1", "2")

	s.hlen(c, 0, "hash", int64(len(ss)/2)-3)
	s.hgetall(c, 0, "hash", ss[6:]...)
	s.kpexpire(c, 0, "hash", 100, 1)
	sleepms(200)
	s.hdelall(c, 0, "hash", 0)

	for i := 0; i < 10; i++ {
		s.hset(c, 0, "hash", strconv.Itoa(i), strconv.Itoa(rand.Int()), 1)
	}
	for i := 0; i < 10; i++ {
		s.hdel(c, 0, "hash", 1, strconv.Itoa(i))
		s.hdel(c, 0, "hash", 0, strconv.Itoa(i))
	}
	s.hgetall(c, 0, "hash")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestHRestore(c *C) {
	ss := []string{}
	for i := 0; i < 32; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(rand.Int())
		ss = append(ss, k, v)
	}

	s.hrestore(c, 0, "hash", 0, ss...)
	s.hgetall(c, 0, "hash", ss...)
	s.kpttl(c, 0, "hash", -1)

	for i := 0; i < len(ss); i++ {
		ss[i] = strconv.Itoa(rand.Int())
	}

	s.hrestore(c, 0, "hash", 100, ss...)
	s.hgetall(c, 0, "hash", ss...)
	sleepms(200)
	s.hlen(c, 0, "hash", 0)
	s.kpttl(c, 0, "hash", -2)
	s.hdelall(c, 0, "hash", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestHIncrBy(c *C) {
	s.hincrby(c, 0, "hash", "a", 100, 100)
	s.hincrby(c, 0, "hash", "a", -100, 0)
	s.hset(c, 0, "hash", "a", "1000", 0)
	s.hincrby(c, 0, "hash", "a", -1000, 0)
	s.hgetall(c, 0, "hash", "a", "0")
	s.hdelall(c, 0, "hash", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestHIncrFloat(c *C) {
	s.hincrbyfloat(c, 0, "hash", "a", 100.5, 100.5)
	s.hincrbyfloat(c, 0, "hash", "a", 10000, 10100.5)
	s.hset(c, 0, "hash", "a", "300", 0)
	s.hincrbyfloat(c, 0, "hash", "a", 3.14, 303.14)
	s.hincrbyfloat(c, 0, "hash", "a", -303.14, 0)

	_, err := s.s.HIncrByFloat(0, FormatBytes("hash", "a", math.Inf(1)))
	c.Assert(err, NotNil)

	_, err = s.s.HIncrByFloat(0, FormatBytes("hash", "a", math.Inf(-1)))
	c.Assert(err, NotNil)

	s.hdelall(c, 0, "hash", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestHSetNX(c *C) {
	for i := 0; i < 16; i++ {
		k := strconv.Itoa(i)
		v := strconv.Itoa(rand.Int())
		s.hsetnx(c, 0, "hash", k, v, 1)
	}
	s.hsetnx(c, 0, "hash", "0", "0", 0)
	s.hsetnx(c, 0, "hash", "128", "128", 1)
	s.hsetnx(c, 0, "hash", "129", "129", 1)
	s.hsetnx(c, 0, "hash", "129", "129", 0)
	s.hlen(c, 0, "hash", 18)

	s.kpexpire(c, 0, "hash", 100, 1)
	sleepms(200)
	s.hsetnx(c, 0, "hash", "0", "1", 1)
	s.hsetnx(c, 0, "hash", "0", "2", 0)
	s.hdel(c, 0, "hash", 1, "0")
	s.hsetnx(c, 0, "hash", "0", "3", 1)
	s.hdel(c, 0, "hash", 1, "0")
	s.hlen(c, 0, "hash", 0)

	s.hsetnx(c, 0, "hash", "0", "a", 1)
	s.hsetnx(c, 0, "hash", "0", "b", 0)
	s.hsetnx(c, 0, "hash", "1", "c", 1)
	s.hsetnx(c, 0, "hash", "1", "d", 0)
	s.hsetnx(c, 0, "hash", "2", "a", 1)
	s.hsetnx(c, 0, "hash", "2", "c", 0)
	s.hvals(c, 0, "hash", "a", "a", "c")
	s.hkeys(c, 0, "hash", "0", "1", "2")

	s.hdel(c, 0, "hash", 1, "0")
	s.hsetnx(c, 0, "hash", "0", "x", 1)
	s.hlen(c, 0, "hash", 3)
	s.hmget(c, 0, "hash", "0", "x", "1", "c", "2", "a")
	s.hdelall(c, 0, "hash", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestHMSet(c *C) {
	s.hset(c, 0, "hash", "a", "0", 1)
	s.hmset(c, 0, "hash", "b", "1", "c", "2")
	s.hmget(c, 0, "hash", "a", "0", "a", "0", "x", "")
	s.hdel(c, 0, "hash", 1, "a")
	s.hmget(c, 0, "hash", "a", "", "b", "1")
	s.hgetall(c, 0, "hash", "b", "1", "c", "2")
	s.hdelall(c, 0, "hash", 1)
	s.hmget(c, 0, "hash", "a", "")
	s.checkEmpty(c)
}
