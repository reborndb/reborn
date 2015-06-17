// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/reborndb/go/redis/rdb"
	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) zdel(c *C, db uint32, key string, expect int64) {
	s.kdel(c, db, expect, key)
}

func (s *testStoreSuite) zdump(c *C, db uint32, key string, expect ...interface{}) {
	s.kexists(c, db, key, 1)
	v, err := s.s.Dump(db, key)
	c.Assert(err, IsNil)
	c.Assert(v, NotNil)

	x, ok := v.(rdb.ZSet)
	c.Assert(ok, Equals, true)
	c.Assert(len(expect)%2, Equals, 0)

	m := make(map[string]float64)
	for i := 0; i < len(expect); i += 2 {
		score, err := ParseFloat(expect[i+1])
		c.Assert(err, IsNil)
		m[fmt.Sprint(expect[i])] = score
	}
	c.Assert(len(x), Equals, len(m))

	for _, e := range x {
		c.Assert(m[string(e.Member)], Equals, e.Score)
	}

	s.zcard(c, db, key, int64(len(m)))
	p, err := s.s.ZGetAll(db, key)
	c.Assert(err, IsNil)
	c.Assert(len(p), Equals, len(m)*2)

	for i := 0; i < len(p); i += 2 {
		s, err := ParseFloat(string(p[i+1]))
		c.Assert(err, IsNil)
		c.Assert((m[string(p[i])]-s) < 1e-9, Equals, true)
	}
}

func (s *testStoreSuite) zrestore(c *C, db uint32, key string, ttlms int64, expect ...interface{}) {
	c.Assert(len(expect)%2, Equals, 0)

	var x rdb.ZSet
	for i := 0; i < len(expect); i += 2 {
		score, err := ParseFloat(expect[i+1])
		c.Assert(err, IsNil)
		x = append(x, &rdb.ZSetElement{Member: []byte(fmt.Sprint(expect[i])), Score: float64(score)})
	}

	dump, err := rdb.EncodeDump(x)
	c.Assert(err, IsNil)

	err = s.s.Restore(db, key, ttlms, dump)
	c.Assert(err, IsNil)

	s.zdump(c, db, key, expect...)
	if ttlms == 0 {
		s.kpttl(c, db, key, -1)
	} else {
		s.kpttl(c, db, key, int64(ttlms))
	}
}

func (s *testStoreSuite) zcard(c *C, db uint32, key string, expect int64) {
	x, err := s.s.ZCard(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect == 0 {
		s.kexists(c, db, key, 0)
	} else {
		s.kexists(c, db, key, 1)
	}
}

func (s *testStoreSuite) zrem(c *C, db uint32, key string, expect int64, members ...string) {
	args := []interface{}{key}
	for _, m := range members {
		args = append(args, m)
	}
	x, err := s.s.ZRem(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zadd(c *C, db uint32, key string, expect int64, pairs ...interface{}) {
	args := []interface{}{key}
	for i := 0; i < len(pairs); i += 2 {
		args = append(args, pairs[i+1], pairs[i])
	}

	x, err := s.s.ZAdd(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	for i := 0; i < len(pairs); i += 2 {
		score, err := ParseFloat(pairs[i+1])
		c.Assert(err, IsNil)
		s.zscore(c, db, key, fmt.Sprint(pairs[i]), score)
	}
}

func (s *testStoreSuite) zscore(c *C, db uint32, key string, member string, expect float64) {
	x, ok, err := s.s.ZScore(db, key, member)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zincrby(c *C, db uint32, key string, member string, delta int64, expect float64) {
	x, err := s.s.ZIncrBy(db, key, delta, member)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zcount(c *C, db uint32, key string, min string, max string, expect int64) {
	x, err := s.s.ZCount(db, key, min, max)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zlexcount(c *C, db uint32, key string, min string, max string, expect int64) {
	x, err := s.s.ZLexCount(db, key, min, max)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zrange(c *C, db uint32, key string, start int64, stop int64, reverse bool, expect ...string) {
	var x [][]byte
	var err error

	if !reverse {
		x, err = s.s.ZRange(db, key, start, stop)
	} else {
		x, err = s.s.ZRevRange(db, key, start, stop)
	}

	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	for i, _ := range expect {
		c.Assert(string(x[i]), Equals, expect[i])
	}
}

func (s *testStoreSuite) zrangebylex(c *C, db uint32, key string, min string, max string, offset int64, count int64, reverse bool, expect ...string) {
	var x [][]byte
	var err error
	if !reverse {
		x, err = s.s.ZRangeByLex(db, key, min, max, "LIMIT", offset, count)
	} else {
		x, err = s.s.ZRevRangeByLex(db, key, min, max, "LIMIT", offset, count)
	}

	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	for i, _ := range expect {
		c.Assert(string(x[i]), Equals, expect[i])
	}
}

func (s *testStoreSuite) zrangebyscore(c *C, db uint32, key string, min string, max string, offset int64, count int64, reverse bool, expect ...string) {
	var x [][]byte
	var err error
	if !reverse {
		x, err = s.s.ZRangeByScore(db, key, min, max, "LIMIT", offset, count)
	} else {
		x, err = s.s.ZRevRangeByScore(db, key, min, max, "LIMIT", offset, count)

	}

	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(expect))

	for i, _ := range expect {
		c.Assert(string(x[i]), Equals, expect[i])
	}
}

func (s *testStoreSuite) zrank(c *C, db uint32, key string, member string, reverse bool, expect int64) {
	var x int64
	var err error
	if !reverse {
		x, err = s.s.ZRank(db, key, member)
	} else {
		x, err = s.s.ZRevRank(db, key, member)
	}

	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zremrangebylex(c *C, db uint32, key string, min string, max string, expect int64) {
	x, err := s.s.ZRemRangeByLex(db, key, min, max)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zremrangebyrank(c *C, db uint32, key string, start int64, stop int64, expect int64) {
	x, err := s.s.ZRemRangeByRank(db, key, start, stop)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) zremrangebyscore(c *C, db uint32, key string, min string, max string, expect int64) {
	x, err := s.s.ZRemRangeByScore(db, key, min, max)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) TestZAdd(c *C) {
	s.zadd(c, 0, "zset", 1, "0", 0)
	for i := 0; i < 32; i++ {
		s.zadd(c, 0, "zset", 1, strconv.Itoa(i), int64(i), strconv.Itoa(i+1), float64(i+1))
	}
	s.zcard(c, 0, "zset", 33)
	ms := []interface{}{}
	for i := 0; i <= 32; i++ {
		ms = append(ms, strconv.Itoa(i), int64(i))
	}
	s.zdump(c, 0, "zset", ms...)
	s.kpexpire(c, 0, "zset", 10, 1)
	sleepms(20)
	s.zdel(c, 0, "zset", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRem(c *C) {
	for i := 0; i < 32; i++ {
		s.zadd(c, 0, "zset", 1, strconv.Itoa(i), int64(i))
	}
	m := []string{}
	for i := -32; i < 32; i++ {
		m = append(m, strconv.Itoa(i))
	}
	s.zrem(c, 0, "zset", 32, append(m, m...)...)
	s.zcard(c, 0, "zset", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZIncrBy(c *C) {
	s.zincrby(c, 0, "zset", "a", 1, 1)
	s.zincrby(c, 0, "zset", "a", -1, 0)
	s.zdump(c, 0, "zset", "a", 0)
	s.zincrby(c, 0, "zset", "a", 1000, 1000)
	s.zcard(c, 0, "zset", 1)
	s.zdel(c, 0, "zset", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRestore(c *C) {
	ms := []interface{}{}
	for i := 0; i < 32; i++ {
		ms = append(ms, strconv.Itoa(i), i*i)
	}
	s.zrestore(c, 0, "zset", 0, ms...)
	s.zdump(c, 0, "zset", ms...)
	s.kpttl(c, 0, "zset", -1)

	for i := 0; i < len(ms); i += 2 {
		ms[i], ms[i+1] = strconv.Itoa(rand.Int()), rand.NormFloat64()
	}
	s.zrestore(c, 0, "zset", 100, ms...)
	s.zcard(c, 0, "zset", 32)
	sleepms(200)
	s.kpttl(c, 0, "zset", -2)
	s.zdel(c, 0, "zset", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZCount(c *C) {
	s.zadd(c, 0, "zset", 1, "0", 0)
	s.zadd(c, 0, "zset", 1, "1", 1.1)
	s.zadd(c, 0, "zset", 1, "2", 2.2)
	s.zadd(c, 0, "zset", 1, "3", 3.3)
	s.zadd(c, 0, "zset", 1, "-1", -1.1)
	s.zadd(c, 0, "zset", 1, "-2", -2.2)
	s.zadd(c, 0, "zset", 1, "-3", -3.3)

	s.zcount(c, 0, "zset", "0", "1.1", 2)
	s.zcount(c, 0, "zset", "(0", "1.1", 1)
	s.zcount(c, 0, "zset", "0", "(2.2", 2)
	s.zcount(c, 0, "zset", "-2.2", "-1.1", 2)
	s.zcount(c, 0, "zset", "(-2.2", "-1.1", 1)
	s.zcount(c, 0, "zset", "-3.3", "(-1.1", 2)
	s.zcount(c, 0, "zset", "2.2", "1.1", 0)
	s.zcount(c, 0, "zset", "-1.1", "-2.2", 0)
	s.zcount(c, 0, "zset", "-inf", "+inf", 7)
	s.zcount(c, 0, "zset", "0", "+inf", 4)
	s.zcount(c, 0, "zset", "-inf", "0", 4)
	s.zcount(c, 0, "zset", "+inf", "-inf", 0)
	s.zcount(c, 0, "zset", "+inf", "+inf", 0)
	s.zcount(c, 0, "zset", "-inf", "-inf", 0)

	s.zdel(c, 0, "zset", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZLexCount(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zlexcount(c, 0, "zset", "-", "+", 7)
	s.zlexcount(c, 0, "zset", "(a", "[c", 2)
	s.zlexcount(c, 0, "zset", "[b", "+", 6)
	s.zlexcount(c, 0, "zset", "(d", "(a", 0)
	s.zlexcount(c, 0, "zset", "+", "-", 0)
	s.zlexcount(c, 0, "zset", "+", "[c", 0)
	s.zlexcount(c, 0, "zset", "[c", "-", 0)
	s.zlexcount(c, 0, "zset", "[c", "[c", 1)
	s.zlexcount(c, 0, "zset", "+", "+", 0)
	s.zlexcount(c, 0, "zset", "-", "-", 0)

	s.zdel(c, 0, "zset", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRange(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)

	s.zrange(c, 0, "zset", 0, 3, false, "a", "b", "c")
	s.zrange(c, 0, "zset", 0, -1, false, "a", "b", "c")
	s.zrange(c, 0, "zset", 2, 3, false, "c")
	s.zrange(c, 0, "zset", -2, -1, false, "b", "c")
	s.zrange(c, 0, "zset", 2, 3, true, "a")
	s.zrange(c, 0, "zset", 0, -1, true, "c", "b", "a")

	s.zadd(c, 0, "zseu", 1, "a", 1)
	s.zrange(c, 0, "zset", -2, -1, true, "b", "a")

	s.zdel(c, 0, "zset", 1)
	s.zdel(c, 0, "zseu", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRangeByLex(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zrangebylex(c, 0, "zset", "-", "+", 0, 0, false)
	s.zrangebylex(c, 0, "zset", "-", "+", 0, 1, false, "a")
	s.zrangebylex(c, 0, "zset", "-", "(c", 0, -1, false, "a", "b")
	s.zrangebylex(c, 0, "zset", "[c", "+", 0, 2, false, "c", "d")
	s.zrangebylex(c, 0, "zset", "[c", "+", 1, 2, false, "d", "e")
	s.zrangebylex(c, 0, "zset", "[c", "-", 0, -1, true, "c", "b", "a")
	s.zrangebylex(c, 0, "zset", "(c", "-", 0, -1, true, "b", "a")
	s.zrangebylex(c, 0, "zset", "(g", "[aaa", 0, -1, true, "f", "e", "d", "c", "b")
	s.zrangebylex(c, 0, "zset", "(g", "[aaa", 1, -1, true, "e", "d", "c", "b")

	s.zdel(c, 0, "zset", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRangeByScore(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zrangebyscore(c, 0, "zset", "0", "7.7", 0, 2, false, "a", "b")
	s.zrangebyscore(c, 0, "zset", "-inf", "7.7", 0, 1, false, "a")
	s.zrangebyscore(c, 0, "zset", "-inf", "7.7", 1, 1, false, "b")
	s.zrangebyscore(c, 0, "zset", "(1.1", "(2.2", 1, 1, false)
	s.zrangebyscore(c, 0, "zset", "8.8", "9.9", 1, 1, false)

	s.zrangebyscore(c, 0, "zset", "+inf", "-inf", 0, 1, true, "g")
	s.zrangebyscore(c, 0, "zset", "3.3", "(1.1", 0, -1, true, "c", "b")
	s.zrangebyscore(c, 0, "zset", "(2.2", "(1.1", 0, -1, true)

	s.zdel(c, 0, "zset", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRank(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zrank(c, 0, "zset", "a", false, 0)
	s.zrank(c, 0, "zset", "aa", false, -1)
	s.zrank(c, 0, "zset", "d", false, 3)
	s.zrank(c, 0, "zset", "g", false, 6)
	s.zrank(c, 0, "zset", "abc", false, -1)
	s.zrank(c, 0, "zset_dummy", "a", false, -1)
	s.zrank(c, 0, "zset", "g", true, 0)
	s.zrank(c, 0, "zset", "a", true, 6)
	s.zrank(c, 0, "zset_dummy", "a", true, -1)
	s.zrank(c, 0, "zset", "abc", true, -1)

	s.zdel(c, 0, "zset", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRemRangeByLex(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zremrangebylex(c, 0, "zset", "[a", "(c", 2)
	s.zcard(c, 0, "zset", 5)
	s.zrangebylex(c, 0, "zset", "-", "+", 0, -1, false, "c", "d", "e", "f", "g")
	s.zremrangebylex(c, 0, "zset", "-", "+", 5)
	s.zcard(c, 0, "zset", 0)

	s.zdel(c, 0, "zset", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRemRangeByRank(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zremrangebyrank(c, 0, "zset", 0, 1, 2)
	s.zcard(c, 0, "zset", 5)
	s.zremrangebyrank(c, 0, "zset", 1, 2, 2)
	s.zrangebylex(c, 0, "zset", "-", "+", 0, -1, false, "c", "f", "g")
	s.zremrangebyrank(c, 0, "zset", 0, -1, 3)
	s.zcard(c, 0, "zset", 0)

	s.zdel(c, 0, "zset", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestZRemRangeByScore(c *C) {
	s.zadd(c, 0, "zset", 1, "a", 1.1)
	s.zadd(c, 0, "zset", 1, "b", 2.2)
	s.zadd(c, 0, "zset", 1, "c", 3.3)
	s.zadd(c, 0, "zset", 1, "d", 4.4)
	s.zadd(c, 0, "zset", 1, "e", 5.5)
	s.zadd(c, 0, "zset", 1, "f", 6.6)
	s.zadd(c, 0, "zset", 1, "g", 7.7)

	s.zremrangebyscore(c, 0, "zset", "1.1", "2.2", 2)
	s.zcard(c, 0, "zset", 5)
	s.zremrangebyscore(c, 0, "zset", "(3.3", "5.5", 2)
	s.zrangebylex(c, 0, "zset", "-", "+", 0, -1, false, "c", "f", "g")
	s.zremrangebyscore(c, 0, "zset", "-inf", "+inf", 3)
	s.zcard(c, 0, "zset", 0)

	s.zdel(c, 0, "zset", 0)
	s.checkEmpty(c)
}
