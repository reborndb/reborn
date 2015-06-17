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

func (s *testStoreSuite) xdel(c *C, db uint32, key string, expect int64) {
	s.kdel(c, db, expect, key)
}

func (s *testStoreSuite) xdump(c *C, db uint32, key string, expect string) {
	s.kexists(c, db, key, 1)

	v, err := s.s.Dump(db, key)
	c.Assert(err, IsNil)
	c.Assert(v, NotNil)

	x, ok := v.(rdb.String)
	c.Assert(ok, Equals, true)
	c.Assert(string([]byte(x)), Equals, expect)

	s.xstrlen(c, db, key, int64(len(expect)))
}

func (s *testStoreSuite) xrestore(c *C, db uint32, key string, ttlms uint64, value string) {
	var x rdb.String = []byte(value)
	dump, err := rdb.EncodeDump(x)
	c.Assert(err, IsNil)

	err = s.s.Restore(db, key, ttlms, dump)
	c.Assert(err, IsNil)

	s.xdump(c, db, key, value)
	if ttlms == 0 {
		s.kpttl(c, db, key, -1)
	} else {
		s.kpttl(c, db, key, int64(ttlms))
	}
}

func (s *testStoreSuite) xset(c *C, db uint32, key, value string) {
	err := s.s.Set(db, []byte(key), []byte(value))
	c.Assert(err, IsNil)

	s.kttl(c, db, key, -1)
	s.xget(c, db, key, value)
}

func (s *testStoreSuite) xget(c *C, db uint32, key string, expect string) {
	x, err := s.s.Get(db, []byte(key))
	c.Assert(err, IsNil)

	if expect == "" {
		c.Assert(x, IsNil)
		s.xstrlen(c, db, key, 0)
	} else {
		c.Assert(string(x), Equals, expect)
		s.xstrlen(c, db, key, int64(len(expect)))
	}
}

func (s *testStoreSuite) xappend(c *C, db uint32, key, value string, expect int64) {
	x, err := s.s.Append(db, key, value)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xgetset(c *C, db uint32, key, value string, expect string) {
	x, err := s.s.GetSet(db, key, value)
	c.Assert(err, IsNil)

	if expect == "" {
		c.Assert(x, IsNil)
	} else {
		c.Assert(string(x), Equals, expect)
	}

	s.kttl(c, db, key, -1)
}

func (s *testStoreSuite) xpsetex(c *C, db uint32, key, value string, ttlms uint64) {
	err := s.s.PSetEX(db, key, ttlms, value)
	c.Assert(err, IsNil)

	s.xdump(c, db, key, value)
	s.kpttl(c, db, key, int64(ttlms))
}

func (s *testStoreSuite) xsetex(c *C, db uint32, key, value string, ttls uint64) {
	err := s.s.SetEX(db, key, ttls, value)
	c.Assert(err, IsNil)

	s.xdump(c, db, key, value)
	s.kpttl(c, db, key, int64(ttls*1e3))
}

func (s *testStoreSuite) xsetnx(c *C, db uint32, key, value string, expect int64) {
	x, err := s.s.SetNX(db, key, value)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect != 0 {
		s.xdump(c, db, key, value)
		s.kpttl(c, db, key, -1)
	}
}

func (s *testStoreSuite) xstrlen(c *C, db uint32, key string, expect int64) {
	if expect != 0 {
		s.kexists(c, db, key, 1)
	} else {
		s.kexists(c, db, key, 0)
	}

	x, err := s.s.Strlen(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xincr(c *C, db uint32, key string, expect int64) {
	x, err := s.s.Incr(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xdecr(c *C, db uint32, key string, expect int64) {
	x, err := s.s.Decr(db, key)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xincrby(c *C, db uint32, key string, delta int64, expect int64) {
	x, err := s.s.IncrBy(db, key, delta)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xdecrby(c *C, db uint32, key string, delta int64, expect int64) {
	x, err := s.s.DecrBy(db, key, delta)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xincrbyfloat(c *C, db uint32, key string, delta float64, expect float64) {
	x, err := s.s.IncrByFloat(db, key, delta)
	c.Assert(err, IsNil)
	c.Assert(math.Abs(x-expect) < 1e-9, Equals, true)
}

func (s *testStoreSuite) xsetbit(c *C, db uint32, key string, offset uint, value int64, expect int64) {
	x, err := s.s.SetBit(db, key, offset, value)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.xgetbit(c, db, key, offset, value)
}

func (s *testStoreSuite) xgetbit(c *C, db uint32, key string, offset uint, expect int64) {
	x, err := s.s.GetBit(db, key, offset)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)
}

func (s *testStoreSuite) xsetrange(c *C, db uint32, key string, offset uint, value string, expect int64) {
	x, err := s.s.SetRange(db, key, offset, value)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	s.xgetrange(c, db, key, int(offset), int(offset)+len(value)-1, value)
}

func (s *testStoreSuite) xgetrange(c *C, db uint32, key string, beg, end int, expect string) {
	x, err := s.s.GetRange(db, key, beg, end)
	c.Assert(err, IsNil)
	c.Assert(string(x), Equals, expect)
}

func (s *testStoreSuite) xmset(c *C, db uint32, pairs ...string) {
	args := make([]interface{}, len(pairs))
	for i, s := range pairs {
		args[i] = s
	}

	err := s.s.MSet(db, args...)
	c.Assert(err, IsNil)

	m := make(map[string]string)
	for i := 0; i < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	for key, value := range m {
		s.xget(c, db, key, value)
		s.kttl(c, db, key, -1)
	}
}

func (s *testStoreSuite) xmsetnx(c *C, db uint32, expect int64, pairs ...string) {
	args := make([]interface{}, len(pairs))
	for i, s := range pairs {
		args[i] = s
	}

	x, err := s.s.MSetNX(db, args...)
	c.Assert(err, IsNil)
	c.Assert(x, Equals, expect)

	if expect == 0 {
		return
	}

	m := make(map[string]string)
	for i := 0; i < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	for key, value := range m {
		s.xget(c, db, key, value)
		s.kttl(c, db, key, -1)
	}
}

func (s *testStoreSuite) xmget(c *C, db uint32, pairs ...string) {
	c.Assert(len(pairs)%2, Equals, 0)

	var args []interface{}
	for i := 0; i < len(pairs); i += 2 {
		args = append(args, pairs[i])
	}

	x, err := s.s.MGet(db, args...)
	c.Assert(err, IsNil)
	c.Assert(len(x), Equals, len(args))

	for i := 0; i < len(pairs); i += 2 {
		value := pairs[i+1]
		if value == "" {
			c.Assert(x[i/2], IsNil)
		} else {
			c.Assert(string(x[i/2]), Equals, value)
		}
	}
}

func (s *testStoreSuite) TestXRestore(c *C) {
	s.xrestore(c, 0, "string", 0, "hello")
	s.xrestore(c, 0, "string", 0, "world")
	s.xget(c, 0, "string", "world")
	s.xrestore(c, 0, "string", 10, "hello")
	sleepms(50)
	s.kpttl(c, 0, "string", -2)

	s.xrestore(c, 0, "string", 100, "test")
	s.xget(c, 0, "string", "test")
	sleepms(200)
	s.xget(c, 0, "string", "")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXSet(c *C) {
	s.xset(c, 0, "string", "hello")
	s.xdel(c, 0, "string", 1)
	s.xdel(c, 0, "string", 0)
	s.xget(c, 0, "string", "")

	s.kpexpire(c, 0, "string", 100, 0)
	s.kpttl(c, 0, "string", -2)

	s.xset(c, 0, "string", "test")
	s.kpttl(c, 0, "string", -1)
	s.kpexpire(c, 0, "string", 1000, 1)
	s.kpexpire(c, 0, "string", 2000, 1)

	s.xset(c, 0, "string", "test")
	s.kpersist(c, 0, "string", 0)
	s.kpexpire(c, 0, "string", 1000, 1)
	s.kpersist(c, 0, "string", 1)

	s.xset(c, 0, "string", "test2")
	s.xdel(c, 0, "string", 1)
	s.kpttl(c, 0, "string", -2)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXAppend(c *C) {
	s.xset(c, 0, "string", "hello")
	s.xget(c, 0, "string", "hello")
	s.xappend(c, 0, "string", " ", 6)

	s.xget(c, 0, "string", "hello ")
	s.xappend(c, 0, "string", "world!!", 13)

	s.xget(c, 0, "string", "hello world!!")
	s.xdel(c, 0, "string", 1)
	s.xget(c, 0, "string", "")

	s.xappend(c, 0, "string", "test", 4)
	s.xget(c, 0, "string", "test")

	s.xdel(c, 0, "string", 1)

	expect := ""
	for i := 0; i < 1024; i++ {
		ss := strconv.Itoa(i) + ","
		expect += ss
		s.xappend(c, 0, "string", ss, int64(len(expect)))
	}
	s.xdump(c, 0, "string", expect)
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXSetEX(c *C) {
	s.xsetex(c, 0, "string", "hello", 1)
	s.kpttl(c, 0, "string", 1000)

	s.xset(c, 0, "string", "hello")
	s.kpttl(c, 0, "string", -1)

	s.xsetex(c, 0, "string", "world", 100)
	s.xget(c, 0, "string", "world")
	s.kpttl(c, 0, "string", 100000)
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXPSetEX(c *C) {
	s.xpsetex(c, 0, "string", "hello", 1000)
	s.kpttl(c, 0, "string", 1000)
	s.xpsetex(c, 0, "string", "world", 2000)
	s.kpttl(c, 0, "string", 2000)
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXSetNX(c *C) {
	s.xset(c, 0, "string", "hello")

	s.xsetnx(c, 0, "string", "world", 0)
	s.xget(c, 0, "string", "hello")
	s.xdel(c, 0, "string", 1)

	s.xsetnx(c, 0, "string", "world", 1)
	s.xdel(c, 0, "string", 1)
	s.xdel(c, 0, "string", 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXGetSet(c *C) {
	s.xgetset(c, 0, "string", "hello", "")
	s.xget(c, 0, "string", "hello")
	s.kpttl(c, 0, "string", -1)

	s.kpexpire(c, 0, "string", 1000, 1)
	s.xgetset(c, 0, "string", "world", "hello")
	s.kpttl(c, 0, "string", -1)

	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXIncrDecr(c *C) {
	for i := 0; i < 32; i++ {
		s.xincr(c, 0, "string", int64(i)+1)
	}
	s.xget(c, 0, "string", "32")

	s.kpexpire(c, 0, "string", 10000, 1)
	for i := 0; i < 32; i++ {
		s.xdecr(c, 0, "string", 31-int64(i))
	}
	s.xget(c, 0, "string", "0")
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXIncrBy(c *C) {
	sum := int64(0)
	for i := 0; i < 32; i++ {
		a := rand.Int63()
		sum += a
		s.xincrby(c, 0, "string", a, sum)
	}
	for i := 0; i < 32; i++ {
		a := rand.Int63()
		sum -= a
		s.xdecrby(c, 0, "string", a, sum)
	}
	s.xget(c, 0, "string", strconv.Itoa(int(sum)))
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)

}

func (s *testStoreSuite) TestXIncrByFloat(c *C) {
	sum := float64(0)
	for i := 0; i < 128; i++ {
		a := rand.Float64()
		sum += a
		s.xincrbyfloat(c, 0, "string", a, sum)
	}

	_, err := s.s.IncrByFloat(0, "string", math.Inf(1))
	c.Assert(err, NotNil)

	_, err = s.s.IncrByFloat(0, "string", math.Inf(-1))
	c.Assert(err, NotNil)

	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXSetBit(c *C) {
	s.xsetbit(c, 0, "string", 0, 1, 0)
	s.xget(c, 0, "string", "\x01")
	s.xsetbit(c, 0, "string", 1, 1, 0)
	s.xget(c, 0, "string", "\x03")
	s.xsetbit(c, 0, "string", 2, 1, 0)
	s.xget(c, 0, "string", "\x07")
	s.xsetbit(c, 0, "string", 3, 1, 0)
	s.xget(c, 0, "string", "\x0f")
	s.xsetbit(c, 0, "string", 4, 1, 0)
	s.xget(c, 0, "string", "\x1f")
	s.xsetbit(c, 0, "string", 5, 1, 0)
	s.xget(c, 0, "string", "\x3f")
	s.xsetbit(c, 0, "string", 6, 1, 0)
	s.xget(c, 0, "string", "\x7f")
	s.xsetbit(c, 0, "string", 7, 1, 0)
	s.xget(c, 0, "string", "\xff")
	s.xsetbit(c, 0, "string", 8, 1, 0)
	s.xget(c, 0, "string", "\xff\x01")
	s.xsetbit(c, 0, "string", 0, 0, 1)
	s.xget(c, 0, "string", "\xfe\x01")
	s.xsetbit(c, 0, "string", 1, 0, 1)
	s.xget(c, 0, "string", "\xfc\x01")
	s.xsetbit(c, 0, "string", 2, 0, 1)
	s.xget(c, 0, "string", "\xf8\x01")
	s.xsetbit(c, 0, "string", 3, 0, 1)
	s.xget(c, 0, "string", "\xf0\x01")
	s.xsetbit(c, 0, "string", 4, 0, 1)
	s.xget(c, 0, "string", "\xe0\x01")
	s.xsetbit(c, 0, "string", 5, 0, 1)
	s.xget(c, 0, "string", "\xc0\x01")
	s.xsetbit(c, 0, "string", 6, 0, 1)
	s.xget(c, 0, "string", "\x80\x01")
	s.xsetbit(c, 0, "string", 7, 0, 1)
	s.xget(c, 0, "string", "\x00\x01")
	s.xsetbit(c, 0, "string", 8, 0, 1)
	s.xget(c, 0, "string", "\x00\x00")
	s.xsetbit(c, 0, "string", 16, 0, 0)
	s.xget(c, 0, "string", "\x00\x00\x00")
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXSetRange(c *C) {
	s.xsetrange(c, 0, "string", 1, "hello", 6)
	s.xget(c, 0, "string", "\x00hello")
	s.xsetrange(c, 0, "string", 7, "world", 12)
	s.xget(c, 0, "string", "\x00hello\x00world")
	s.xsetrange(c, 0, "string", 2, "test1test2test3", 17)
	s.xget(c, 0, "string", "\x00htest1test2test3")
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestGetBit(c *C) {
	s.xgetbit(c, 0, "string", 0, 0)
	s.xgetbit(c, 0, "string", 1000, 0)
	s.xset(c, 0, "string", "\x01\x03")
	s.xgetbit(c, 0, "string", 0, 1)
	s.xgetbit(c, 0, "string", 1, 0)
	s.xgetbit(c, 0, "string", 8, 1)
	s.xgetbit(c, 0, "string", 9, 1)
	s.xdel(c, 0, "string", 1)

	for i := 0; i < 32; i += 2 {
		s.xsetbit(c, 0, "string", uint(i), 1, 0)
		s.xsetbit(c, 0, "string", uint(i), 1, 1)
	}
	for i := 0; i < 32; i++ {
		v := int64(1)
		if i%2 != 0 {
			v = 0
		}
		s.xgetbit(c, 0, "string", uint(i), v)
	}
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestGetRange(c *C) {
	s.xgetrange(c, 0, "string", 0, 0, "")
	s.xgetrange(c, 0, "string", 100, -100, "")
	s.xgetrange(c, 0, "string", -100, 100, "")
	s.xset(c, 0, "string", "hello world!!")
	s.xgetrange(c, 0, "string", 0, 3, "hell")
	s.xgetrange(c, 0, "string", 2, 1, "")
	s.xgetrange(c, 0, "string", -12, 3, "ell")
	s.xgetrange(c, 0, "string", -100, 3, "hell")
	s.xgetrange(c, 0, "string", -1, 10000, "!")
	s.xgetrange(c, 0, "string", -1, -1, "!")
	s.xgetrange(c, 0, "string", -1, -2, "")
	s.xgetrange(c, 0, "string", -1, -1000, "")
	s.xgetrange(c, 0, "string", -100, 100, "hello world!!")
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestXMSet(c *C) {
	s.xmset(c, 0, "a", "1", "b", "2", "c", "3", "a", "4", "b", "5", "c", "6")
	s.xget(c, 0, "a", "4")
	s.xget(c, 0, "b", "5")
	s.xget(c, 0, "c", "6")

	s.kpexpire(c, 0, "a", 1000, 1)
	s.xmset(c, 0, "a", "x")
	s.kpttl(c, 0, "a", -1)
	s.xget(c, 0, "a", "x")

	s.xmset(c, 0, "a", "1", "a", "2", "a", "3", "b", "1", "b", "2")
	s.kdel(c, 0, 3, "a", "b", "c")
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestMSetNX(c *C) {
	s.xsetex(c, 0, "string", "hello", 100)
	s.xmsetnx(c, 0, 0, "string", "world", "string2", "blabla")
	s.xget(c, 0, "string", "hello")

	s.xsetex(c, 0, "string", "hello1", 1)
	s.kpttl(c, 0, "string", 1000)
	s.kpexpire(c, 0, "string", 10, 1)
	sleepms(20)
	s.xget(c, 0, "string", "")

	s.xmsetnx(c, 0, 1, "string", "world1")
	s.xget(c, 0, "string", "world1")
	s.kpttl(c, 0, "string", -1)

	s.kpexpire(c, 0, "string", 10, 1)
	sleepms(20)

	s.xmsetnx(c, 0, 1, "string", "hello", "string", "world")
	s.xget(c, 0, "string", "world")
	s.xdel(c, 0, "string", 1)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestMGet(c *C) {
	s.xmsetnx(c, 0, 1, "a", "1", "b", "2", "c", "3")
	s.kpexpire(c, 0, "a", 10, 1)
	sleepms(20)

	s.xmget(c, 0, "a", "", "b", "2", "c", "3", "d", "")
	s.kdel(c, 0, 2, "a", "b", "c")
	s.checkEmpty(c)
}
