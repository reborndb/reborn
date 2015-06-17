// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/reborndb/go/redis/rdb"
	redis "github.com/reborndb/go/redis/resp"
	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) checkConn(c *C) (*net.TCPAddr, net.Conn) {
	l, err := net.Listen("tcp4", ":0")
	c.Assert(err, IsNil)
	defer l.Close()

	addr := l.Addr().(*net.TCPAddr)

	x := make(chan interface{}, 1)

	go func() {
		c, err := l.Accept()
		if err != nil {
			x <- err
		} else {
			x <- c
		}
	}()

	conn, err := getSockConn(addr.String(), time.Second)
	c.Assert(err, IsNil)

	putSockConn(addr.String(), conn)

	o := <-x
	if err, ok := o.(error); ok {
		c.Fatal(err)
	}
	return addr, o.(net.Conn)
}

func (s *testStoreSuite) checkSlotsMgrt(c *C, r *bufio.Reader, w *bufio.Writer, cc chan error, expect ...interface{}) {
	if len(expect) != 0 {
		req1, err := redis.Decode(r)
		c.Assert(err, IsNil)

		cmd1, args1, err := redis.ParseArgs(req1)
		c.Assert(err, IsNil)
		c.Assert(cmd1, Equals, "select")
		c.Assert(len(args1), Equals, 1)

		err = redis.Encode(w, redis.NewString("OK"))
		c.Assert(err, IsNil)

		err = w.Flush()
		c.Assert(err, IsNil)

		req2, err := redis.Decode(r)
		cmd2, args2, err := redis.ParseArgs(req2)
		c.Assert(err, IsNil)
		c.Assert(cmd2, Equals, "slotsrestore")
		c.Assert(len(args2), Equals, len(expect))

		m := make(map[string]*struct {
			key, value string
			ttlms      uint64
		})
		for i := 0; i < len(expect)/3; i++ {
			v := &struct {
				key, value string
				ttlms      uint64
			}{key: expect[i*3].(string), value: expect[i*3+2].(string)}
			v.ttlms, err = ParseUint(expect[i*3+1])
			c.Assert(err, IsNil)
			m[v.key] = v
		}

		for i := 0; i < len(expect)/3; i++ {
			key := args2[i*3]
			ttlms := args2[i*3+1]
			value := args2[i*3+2]

			v := m[string(key)]
			c.Assert(v, NotNil)
			c.Assert(string(key), Equals, v.key)

			b, err := rdb.DecodeDump(value)
			c.Assert(err, IsNil)
			c.Assert(string(b.(rdb.String)), Equals, v.value)

			x, err := strconv.Atoi(string(ttlms))
			c.Assert(err, IsNil)

			if v.ttlms == 0 {
				c.Assert(x, Equals, 0)
			} else {
				c.Assert(x, Not(Equals), 0)
				c.Assert(math.Abs(float64(x)-float64(v.ttlms)) < 1000, Equals, true)
			}
		}

		err = redis.Encode(w, redis.NewString("OK"))
		c.Assert(err, IsNil)

		err = w.Flush()
		c.Assert(err, IsNil)
	}

	select {
	case err := <-cc:
		c.Assert(err, IsNil)
	case <-time.After(time.Second):
		c.Fatal("timeout error")
	}
}

func (s *testStoreSuite) xslotsrestore(c *C, db uint32, args ...interface{}) {
	x := []interface{}{}
	for i, a := range args {
		switch i % 3 {
		case 0, 1:
			x = append(x, a)
		case 2:
			dump, err := rdb.EncodeDump(rdb.String([]byte(a.(string))))
			c.Assert(err, IsNil)

			x = append(x, dump)
		}
	}

	err := s.s.SlotsRestore(db, x...)
	c.Assert(err, IsNil)
}

func (s *testStoreSuite) slotsinfo(c *C, db uint32, sum int64) {
	m, err := s.s.SlotsInfo(db)
	c.Assert(err, IsNil)
	c.Assert(m, NotNil)

	a := int64(0)
	for _, v := range m {
		a += v
	}
	c.Assert(a, Equals, sum)
}

func (s *testStoreSuite) slotsmgrtslot(addr *net.TCPAddr, db uint32, tag string, expect int64) chan error {
	c := make(chan error, 1)
	go func() {
		host, port := addr.IP.String(), addr.Port
		n, err := s.s.SlotsMgrtSlot(db, host, port, 1000, HashTagToSlot([]byte(tag)))
		if err != nil {
			c <- err
		} else if n != expect {
			c <- errors.Errorf("n = %d, expect = %d", n, expect)
		} else {
			c <- nil
		}
	}()
	return c
}

func (s *testStoreSuite) slotsmgrttagslot(addr *net.TCPAddr, db uint32, tag string, expect int64) chan error {
	c := make(chan error, 1)
	go func() {
		host, port := addr.IP.String(), addr.Port
		n, err := s.s.SlotsMgrtTagSlot(db, host, port, 1000, HashTagToSlot([]byte(tag)))
		if err != nil {
			c <- err
		} else if n != expect {
			c <- errors.Errorf("n = %d, expect = %d", n, expect)
		} else {
			c <- nil
		}
	}()
	return c
}

func (s *testStoreSuite) slotsmgrtone(addr *net.TCPAddr, db uint32, key string, expect int64) chan error {
	c := make(chan error, 1)
	go func() {
		host, port := addr.IP.String(), addr.Port
		n, err := s.s.SlotsMgrtOne(db, host, port, 1000, []byte(key))
		if err != nil {
			c <- err
		} else if n != expect {
			c <- errors.Errorf("n = %d, expect = %d", n, expect)
		} else {
			c <- nil
		}
	}()
	return c
}

func (s *testStoreSuite) slotsmgrttagone(addr *net.TCPAddr, db uint32, key string, expect int64) chan error {
	c := make(chan error, 1)
	go func() {
		host, port := addr.IP.String(), addr.Port
		n, err := s.s.SlotsMgrtTagOne(db, host, port, 1000, []byte(key))
		if err != nil {
			c <- err
		} else if n != expect {
			c <- errors.Errorf("n = %d, expect = %d", n, expect)
		} else {
			c <- nil
		}
	}()
	return c
}

func (s *testStoreSuite) TestSlotNum(c *C) {
	tests := [][]string{
		[]string{"", ""},
		[]string{"{", "{"},
		[]string{"{test", "{test"},
		[]string{"{test{0}", "test{0"},
		[]string{"test{a}", "a"},
		[]string{"{b}test", "b"},
		[]string{"}test{c}", "c"},
		[]string{"}test", "}test"},
		[]string{"}test1{test2{d}}{e}", "test2{d"},
	}
	for _, p := range tests {
		key, tag := []byte(p[0]), []byte(p[1])
		c.Assert(bytes.Equal(HashTag(key), tag), Equals, true)
	}

	const n = MaxSlotNum * 32
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key_%d_%d", rand.Int(), rand.Int()))
		c.Assert(bytes.Equal(HashTag(key), key), Equals, true)
	}

	for i := 0; i < n; i++ {
		v := rand.Int()
		tag := []byte(fmt.Sprintf("%d", v))
		key := []byte(fmt.Sprintf("key_{%d}_%d", v, rand.Int()))
		c.Assert(bytes.Equal(HashTag(key), tag), Equals, true)
	}
}

func (s *testStoreSuite) TestSlotsRestore(c *C) {
	s.xslotsrestore(c, 0, "key", 1000, "hello")
	s.xget(c, 0, "key", "hello")
	s.kpttl(c, 0, "key", 1000)

	s.xslotsrestore(c, 0, "key", 8000, "world")
	s.xget(c, 0, "key", "world")
	s.kpttl(c, 0, "key", 8000)

	s.xslotsrestore(c, 0, "key", 2000, "abc0", "key", 6000, "abc2")
	s.xget(c, 0, "key", "abc2")
	s.kpttl(c, 0, "key", 6000)

	s.xslotsrestore(c, 0, "key", 1000, "abc3", "key", 1000, "abc1")
	s.xget(c, 0, "key", "abc1")
	s.kpttl(c, 0, "key", 1000)

	s.slotsinfo(c, 0, 1)
	s.xdel(c, 0, "key", 1)
	s.slotsinfo(c, 0, 0)
	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSlotsMgrtSlot(c *C) {
	s.xslotsrestore(c, 0, "key", 1000, "hello", "key", 8000, "world")

	addr, cc := s.checkConn(c)
	defer cc.Close()

	r, w := bufio.NewReader(cc), bufio.NewWriter(cc)

	s.slotsinfo(c, 0, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrtslot(addr, 0, "key", 1), "key", 8000, "world")
	s.slotsinfo(c, 0, 0)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrtslot(addr, 0, "key", 0))
	s.slotsinfo(c, 0, 0)

	s.slotsinfo(c, 1, 0)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrtslot(addr, 1, "key", 0))

	s.xslotsrestore(c, 1, "key", 0, "world2")
	s.slotsinfo(c, 1, 1)

	s.checkSlotsMgrt(c, r, w, s.slotsmgrtslot(addr, 1, "key", 1), "key", 0, "world2")
	s.slotsinfo(c, 1, 0)

	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSlotsMgrtTagSlot(c *C) {
	args := []interface{}{}
	for i := 0; i < 32; i++ {
		key := "{}_" + strconv.Itoa(i)
		value := "test_" + strconv.Itoa(i)
		s.xset(c, 0, key, value)
		s.kpexpire(c, 0, key, 1000, 1)
		args = append(args, key, 1000, value)
	}

	addr, cc := s.checkConn(c)
	defer cc.Close()

	r, w := bufio.NewReader(cc), bufio.NewWriter(cc)

	s.slotsinfo(c, 0, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagslot(addr, 0, "tag", 0))
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagslot(addr, 0, "", 32), args...)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagslot(addr, 0, "", 0))
	s.slotsinfo(c, 0, 0)

	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSlotsMgrtOne(c *C) {
	s.xset(c, 0, "key{tag}", "hello")
	s.xset(c, 1, "key{tag}1", "hello")
	s.xset(c, 1, "key{tag}2", "world")

	addr, cc := s.checkConn(c)
	defer cc.Close()

	r, w := bufio.NewReader(cc), bufio.NewWriter(cc)

	s.slotsinfo(c, 0, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrtone(addr, 0, "key{tag}", 1), "key{tag}", 0, "hello")
	s.slotsinfo(c, 0, 0)

	s.slotsinfo(c, 1, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrtone(addr, 1, "key{tag}1", 1), "key{tag}1", 0, "hello")
	s.slotsinfo(c, 1, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrtone(addr, 1, "key{tag}2", 1), "key{tag}2", 0, "world")
	s.slotsinfo(c, 1, 0)

	s.checkEmpty(c)
}

func (s *testStoreSuite) TestSlotsMgrtTagOne(c *C) {
	s.xset(c, 0, "tag", "xxxx")
	s.xset(c, 0, "key{tag}", "hello")
	s.xset(c, 1, "key{tag}1", "hello")
	s.xset(c, 1, "key{tag}2", "world")

	addr, cc := s.checkConn(c)
	defer cc.Close()

	r, w := bufio.NewReader(cc), bufio.NewWriter(cc)

	s.slotsinfo(c, 0, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagone(addr, 0, "tag", 1), "tag", 0, "xxxx")
	s.slotsinfo(c, 0, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagone(addr, 0, "key{tag}", 1), "key{tag}", 0, "hello")
	s.slotsinfo(c, 0, 0)

	s.slotsinfo(c, 1, 1)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagone(addr, 1, "key{tag}1", 2), "key{tag}1", 0, "hello", "key{tag}2", 0, "world")
	s.slotsinfo(c, 1, 0)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagone(addr, 1, "key{tag}2", 0))

	s.xset(c, 2, "key{tag3}", "test")
	s.kpexpire(c, 2, "key{tag3}", 100, 1)
	sleepms(200)
	s.checkSlotsMgrt(c, r, w, s.slotsmgrttagone(addr, 2, "key{tag}3", 0))
	s.xdel(c, 2, "key{tag3}", 0)

	s.checkEmpty(c)
}
