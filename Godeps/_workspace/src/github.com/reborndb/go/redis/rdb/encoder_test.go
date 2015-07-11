// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rdb

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/reborndb/go/atomic2"
	"github.com/reborndb/go/io/ioutils"
	gocheck "gopkg.in/check.v1"
)

func (s *testRedisRdbSuite) toString(text string) String {
	return String([]byte(text))
}

func (s *testRedisRdbSuite) checkString(c *gocheck.C, o interface{}, text string) {
	x, ok := o.(String)
	c.Assert(ok, gocheck.Equals, true)
	c.Assert(string(x), gocheck.Equals, text)
}

func (s *testRedisRdbSuite) TestEncodeString(c *gocheck.C) {
	docheck := func(text string) {
		p, err := EncodeDump(s.toString(text))
		c.Assert(err, gocheck.IsNil)

		o, err := DecodeDump(p)
		c.Assert(err, gocheck.IsNil)

		s.checkString(c, o, text)
	}

	docheck("hello world!!")
	docheck("2147483648")
	docheck("4294967296")
	docheck("")
	var b bytes.Buffer
	for i := 0; i < 1024; i++ {
		b.Write([]byte("01"))
	}
	docheck(b.String())
}

func (s *testRedisRdbSuite) toList(list ...string) List {
	o := List{}
	for _, e := range list {
		o = append(o, []byte(e))
	}
	return o
}

func (s *testRedisRdbSuite) checkList(c *gocheck.C, o interface{}, list []string) {
	x, ok := o.(List)
	c.Assert(ok, gocheck.Equals, true)
	c.Assert(len(x), gocheck.Equals, len(list))

	for i, e := range x {
		c.Assert(string(e), gocheck.Equals, list[i])
	}
}

func (s *testRedisRdbSuite) TestEncodeList(c *gocheck.C) {
	docheck := func(list ...string) {
		p, err := EncodeDump(s.toList(list...))
		c.Assert(err, gocheck.IsNil)

		o, err := DecodeDump(p)
		c.Assert(err, gocheck.IsNil)

		s.checkList(c, o, list)
	}

	docheck("")
	docheck("", "a", "b", "c", "d", "e")
	list := []string{}
	for i := 0; i < 65536; i++ {
		list = append(list, strconv.Itoa(i))
	}
	docheck(list...)
}

func (s *testRedisRdbSuite) toHash(m map[string]string) Hash {
	o := Hash{}
	for k, v := range m {
		o = append(o, &HashElement{Field: []byte(k), Value: []byte(v)})
	}
	return o
}

func (s *testRedisRdbSuite) checkHash(c *gocheck.C, o interface{}, m map[string]string) {
	x, ok := o.(Hash)
	c.Assert(ok, gocheck.Equals, true)
	c.Assert(len(x), gocheck.Equals, len(m))

	for _, e := range x {
		c.Assert(m[string(e.Field)], gocheck.Equals, string(e.Value))
	}
}

func (s *testRedisRdbSuite) TestEncodeHash(c *gocheck.C) {
	docheck := func(m map[string]string) {
		p, err := EncodeDump(s.toHash(m))
		c.Assert(err, gocheck.IsNil)

		o, err := DecodeDump(p)
		c.Assert(err, gocheck.IsNil)

		s.checkHash(c, o, m)
	}
	docheck(map[string]string{"": ""})
	docheck(map[string]string{"": "", "a": "", "b": "a", "c": "b", "d": "c"})
	hash := make(map[string]string)
	for i := 0; i < 65536; i++ {
		hash[strconv.Itoa(i)] = strconv.Itoa(i + 1)
	}
	docheck(hash)
}

func (s *testRedisRdbSuite) toZSet(m map[string]float64) ZSet {
	o := ZSet{}
	for k, v := range m {
		o = append(o, &ZSetElement{Member: []byte(k), Score: v})
	}
	return o
}

func (s *testRedisRdbSuite) checkZSet(c *gocheck.C, o interface{}, m map[string]float64) {
	x, ok := o.(ZSet)
	c.Assert(ok, gocheck.Equals, true)
	c.Assert(len(x), gocheck.Equals, len(m))

	for _, e := range x {
		v := m[string(e.Member)]
		switch {
		case math.IsInf(v, 1):
			c.Assert(math.IsInf(e.Score, 1), gocheck.Equals, true)
		case math.IsInf(v, -1):
			c.Assert(math.IsInf(e.Score, -1), gocheck.Equals, true)
		case math.IsNaN(v):
			c.Assert(math.IsNaN(e.Score), gocheck.Equals, true)
		default:
			c.Assert(math.Abs(e.Score-v) < 1e-10, gocheck.Equals, true)
		}
	}
}

func (s *testRedisRdbSuite) TestEncodeZSet(c *gocheck.C) {
	docheck := func(m map[string]float64) {
		p, err := EncodeDump(s.toZSet(m))
		c.Assert(err, gocheck.IsNil)

		o, err := DecodeDump(p)
		c.Assert(err, gocheck.IsNil)

		s.checkZSet(c, o, m)
	}
	docheck(map[string]float64{"": 0})
	zset := make(map[string]float64)
	for i := -65535; i < 65536; i++ {
		zset[strconv.Itoa(i)] = float64(i)
	}
	docheck(zset)
	zset["inf"] = math.Inf(1)
	zset["-inf"] = math.Inf(-1)
	zset["nan"] = math.NaN()
	docheck(zset)
}

func (s *testRedisRdbSuite) toSet(set ...string) Set {
	o := Set{}
	for _, e := range set {
		o = append(o, []byte(e))
	}
	return o
}

func (s *testRedisRdbSuite) checkSet(c *gocheck.C, o interface{}, set []string) {
	x, ok := o.(Set)
	c.Assert(ok, gocheck.Equals, true)
	c.Assert(len(x), gocheck.Equals, len(set))

	for i, e := range x {
		c.Assert(string(e), gocheck.Equals, set[i])
	}
}

func (s *testRedisRdbSuite) TestEncodeSet(c *gocheck.C) {
	docheck := func(set ...string) {
		p, err := EncodeDump(s.toSet(set...))
		c.Assert(err, gocheck.IsNil)

		o, err := DecodeDump(p)
		c.Assert(err, gocheck.IsNil)

		s.checkSet(c, o, set)
	}
	docheck("")
	docheck("", "a", "b", "c")
	set := []string{}
	for i := 0; i < 65536; i++ {
		set = append(set, strconv.Itoa(i))
	}
	docheck(set...)
}

func (s *testRedisRdbSuite) TestEncodeRdb(c *gocheck.C) {
	objs := make([]struct {
		db       uint32
		expireat uint64
		key      []byte
		obj      interface{}
		typ      string
	}, 128)
	var b bytes.Buffer
	enc := NewEncoder(&b)
	c.Assert(enc.EncodeHeader(), gocheck.IsNil)

	for i := 0; i < len(objs); i++ {
		db := uint32(i + 32)
		expireat := uint64(i)
		key := []byte(strconv.Itoa(i))
		var obj interface{}
		var typ string
		switch i % 5 {
		case 0:
			sss := strconv.Itoa(i)
			obj = sss
			typ = "string"
			c.Assert(enc.EncodeObject(db, key, expireat, s.toString(sss)), gocheck.IsNil)
		case 1:
			list := []string{}
			for j := 0; j < 32; j++ {
				list = append(list, fmt.Sprintf("l%d_%d", i, rand.Int()))
			}
			obj = list
			typ = "list"
			c.Assert(enc.EncodeObject(db, key, expireat, s.toList(list...)), gocheck.IsNil)
		case 2:
			hash := make(map[string]string)
			for j := 0; j < 32; j++ {
				hash[strconv.Itoa(j)] = fmt.Sprintf("h%d_%d", i, rand.Int())
			}
			obj = hash
			typ = "hash"
			c.Assert(enc.EncodeObject(db, key, expireat, s.toHash(hash)), gocheck.IsNil)
		case 3:
			zset := make(map[string]float64)
			for j := 0; j < 32; j++ {
				zset[strconv.Itoa(j)] = rand.Float64()
			}
			obj = zset
			typ = "zset"
			c.Assert(enc.EncodeObject(db, key, expireat, s.toZSet(zset)), gocheck.IsNil)
		case 4:
			set := []string{}
			for j := 0; j < 32; j++ {
				set = append(set, fmt.Sprintf("s%d_%d", i, rand.Int()))
			}
			obj = set
			typ = "set"
			c.Assert(enc.EncodeObject(db, key, expireat, s.toSet(set...)), gocheck.IsNil)
		}

		objs[i].db = db
		objs[i].expireat = expireat
		objs[i].key = key
		objs[i].obj = obj
		objs[i].typ = typ
	}

	c.Assert(enc.EncodeFooter(), gocheck.IsNil)

	rdb := b.Bytes()
	var cc atomic2.Int64
	l := NewLoader(ioutils.NewCountReader(bytes.NewReader(rdb), &cc))
	c.Assert(l.Header(), gocheck.IsNil)

	var i int = 0
	for {
		e, err := l.NextBinEntry()
		c.Assert(err, gocheck.IsNil)
		if e == nil {
			break
		}

		c.Assert(objs[i].db, gocheck.Equals, e.DB)
		c.Assert(objs[i].expireat, gocheck.Equals, e.ExpireAt)
		c.Assert(objs[i].key, gocheck.DeepEquals, e.Key)

		o, err := DecodeDump(e.Value)
		c.Assert(err, gocheck.IsNil)

		switch objs[i].typ {
		case "string":
			s.checkString(c, o, objs[i].obj.(string))
		case "list":
			s.checkList(c, o, objs[i].obj.([]string))
		case "hash":
			s.checkHash(c, o, objs[i].obj.(map[string]string))
		case "zset":
			s.checkZSet(c, o, objs[i].obj.(map[string]float64))
		case "set":
			s.checkSet(c, o, objs[i].obj.([]string))
		}
		i++
	}

	c.Assert(i, gocheck.Equals, len(objs))
	c.Assert(l.Footer(), gocheck.IsNil)
	c.Assert(cc.Get(), gocheck.DeepEquals, int64(len(rdb)))
}
