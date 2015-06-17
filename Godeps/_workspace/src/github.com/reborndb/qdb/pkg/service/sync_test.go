// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"os"
	"strconv"

	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/store"
	. "gopkg.in/check.v1"
)

func (s *testServiceSuite) TestBgsaveTo(c *C) {
	k := randomKey(c)
	s.checkOK(c, "flushall")
	const max = 100
	for i := 0; i < max; i++ {
		s.checkOK(c, "set", k+strconv.Itoa(i), i)
	}
	path := "/tmp/testdb-dump.rdb"
	s.checkOK(c, "bgsaveto", path)
	f, err := os.Open(path)
	c.Assert(err, IsNil)
	defer f.Close()
	l := rdb.NewLoader(f)
	c.Assert(l.Header(), IsNil)
	m := make(map[string][]byte)
	for {
		e, err := l.NextBinEntry()
		c.Assert(err, IsNil)
		if e == nil {
			break
		}
		c.Assert(e.DB, Equals, uint32(0))
		c.Assert(e.ExpireAt, Equals, uint64(0))
		m[string(e.Key)] = e.Value
	}
	c.Assert(l.Footer(), IsNil)
	for i := 0; i < max; i++ {
		b := m[k+strconv.Itoa(i)]
		o, err := rdb.DecodeDump(b)
		c.Assert(err, IsNil)
		x, ok := o.(rdb.String)
		c.Assert(ok, Equals, true)
		c.Assert(string(x), Equals, string(store.FormatInt(int64(i))))
	}
}
