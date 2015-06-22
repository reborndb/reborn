// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"fmt"
	"time"

	"github.com/reborndb/go/redis/rdb"
	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) TestSnapshot(c *C) {
	s.xsetex(c, 0, "string", "value", 10)
	s.kpexpire(c, 0, "string", 10, 1)

	now := nowms()

	m := make(map[string]string)
	for db := uint32(0); db < 128; db++ {
		key := fmt.Sprintf("key_%d", db)
		val := fmt.Sprintf("val_%d", db)
		m[key] = val
		ss := []string{}
		for k, v := range m {
			ss = append(ss, k, v)
		}
		s.hmset(c, db, "hash", ss...)
		s.kpexpireat(c, db, "hash", now+1000*int64(db+37), 1)
	}

	sleepms(20)

	ss, err := s.s.NewSnapshot()
	c.Assert(err, IsNil)

	objs, _, err := ss.LoadObjCron(time.Hour, 4, 4096)
	c.Assert(err, IsNil)
	c.Assert(len(objs), Equals, 128)

	s.kpttl(c, 0, "string", -2)

	s.s.ReleaseSnapshot(ss)

	for db := uint32(0); db < 128; db++ {
		ok := false
		for _, obj := range objs {
			if obj.DB != db {
				continue
			}
			ok = true
			c.Assert(string(obj.Key), Equals, "hash")
			c.Assert(int64(obj.ExpireAt), Equals, now+int64(db+37)*1000)

			x := obj.Value.(rdb.Hash)
			c.Assert(err, IsNil)
			c.Assert(len(x), Equals, int(db+1))

			for _, e := range x {
				c.Assert(m[string(e.Field)], Equals, string(e.Value))
			}
		}

		c.Assert(err, IsNil)
		c.Assert(ok, Equals, true)

		s.hdelall(c, db, "hash", 1)
	}

	s.checkCompact(c)
	s.checkEmpty(c)

	ss, err = s.s.NewSnapshot()
	c.Assert(err, IsNil)

	objs, _, err = ss.LoadObjCron(time.Hour, 4, 4096)
	c.Assert(err, IsNil)
	c.Assert(len(objs), Equals, 0)

	s.s.ReleaseSnapshot(ss)
	s.checkEmpty(c)
}
