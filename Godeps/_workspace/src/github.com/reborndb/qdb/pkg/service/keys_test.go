// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"github.com/reborndb/qdb/pkg/store"
	. "gopkg.in/check.v1"
)

func (s *testServiceSuite) TestSelect(c *C) {
	k := randomKey(c)

	nc := s.getConn(c)
	defer nc.Recycle()

	nc.checkOK(c, "select", 128)
	nc.checkOK(c, "set", k, "128")
	nc.checkString(c, "128", "get", k)
	nc.checkOK(c, "select", 258)
	nc.checkNil(c, "get", k)
	nc.checkOK(c, "select", 128)
	nc.checkString(c, "128", "get", k)
	nc.checkOK(c, "select", 0)
}

func (s *testServiceSuite) TestDel(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, 100)
	s.checkInt(c, 1, "del", k)
	s.checkOK(c, "set", k, 200)
	s.checkInt(c, 1, "del", k, k, k, k)
	s.checkInt(c, 0, "del", k)
}

func (s *testServiceSuite) TestDump(c *C) {
	k := randomKey(c)
	s.checkOK(c, "set", k, "hello")
	expect := "\x00\x05\x68\x65\x6c\x6c\x6f\x06\x00\xf5\x9f\xb7\xf6\x90\x61\x1c\x99"
	s.checkBytes(c, []byte(expect), "dump", k)
	s.checkInt(c, 1, "del", k)
	s.checkOK(c, "restore", k, 1000, expect)
	s.checkString(c, "hello", "get", k)
	s.checkIntApprox(c, 1000, 50, "pttl", k)
}

func (s *testServiceSuite) TestType(c *C) {
	k := randomKey(c)
	s.checkString(c, "none", "type", k)
	s.checkInt(c, 0, "exists", k)
	s.checkOK(c, "set", k, "hello")
	s.checkString(c, "string", "type", k)
	s.checkString(c, "hello", "GET", k)
	s.checkInt(c, 1, "exists", k)
}

func (s *testServiceSuite) TestExpire(c *C) {
	k := randomKey(c)
	s.checkInt(c, -2, "ttl", k)
	s.checkInt(c, 0, "expire", k, 1000)
	s.checkOK(c, "set", k, 100)
	s.checkInt(c, -1, "ttl", k)
	s.checkInt(c, 1, "expire", k, 1000)
	s.checkIntApprox(c, 1000, 5, "ttl", k)
}

func (s *testServiceSuite) TestPExpire(c *C) {
	k := randomKey(c)
	s.checkInt(c, -2, "pttl", k)
	s.checkInt(c, 0, "pexpire", k, 100000)
	s.checkOK(c, "set", k, 100)
	s.checkInt(c, -1, "pttl", k)
	s.checkInt(c, 1, "pexpire", k, 100000)
	s.checkIntApprox(c, 100000, 5000, "pttl", k)
}

func (s *testServiceSuite) TestExpireAt(c *C) {
	k := randomKey(c)
	expireat, _ := store.TTLmsToExpireAt(1000)
	s.checkInt(c, -2, "ttl", k)
	s.checkOK(c, "set", k, 100)
	s.checkInt(c, 1, "expireat", k, expireat/1e3+1000)
	s.checkIntApprox(c, 1000, 5, "ttl", k)
	s.checkIntApprox(c, 1000000, 5000, "pttl", k)
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, -2, "ttl", k)
}

func (s *testServiceSuite) TestPExpireAt(c *C) {
	k := randomKey(c)
	expireat, _ := store.TTLmsToExpireAt(1000)
	s.checkInt(c, -2, "pttl", k)
	s.checkOK(c, "set", k, 100)
	s.checkInt(c, 1, "pexpireat", k, expireat+100000)
	s.checkIntApprox(c, 100000, 5000, "pttl", k)
	s.checkIntApprox(c, 100, 5, "ttl", k)
	s.checkInt(c, 1, "del", k)
	s.checkInt(c, -2, "pttl", k)
}

func (s *testServiceSuite) TestPersist(c *C) {
	k := randomKey(c)
	expireat, _ := store.TTLmsToExpireAt(1000)
	s.checkInt(c, -2, "pttl", k)
	s.checkInt(c, 0, "persist", k)
	s.checkOK(c, "set", k, "100")
	s.checkInt(c, 1, "pexpireat", k, expireat+100000)
	s.checkIntApprox(c, 100000, 5000, "pttl", k)
	s.checkInt(c, 1, "persist", k)
	s.checkInt(c, 0, "persist", k)
	s.checkInt(c, -1, "pttl", k)
}
