// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"testing"

	"github.com/alicebob/miniredis"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilsSuite{})

type testUtilsSuite struct {
	r    *miniredis.Miniredis
	addr string
	auth string
}

func (s *testUtilsSuite) SetUpSuite(c *C) {
	var err error
	s.r, err = miniredis.Run()
	c.Assert(err, IsNil)

	s.addr = s.r.Addr()
	s.auth = "abc"
	s.r.RequireAuth(s.auth)
}

func (s *testUtilsSuite) TearDownSuite(c *C) {
	if s.r != nil {
		s.r.Close()
		s.r = nil
	}
}

func (s *testUtilsSuite) TestPing(c *C) {
	err := Ping(s.addr, s.auth)
	c.Assert(err, IsNil)
}

func (s *testUtilsSuite) TestGetInfo(c *C) {
	// miniredis has no INFO command, we will use qdb instead later.
	c.Skip("miniredis has no INFO command, use qdb instead later, skip now")

	_, err := GetRedisInfo(s.addr, "", s.auth)
	c.Assert(err, IsNil)
}
