// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strings"

	. "gopkg.in/check.v1"
)

func (s *testProxyRouterSuite) TestMgetResults(c *C) {
	moper := newMultiOperator(s.s.addr, storeAuth)

	var err error
	err = s.s.store.Set(0, "a", "a")
	c.Assert(err, IsNil)
	err = s.s.store.Set(0, "b", "b")
	c.Assert(err, IsNil)
	err = s.s.store.Set(0, "c", "c")
	c.Assert(err, IsNil)

	buf, err := moper.mgetResults(&MulOp{
		op: "mget",
		keys: [][]byte{[]byte("a"),
			[]byte("b"), []byte("c"), []byte("x")}})
	c.Assert(err, IsNil)

	res := string(buf)
	c.Assert(strings.Contains(res, "a"), Equals, true)
	c.Assert(strings.Contains(res, "b"), Equals, true)
	c.Assert(strings.Contains(res, "c"), Equals, true)

	buf, err = moper.mgetResults(&MulOp{
		op: "mget",
		keys: [][]byte{[]byte("x"),
			[]byte("c"), []byte("x")}})
	c.Assert(err, IsNil)

	buf, err = moper.mgetResults(&MulOp{
		op: "mget",
		keys: [][]byte{[]byte("x"),
			[]byte("y"), []byte("x")}})
	c.Assert(err, IsNil)

	err = s.s.store.Reset()
	c.Assert(err, IsNil)
}

func (s *testProxyRouterSuite) TestMsetResults(c *C) {
	// for mset x y z bad case test
	moper := newMultiOperator(s.s.addr, storeAuth)
	_, err := moper.msetResults(&MulOp{
		op: "mset",
		keys: [][]byte{[]byte("x"),
			[]byte("y"), []byte("z")}})
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "bad number of keys for mset command"), Equals, true)

	err = s.s.store.Reset()
	c.Assert(err, IsNil)
}

func (s *testProxyRouterSuite) TestDeltResults(c *C) {
	moper := newMultiOperator(s.s.addr, storeAuth)

	var err error
	err = s.s.store.Set(0, "a", "a")
	c.Assert(err, IsNil)
	err = s.s.store.Set(0, "b", "b")
	c.Assert(err, IsNil)
	err = s.s.store.Set(0, "c", "c")
	c.Assert(err, IsNil)

	buf, err := moper.delResults(&MulOp{
		op: "del",
		keys: [][]byte{[]byte("a"),
			[]byte("b"), []byte("c")}})
	c.Assert(err, IsNil)

	res := string(buf)
	c.Assert(strings.Contains(res, "3"), Equals, true)

	err = s.s.store.Reset()
	c.Assert(err, IsNil)
}
