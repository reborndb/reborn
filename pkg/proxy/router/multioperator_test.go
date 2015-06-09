// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strings"

	"github.com/alicebob/miniredis"
	. "gopkg.in/check.v1"
)

var redisrv *miniredis.Miniredis

func (s *testProxyRouterSuite) TestMgetResults(c *C) {
	redisrv, err := miniredis.Run()
	c.Assert(err, IsNil)
	defer redisrv.Close()

	moper := newMultiOperator(redisrv.Addr(), "")
	redisrv.Set("a", "a")
	redisrv.Set("b", "b")
	redisrv.Set("c", "c")
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
}

func (s *testProxyRouterSuite) TestMsetResults(c *C) {
	redisrv, err := miniredis.Run()
	c.Assert(err, IsNil)
	defer redisrv.Close()

	// for mset x y z bad case test
	moper := newMultiOperator(redisrv.Addr(), "")
	_, err = moper.msetResults(&MulOp{
		op: "mset",
		keys: [][]byte{[]byte("x"),
			[]byte("y"), []byte("z")}})
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "bad number of keys for mset command"), Equals, true)
}

func (s *testProxyRouterSuite) TestDeltResults(c *C) {
	redisrv, err := miniredis.Run()
	c.Assert(err, IsNil)
	defer redisrv.Close()

	moper := newMultiOperator(redisrv.Addr(), "")
	redisrv.Set("a", "a")
	redisrv.Set("b", "b")
	redisrv.Set("c", "c")
	buf, err := moper.delResults(&MulOp{
		op: "del",
		keys: [][]byte{[]byte("a"),
			[]byte("b"), []byte("c")}})
	c.Assert(err, IsNil)

	res := string(buf)
	c.Assert(strings.Contains(res, "3"), Equals, true)
}
