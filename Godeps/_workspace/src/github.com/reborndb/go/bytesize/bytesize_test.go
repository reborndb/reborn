// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package bytesize

import (
	"testing"

	"github.com/reborndb/go/errors2"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBytesizeSuite{})

type testBytesizeSuite struct {
}

func (s *testBytesizeSuite) TestBytesize(c *C) {
	c.Assert(MustParse("1"), Equals, int64(1))
	c.Assert(MustParse("1B"), Equals, int64(1))
	c.Assert(MustParse("1K"), Equals, int64(KB))
	c.Assert(MustParse("1M"), Equals, int64(MB))
	c.Assert(MustParse("1G"), Equals, int64(GB))
	c.Assert(MustParse("1T"), Equals, int64(TB))
	c.Assert(MustParse("1P"), Equals, int64(PB))

	c.Assert(MustParse(" -1"), Equals, int64(-1))
	c.Assert(MustParse(" -1 b"), Equals, int64(-1))
	c.Assert(MustParse(" -1 kb "), Equals, int64(-1*KB))
	c.Assert(MustParse(" -1 mb "), Equals, int64(-1*MB))
	c.Assert(MustParse(" -1 gb "), Equals, int64(-1*GB))
	c.Assert(MustParse(" -1 tb "), Equals, int64(-1*TB))
	c.Assert(MustParse(" -1 pb "), Equals, int64(-1*PB))

	c.Assert(MustParse(" 1.5"), Equals, int64(1))
	c.Assert(MustParse(" 1.5 kb "), Equals, int64(1.5*KB))
	c.Assert(MustParse(" 1.5 mb "), Equals, int64(1.5*MB))
	c.Assert(MustParse(" 1.5 gb "), Equals, int64(1.5*GB))
	c.Assert(MustParse(" 1.5 tb "), Equals, int64(1.5*TB))
	c.Assert(MustParse(" 1.5 pb "), Equals, int64(1.5*PB))
}

func (s *testBytesizeSuite) TestBytesizeError(c *C) {
	var err error
	_, err = Parse("--1")
	c.Assert(errors2.ErrorEqual(err, ErrBadBytesize), Equals, true)
	_, err = Parse("hello world")
	c.Assert(errors2.ErrorEqual(err, ErrBadBytesize), Equals, true)
	_, err = Parse("123.132.32")
	c.Assert(errors2.ErrorEqual(err, ErrBadBytesize), Equals, true)
}
