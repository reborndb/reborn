// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package ring

import (
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type testRingSuite struct {
}

var _ = Suite(&testRingSuite{})

func (s *testRingSuite) testRing(c *C, r *Ring) {
	var p []byte
	var n int
	var err error

	p = make([]byte, 0)
	n, err = r.ReadAt(p, 0)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)

	p = []byte("0123456789")
	n, err = r.Write(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(r.Len(), Equals, 10)
	c.Assert(r.Offset(), Equals, 10)

	p = make([]byte, 10)
	n, err = r.ReadAt(p, 0)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(string(p), Equals, "0123456789")

	p = make([]byte, 5)
	n, err = r.ReadAt(p, 5)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "56789")

	n, err = r.ReadAt(p, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)

	_, err = r.ReadAt(p, 11)
	c.Assert(err, NotNil)

	p = []byte("0123456789")
	n, err = r.Write(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(r.Len(), Equals, 20)
	c.Assert(r.Offset(), Equals, 0)

	p = make([]byte, 10)
	n, err = r.ReadAt(p, 15)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p[0:5]), Equals, "56789")

	p = []byte("aaaaaaaaaa")
	n, err = r.Write(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(r.Len(), Equals, 20)
	c.Assert(r.Offset(), Equals, 10)

	p = make([]byte, 10)
	n, err = r.ReadAt(p, 5)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(string(p), Equals, "56789aaaaa")

	p = make([]byte, 100)
	n, err = r.Write(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 100)
	c.Assert(r.Len(), Equals, 20)
	c.Assert(r.Offset(), Equals, 10)

	p = make([]byte, 1)
	n, err = r.ReadAt(p, 0)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
	c.Assert(p[0], Equals, byte('\x00'))
}

func (s *testRingSuite) TestMemRing(c *C) {
	r, err := NewMemRing(20)
	c.Assert(err, IsNil)

	s.testRing(c, r)
}

func (s *testRingSuite) TestFileRing(c *C) {
	name := "/tmp/test_filering"
	defer os.Remove(name)

	r, err := NewFileRing(name, 20)
	c.Assert(err, IsNil)

	s.testRing(c, r)
}
