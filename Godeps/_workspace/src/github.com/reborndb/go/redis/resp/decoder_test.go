// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package resp

import (
	"testing"

	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRedisRespSuite{})

type testRedisRespSuite struct {
}

func (s *testRedisRespSuite) TestDecodeInvalidRequests(c *C) {
	test := []string{
		"",
		"*hello\r\n",
		"*-100\r\n",
		"*3\r\nhi",
		"*3\r\nhi\r\n",
		"*4\r\n$1",
		"*4\r\n$1\r",
		"*4\r\n$1\n",
		"*2\r\n$3\r\nget\r\n$what?\r\nx\r\n",
		"*4\r\n$3\r\nget\r\n$1\r\nx\r\n",
		"*2\r\n$3\r\nget\r\n$1\r\nx",
		"*2\r\n$3\r\nget\r\n$1\r\nx\r",
		"*2\r\n$3\r\nget\r\n$100\r\nx\r\n",
		"$6\r\nfoobar\r",
		"$0\rn\r\n",
		"$-1\n",
		"*0",
		"*2n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"3\r\n:1\r\n:2\r\n:3\r\n",
		"*-\r\n",
		"+OK\n",
		"-Error message\r",
	}
	for _, ss := range test {
		_, err := DecodeFromBytes([]byte(ss))
		c.Assert(err, NotNil)
	}
}

func (s *testRedisRespSuite) TestDecodeBulkBytes(c *C) {
	test := "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
	resp, err := DecodeFromBytes([]byte(test))
	c.Assert(err, IsNil)

	x, ok := resp.(*Array)
	c.Assert(ok, Equals, true)
	c.Assert(len(x.Value), Equals, 2)

	s1, ok := x.Value[0].(*BulkBytes)
	c.Assert(ok, Equals, true)
	c.Assert(s1.Value, DeepEquals, []byte("LLEN"))

	s2, ok := x.Value[1].(*BulkBytes)
	c.Assert(ok, Equals, true)
	c.Assert(s2.Value, DeepEquals, []byte("mylist"))
}

func (s *testRedisRespSuite) TestDecoder(c *C) {
	test := []string{
		"$6\r\nfoobar\r\n",
		"$0\r\n\r\n",
		"$-1\r\n",
		"*0\r\n",
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*3\r\n:1\r\n:2\r\n:3\r\n",
		"*-1\r\n",
		"+OK\r\n",
		"-Error message\r\n",
		"*2\r\n$1\r\n0\r\n*0\r\n",
		"*3\r\n$4\r\nEVAL\r\n$31\r\nreturn {1,2,{3,'Hello World!'}}\r\n$1\r\n0\r\n",
		"\n",
	}
	for _, ss := range test {
		_, err := DecodeFromBytes([]byte(ss))
		c.Assert(err, IsNil)
	}
}

func (s *testRedisRespSuite) TestDecodeRequest(c *C) {
	test := []string{
		"PING\r\n",
		"ECHO   abc\r\n",
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"\n",
	}
	for _, ss := range test {
		_, err := DecodeRequestFromBytes([]byte(ss))
		c.Assert(err, IsNil)
	}

	invalidTest := []string{
		"+OK\r\n",
	}
	for _, ss := range invalidTest {
		_, err := DecodeRequestFromBytes([]byte(ss))
		c.Assert(err, NotNil)
	}
}
