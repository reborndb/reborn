// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package parser

import (
	"bufio"
	"bytes"
	"testing"

	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testProxyParserSuite{})

type testProxyParserSuite struct {
}

func (s *testProxyParserSuite) SetUpSuite(c *C) {
}

func (s *testProxyParserSuite) TearDownSuite(c *C) {
}

func (s *testProxyParserSuite) testParser(c *C, str string) *Resp {
	buf := bytes.NewBuffer([]byte(str))
	r := bufio.NewReader(buf)

	resp, err := Parse(r)
	c.Assert(err, IsNil)

	return resp
}

func (s *testProxyParserSuite) TestBtoi(c *C) {
	tbl := map[string]int{
		"-1": -1,
		"0":  0,
		"1":  1,
	}

	for k, v := range tbl {
		n, _ := Btoi([]byte(k))
		c.Assert(n, Equals, v)
	}
}

func (s *testProxyParserSuite) TestParserBulk(c *C) {
	sample := "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"

	resp := s.testParser(c, sample)
	c.Assert(resp, NotNil)

	b, err := resp.Bytes()
	c.Assert(err, IsNil)
	c.Assert(len(b), Equals, len(sample))

	op, keys, err := resp.GetOpKeys()
	c.Assert(string(op), Equals, "LLEN")
	c.Assert(string(keys[0]), Equals, "mylist")
}

func (s *testProxyParserSuite) TestKeys(c *C) {
	table := []string{
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
	}

	for _, t := range table {
		resp := s.testParser(c, t)

		b, err := resp.Bytes()
		c.Assert(err, IsNil)
		c.Assert(t, Equals, string(b))

		_, keys, err := resp.GetOpKeys()
		c.Assert(err, IsNil)
		c.Assert(len(keys), Equals, 1)
		c.Assert(string(keys[0]), Equals, "bar")
	}
}

func (s *testProxyParserSuite) TestMulOpKeys(c *C) {
	table := []string{
		"*7\r\n$4\r\nmset\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n$4\r\nkey3\r\n$0\r\n\r\n",
	}

	for _, t := range table {
		resp := s.testParser(c, t)

		b, err := resp.Bytes()
		c.Assert(err, IsNil)
		c.Assert(t, Equals, string(b))

		_, keys, err := resp.GetOpKeys()
		c.Assert(err, IsNil)
		c.Assert(len(keys), Equals, 6)
		c.Assert(string(keys[5]), Equals, "")
	}
}

func (s *testProxyParserSuite) TestParser(c *C) {
	table := []string{
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
		"mget a b c\r\n",
	}

	for _, t := range table {
		resp := s.testParser(c, t)

		_, err := resp.Bytes()
		c.Assert(err, IsNil)
	}

	//test invalid input
	buf := bytes.NewBuffer([]byte("*xx$**"))
	r := bufio.NewReader(buf)

	_, err := Parse(r)
	c.Assert(err, NotNil)
}

func (s *testProxyParserSuite) TestTelnet(c *C) {
	resp := s.testParser(c, "echo   abc\r\n")
	b, err := resp.Bytes()
	c.Assert(err, IsNil)

	s.testParser(c, string(b))
}

func (s *testProxyParserSuite) TestEval(c *C) {
	table := []string{
		"*3\r\n$4\r\nEVAL\r\n$31\r\nreturn {1,2,{3,'Hello World!'}}\r\n$1\r\n0\r\n",
	}

	for _, t := range table {
		resp := s.testParser(c, t)

		op, keys, err := resp.GetOpKeys()
		c.Assert(err, IsNil)
		c.Assert(string(op), Equals, "EVAL")
		c.Assert(len(resp.Multi), Equals, 3)
		c.Assert(len(keys), Equals, 1)

		_, err = resp.Bytes()
		c.Assert(err, IsNil)
	}
}

func (s *testProxyParserSuite) TestParserErrorHandling(c *C) {
	buf := bytes.NewBuffer([]byte("-Error message\r\n"))
	r := bufio.NewReader(buf)

	resp, err := Parse(r)
	c.Assert(err, IsNil)
	c.Assert(resp.Type, Equals, ErrorResp)
	c.Assert(len(raw2Error(resp)), Not(Equals), 0)
}

func (s *testProxyParserSuite) TestParserInvalid(c *C) {
	table := []string{
		"",
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

	for _, t := range table {
		// test invalid input
		buf := bytes.NewBuffer([]byte(t))
		r := bufio.NewReader(buf)

		_, err := Parse(r)
		c.Assert(err, NotNil)
	}
}

func (s *testProxyParserSuite) TestWriteCommand(c *C) {
	var buf bytes.Buffer
	WriteCommand(&buf, "SET", "a", 1)

	s.testParser(c, buf.String())
}
