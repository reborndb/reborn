// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package resp

import (
	"strconv"

	. "gopkg.in/check.v1"
)

func (s *testRedisRespSuite) TestItos(c *C) {
	for i := 0; i < len(imap)*2; i++ {
		n, p := -i, i
		c.Assert(strconv.Itoa(n), Equals, itos(int64(n)))
		c.Assert(strconv.Itoa(p), Equals, itos(int64(p)))
	}
}

func (s *testRedisRespSuite) TestEncodeString(c *C) {
	resp := &String{"OK"}
	s.testEncodeAndCheck(c, resp, []byte("+OK\r\n"))
}

func (s *testRedisRespSuite) TestEncodeError(c *C) {
	resp := &Error{"Error"}
	s.testEncodeAndCheck(c, resp, []byte("-Error\r\n"))
}

func (s *testRedisRespSuite) TestEncodeInt(c *C) {
	resp := &Int{}
	for _, v := range []int{-1, 0, 1024 * 1024} {
		resp.Value = int64(v)
		s.testEncodeAndCheck(c, resp, []byte(":"+strconv.FormatInt(int64(v), 10)+"\r\n"))
	}
}

func (s *testRedisRespSuite) TestEncodeBulkBytes(c *C) {
	resp := &BulkBytes{}
	resp.Value = nil
	s.testEncodeAndCheck(c, resp, []byte("$-1\r\n"))
	resp.Value = []byte{}
	s.testEncodeAndCheck(c, resp, []byte("$0\r\n\r\n"))
	resp.Value = []byte("helloworld!!")
	s.testEncodeAndCheck(c, resp, []byte("$12\r\nhelloworld!!\r\n"))
}

func (s *testRedisRespSuite) TestEncodeArray(c *C) {
	resp := &Array{}
	resp.Value = nil
	s.testEncodeAndCheck(c, resp, []byte("*-1\r\n"))
	resp.Value = []Resp{}
	s.testEncodeAndCheck(c, resp, []byte("*0\r\n"))
	resp.Append(&Int{0})
	s.testEncodeAndCheck(c, resp, []byte("*1\r\n:0\r\n"))
	resp.Append(&BulkBytes{nil})
	s.testEncodeAndCheck(c, resp, []byte("*2\r\n:0\r\n$-1\r\n"))
	resp.Append(&BulkBytes{[]byte("test")})
	s.testEncodeAndCheck(c, resp, []byte("*3\r\n:0\r\n$-1\r\n$4\r\ntest\r\n"))
}

func (s *testRedisRespSuite) TestEncodePing(c *C) {
	resp := NewPing()
	s.testEncodeAndCheck(c, resp, []byte("\n"))
}

func (s *testRedisRespSuite) TestEncodeRequest(c *C) {
	resp := NewRequest("PING")
	s.testEncodeAndCheck(c, resp, []byte("*1\r\n$4\r\nPING\r\n"))

	resp = NewRequest("SELECT", 1)
	s.testEncodeAndCheck(c, resp, []byte("*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"))
}

func (s *testRedisRespSuite) testEncodeAndCheck(c *C, resp Resp, expect []byte) {
	b, err := EncodeToBytes(resp)
	c.Assert(err, IsNil)
	c.Assert(b, DeepEquals, expect)
}
