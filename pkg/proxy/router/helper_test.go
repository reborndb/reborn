// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"bufio"
	"bytes"
	"time"

	"github.com/reborndb/reborn/pkg/proxy/parser"

	stats "github.com/ngaut/gostats"
	. "gopkg.in/check.v1"
)

const (
	simpleRequest = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$8\r\nmy value\r\n"
)

func (s *testProxyRouterSuite) TestStringsContain(c *C) {
	ss := []string{"abc", "bcd", "ab"}
	c.Assert(StringsContain(ss, "a"), Equals, false)
	c.Assert(StringsContain(ss, "ab"), Equals, true)
}

func (s *testProxyRouterSuite) TestAllowOp(c *C) {
	c.Assert(allowOp("SLOTSMGRT"), Equals, false)
	c.Assert(allowOp("SLOTSMGRTONE"), Equals, false)
	c.Assert(allowOp("SET"), Equals, true)
}

func (s *testProxyRouterSuite) TestIsMulOp(c *C) {
	c.Assert(isMulOp("GET"), Equals, false)
	c.Assert(isMulOp("MGET"), Equals, true)
	c.Assert(isMulOp("MSET"), Equals, true)
	c.Assert(isMulOp("DEL"), Equals, true)
}

func (s *testProxyRouterSuite) TestRecordResponseTime(c *C) {
	cc := stats.NewCounters("test")
	recordResponseTime(cc, 1)
	recordResponseTime(cc, 5)
	recordResponseTime(cc, 10)
	recordResponseTime(cc, 50)
	recordResponseTime(cc, 200)
	recordResponseTime(cc, 1000)
	recordResponseTime(cc, 5000)
	recordResponseTime(cc, 8000)
	recordResponseTime(cc, 10000)
	cnts := cc.Counts()

	c.Assert(cnts["0-5ms"], Equals, int64(1))
	c.Assert(cnts["5-10ms"], Equals, int64(1))
	c.Assert(cnts["50-200ms"], Equals, int64(1))
	c.Assert(cnts["200-1000ms"], Equals, int64(1))
	c.Assert(cnts["1000-5000ms"], Equals, int64(1))
	c.Assert(cnts["5000-10000ms"], Equals, int64(2))
	c.Assert(cnts["10000ms+"], Equals, int64(1))
}

func (s *testProxyRouterSuite) TestValidSlot(c *C) {
	c.Assert(validSlot(-1), Equals, false)
	c.Assert(validSlot(1024), Equals, false)
	c.Assert(validSlot(0), Equals, true)
}

type fakeDeadlineReadWriter struct {
	r *bufio.Reader
	w *bufio.Writer
}

func (rw *fakeDeadlineReadWriter) BufioReader() *bufio.Reader {
	return rw.r
}

func (rw *fakeDeadlineReadWriter) SetReadDeadline(t time.Time) error {
	return nil
}

func (rw *fakeDeadlineReadWriter) SetWriteDeadline(t time.Time) error {
	return nil
}

func (rw *fakeDeadlineReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *fakeDeadlineReadWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func (s *testProxyRouterSuite) TestForward(c *C) {
	client := &fakeDeadlineReadWriter{r: bufio.NewReader(bytes.NewBuffer([]byte(simpleRequest))),
		w: bufio.NewWriter(&bytes.Buffer{})}
	redis := &fakeDeadlineReadWriter{r: bufio.NewReader(bytes.NewBuffer([]byte(simpleRequest))),
		w: bufio.NewWriter(&bytes.Buffer{})}

	resp, err := parser.Parse(bufio.NewReader(bytes.NewBuffer([]byte(simpleRequest))))
	c.Assert(err, IsNil)

	_, clientErr := forward(client, redis, resp, 5)
	c.Assert(clientErr, IsNil)
}

func (s *testProxyRouterSuite) TestWrite2Client(c *C) {
	var result bytes.Buffer
	var input bytes.Buffer
	_, clientErr := write2Client(bufio.NewReader(&input), &result)
	c.Assert(clientErr, NotNil)

	input.WriteString(simpleRequest)
	_, clientErr = write2Client(bufio.NewReader(&input), &result)
	c.Assert(clientErr, IsNil)

	c.Assert(string(result.Bytes()), Equals, simpleRequest)
}

func (s *testProxyRouterSuite) TestWrite2Redis(c *C) {
	var result bytes.Buffer
	var input bytes.Buffer
	input.WriteString(simpleRequest)
	resp, err := parser.Parse(bufio.NewReader(&input))
	c.Assert(err, IsNil)

	err = resp.WriteTo(&result)
	c.Assert(err, IsNil)

	c.Assert(string(result.Bytes()), Equals, simpleRequest)
}

func (s *testProxyRouterSuite) TestHandleSpecCommand(c *C) {
	var tbl = map[string]string{
		"PING":   "+PONG\r\n",
		"QUIT":   string(OK_BYTES),
		"SELECT": string(OK_BYTES),
	}

	for k, v := range tbl {
		resp, err := parser.Parse(bufio.NewReader(bytes.NewBufferString(k + string(parser.NEW_LINE))))
		c.Assert(err, IsNil)

		_, keys, err := resp.GetOpKeys()
		c.Assert(err, IsNil)

		result, _, _, err := handleSpecCommand(k, keys, 5)
		c.Assert(err, IsNil)
		c.Assert(string(result), Equals, v)
	}

	// "ECHO xxxx": "xxxx\r\n",
	{
		resp, err := parser.Parse(bufio.NewReader(bytes.NewBufferString("ECHO xxxx\r\n")))
		c.Assert(err, IsNil)

		_, keys, _ := resp.GetOpKeys()
		result, _, _, err := handleSpecCommand("ECHO", keys, 5)
		c.Assert(err, IsNil)
		c.Assert(string(result), Equals, "$4\r\nxxxx\r\n")
	}

	// test empty key
	{
		resp, err := parser.Parse(bufio.NewReader(bytes.NewBufferString("ECHO\r\n")))
		c.Assert(err, IsNil)

		_, keys, _ := resp.GetOpKeys()
		_, shouldClose, _, err := handleSpecCommand("ECHO", keys, 5)
		c.Assert(shouldClose, Equals, true)
	}

	// test not specific command
	{
		_, _, handled, err := handleSpecCommand("GET", nil, 5)
		c.Assert(handled, Equals, false)
		c.Assert(err, IsNil)
	}
}
