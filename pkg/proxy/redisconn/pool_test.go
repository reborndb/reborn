// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redisconn

import (
	"net"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testPoolSuite struct {
}

var _ = Suite(&testPoolSuite{})

type testDummyConn struct {
}

func (c *testDummyConn) Read(b []byte) (n int, err error)   { return len(b), nil }
func (c *testDummyConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *testDummyConn) Close() error                       { return nil }
func (c *testDummyConn) LocalAddr() net.Addr                { return nil }
func (c *testDummyConn) RemoteAddr() net.Addr               { return nil }
func (c *testDummyConn) SetDeadline(t time.Time) error      { return nil }
func (c *testDummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *testDummyConn) SetWriteDeadline(t time.Time) error { return nil }

func (s *testPoolSuite) TestPool(c *C) {
	f := func(addr string) (*Conn, error) {
		return &Conn{closed: false, nc: &testDummyConn{}}, nil
	}

	capability := 4
	p := NewPool("127.0.0.1:6379", 4, f)

	conns := make([]*Conn, 0, capability+2)
	for i := 0; i < capability+2; i++ {
		conn, err := p.GetConn()
		c.Assert(err, IsNil)
		conns = append(conns, conn)
	}

	for i := 0; i < len(conns); i++ {
		p.PutConn(conns[i])
	}

	c.Assert(p.conns.Len(), Equals, capability)

	conn, err := p.GetConn()
	c.Assert(err, IsNil)
	conn.Close()
	p.PutConn(conn)

	c.Assert(p.conns.Len(), Equals, capability-1)

	p.Clear()
	c.Assert(p.conns.Len(), Equals, 0)
}
