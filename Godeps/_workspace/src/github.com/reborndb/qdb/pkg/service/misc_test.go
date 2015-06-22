// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) TestPing(c *C) {
	s.checkString(c, "PONG", "ping")
}

func (s *testServiceSuite) TestEcho(c *C) {
	s.checkString(c, "hello", "echo", "hello")
}

func (s *testServiceSuite) TestFlushAll(c *C) {
	k := randomKey(c)
	s.checkNil(c, "get", k)
	s.checkInt(c, 5, "append", k, "hello")
	s.checkInt(c, 11, "append", k, " world")
	s.checkString(c, "hello world", "get", k)
	s.checkOK(c, "flushall")
	s.checkNil(c, "get", k)
}

func (s *testServiceSuite) TestAuth(c *C) {
	// only reuse testPoolConn for auth test
	pc := newTestPoolConn(s.conn)
	pc.checkString(c, "PONG", "ping")
	pc.checkOK(c, "config", "set", "requirepass", "123456")
	pc.checkContainError(c, "NOAUTH Authentication required", "ping")
	pc.checkContainError(c, "invalid password", "auth", "123")
	pc.checkContainError(c, "invalid password", "auth", "")
	pc.checkOK(c, "auth", "123456")
	pc.checkString(c, "PONG", "ping")
	pc.checkOK(c, "config", "set", "requirepass", "")
	pc.checkString(c, "PONG", "ping")
	pc.checkContainError(c, "Client sent AUTH, but no password is set", "auth", "123")
	pc.checkContainError(c, "Client sent AUTH, but no password is set", "auth", "")
}
