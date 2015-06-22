// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) TestCustomCommand(c *C) {
	RegisterOKReply("test_ok", func(s Session, args [][]byte) error {
		return nil
	})

	s.checkOK(c, "test_ok")

	RegisterIntReply("test_int", func(s Session, args [][]byte) (int64, error) {
		return 0, nil
	})

	s.checkInt(c, 0, "test_int")

	RegisterStringReply("test_string", func(s Session, args [][]byte) (string, error) {
		return "ABC", nil
	})

	s.checkString(c, "ABC", "test_string")

	RegisterBulkReply("test_bulk", func(s Session, args [][]byte) ([]byte, error) {
		return []byte("ABC"), nil
	})

	s.checkBytes(c, []byte("ABC"), "test_bulk")

	RegisterArrayReply("test_array", func(s Session, args [][]byte) ([][]byte, error) {
		return [][]byte{[]byte("1"), []byte("2")}, nil
	})

	ay := s.checkBytesArray(c, "test_array")
	c.Assert(ay, HasLen, 2)
	c.Assert(ay, DeepEquals, [][]byte{[]byte("1"), []byte("2")})
}
