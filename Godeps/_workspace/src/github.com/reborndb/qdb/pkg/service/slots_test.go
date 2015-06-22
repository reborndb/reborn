// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import . "gopkg.in/check.v1"

func (s *testServiceSuite) slotCheckString(c *C, db uint32, key string, expect string) {
	nc := s.slotConnPool.Get(c)
	defer nc.Recycle()

	nc.checkOK(c, "SELECT", db)
	nc.checkString(c, expect, "GET", key)
}

func (s *testServiceSuite) TestSlotsHashKey(c *C) {
	s.checkIntArray(c, []int64{579, 1017, 879}, "slotshashkey", "a", "b", "c")
}

func (s *testServiceSuite) TestSlotsMgrtOne(c *C) {
	port := s.slotPort
	k1 := "{tag}" + randomKey(c)
	k2 := "{tag}" + randomKey(c)
	s.checkOK(c, "mset", k1, "1", k2, "2")
	s.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", port, 1000, k1)
	s.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", port, 1000, k1)
	s.slotCheckString(c, 0, k1, "1")

	s.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", port, 1000, k2)
	s.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", port, 1000, k2)
	s.slotCheckString(c, 0, k2, "2")

	s.checkOK(c, "set", k1, "3")

	s.checkInt(c, 1, "slotsmgrtone", "127.0.0.1", port, 1000, k1)
	s.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", port, 1000, k1)
	s.slotCheckString(c, 0, k1, "3")
}

func (s *testServiceSuite) TestSlotsMgrtTagOne(c *C) {
	port := s.slotPort

	k1 := "{tag}" + randomKey(c)
	k2 := "{tag}" + randomKey(c)
	k3 := "{tag}" + randomKey(c)
	s.checkOK(c, "mset", k1, "1", k2, "2")
	s.checkInt(c, 2, "slotsmgrttagone", "127.0.0.1", port, 1000, k1)
	s.checkInt(c, 0, "slotsmgrttagone", "127.0.0.1", port, 1000, k1)
	s.slotCheckString(c, 0, k1, "1")

	s.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", port, 1000, k2)
	s.slotCheckString(c, 0, k2, "2")

	s.checkOK(c, "mset", k1, "0", k3, "100")

	s.checkInt(c, 2, "slotsmgrttagone", "127.0.0.1", port, 1000, k1)
	s.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", port, 1000, k1)
	s.checkInt(c, 0, "slotsmgrtone", "127.0.0.1", port, 1000, k3)
	s.slotCheckString(c, 0, k1, "0")
	s.slotCheckString(c, 0, k3, "100")
}

func (s *testServiceSuite) TestSlotsMgrtSlot(c *C) {
	port := s.slotPort

	k1 := "{tag}" + randomKey(c)
	k2 := "{tag}" + randomKey(c)
	s.checkOK(c, "mset", k1, "1", k2, "2")
	s.checkIntArray(c, []int64{1, 1}, "slotsmgrtslot", "127.0.0.1", port, 1000, 899)
	s.checkIntArray(c, []int64{1, 1}, "slotsmgrtslot", "127.0.0.1", port, 1000, 899)
	s.checkIntArray(c, []int64{0, 0}, "slotsmgrtslot", "127.0.0.1", port, 1000, 899)

	s.slotCheckString(c, 0, k1, "1")
	s.slotCheckString(c, 0, k2, "2")
}

func (s *testServiceSuite) TestSlotsMgrtTagSlot(c *C) {
	port := s.slotPort

	k1 := "{tag}" + randomKey(c)
	k2 := "{tag}" + randomKey(c)
	k3 := "{tag}" + randomKey(c)
	s.checkOK(c, "mset", k1, "1", k2, "2", k3, "3")
	s.checkIntArray(c, []int64{3, 1}, "slotsmgrttagslot", "127.0.0.1", port, 1000, 899)
	s.checkIntArray(c, []int64{0, 0}, "slotsmgrttagslot", "127.0.0.1", port, 1000, 899)

	s.slotCheckString(c, 0, k1, "1")
	s.slotCheckString(c, 0, k2, "2")

	s.checkOK(c, "mset", k1, "0", k3, "100")
	s.checkIntArray(c, []int64{2, 1}, "slotsmgrttagslot", "127.0.0.1", port, 1000, 899)
	s.checkIntArray(c, []int64{0, 0}, "slotsmgrttagslot", "127.0.0.1", port, 1000, 899)
	s.slotCheckString(c, 0, k1, "0")
	s.slotCheckString(c, 0, k3, "100")
}
