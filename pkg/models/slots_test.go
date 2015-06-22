// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

func (s *testModelSuite) TestSlots(c *C) {
	log.Info("[TestSlots][start]")
	fakeCoordConn := zkhelper.NewConn()

	path := GetSlotBasePath(productName)
	children, _, _ := fakeCoordConn.Children(path)
	c.Assert(len(children), Equals, 0)

	err := InitSlotSet(fakeCoordConn, productName, 1024)
	c.Assert(err, IsNil)

	children, _, err = fakeCoordConn.Children(path)
	c.Assert(err, IsNil)
	c.Assert(len(children), Equals, 1024)

	sl, err := GetSlot(fakeCoordConn, productName, 1)
	c.Assert(err, IsNil)
	c.Assert(sl.GroupId, Equals, -1)

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	// test create new group
	_, err = ServerGroups(fakeCoordConn, productName)
	c.Assert(err, IsNil)

	ok, err := g.Exists(fakeCoordConn)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)

	err = SetSlotRange(fakeCoordConn, productName, 0, 1023, 1, SLOT_STATUS_ONLINE)
	c.Assert(err, IsNil)

	sl, err = GetSlot(fakeCoordConn, productName, 1)
	c.Assert(err, IsNil)
	c.Assert(sl.GroupId, Equals, 1)

	err = sl.SetMigrateStatus(fakeCoordConn, 1, 2)
	c.Assert(err, IsNil)
	c.Assert(sl.GroupId, Equals, 2)
	c.Assert(sl.State.Status, Equals, SLOT_STATUS_MIGRATE)

	fakeCoordConn.Close()
	log.Info("[TestSlots][end]")
}
