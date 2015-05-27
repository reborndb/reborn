// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"testing"

	"github.com/ngaut/zkhelper"
)

func TestSlots(t *testing.T) {
	fakeCoordConn := zkhelper.NewConn()
	path := GetSlotBasePath(productName)
	children, _, _ := fakeCoordConn.Children(path)
	if len(children) != 0 {
		t.Error("slot is no empty")
	}

	err := InitSlotSet(fakeCoordConn, productName, 1024)
	if err != nil {
		t.Error(err)
	}

	children, _, _ = fakeCoordConn.Children(path)
	if len(children) != 1024 {
		t.Error("init slots error")
	}

	s, err := GetSlot(fakeCoordConn, productName, 1)
	if err != nil {
		t.Error(err)
	}

	if s.GroupId != -1 {
		t.Error("init slots error")
	}

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	// test create new group
	_, err = ServerGroups(fakeCoordConn, productName)
	if err != nil {
		t.Error(err)
	}

	ok, err := g.Exists(fakeCoordConn)
	if !ok || err != nil {
		t.Error("create group error")
	}

	err = SetSlotRange(fakeCoordConn, productName, 0, 1023, 1, SLOT_STATUS_ONLINE)
	if err != nil {
		t.Error(err)
	}

	s, err = GetSlot(fakeCoordConn, productName, 1)
	if err != nil {
		t.Error(err)
	}

	if s.GroupId != 1 {
		t.Error("range set error")
	}

	err = s.SetMigrateStatus(fakeCoordConn, 1, 2)
	if err != nil {
		t.Error(err)
	}

	if s.GroupId != 2 || s.State.Status != SLOT_STATUS_MIGRATE {
		t.Error("migrate error")
	}

}
