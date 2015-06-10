// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"testing"

	"github.com/ngaut/zkhelper"
)

var (
	auth = ""
)

func TestProxy(t *testing.T) {
	fakeCoordConn := zkhelper.NewConn()
	path := GetSlotBasePath(productName)
	children, _, _ := fakeCoordConn.Children(path)
	if len(children) != 0 {
		t.Error("slot is no empty")
	}

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	// test create new group
	_, err := ServerGroups(fakeCoordConn, productName)
	if err != nil {
		t.Error(err)
	}

	ok, err := g.Exists(fakeCoordConn)
	if !ok || err != nil {
		t.Error("create group error")
	}

	s1 := NewServer(SERVER_TYPE_MASTER, "localhost:1111")

	g.AddServer(fakeCoordConn, s1, auth)

	err = InitSlotSet(fakeCoordConn, productName, 1024)
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

	err = SetSlotRange(fakeCoordConn, productName, 0, 1023, 1, SLOT_STATUS_ONLINE)
	if err != nil {
		t.Error(err)
	}

	pi := &ProxyInfo{
		ID:    "proxy_1",
		Addr:  "localhost:1234",
		State: PROXY_STATE_OFFLINE,
	}

	_, err = CreateProxyInfo(fakeCoordConn, productName, pi)
	if err != nil {
		t.Error(err)
	}

	ps, err := ProxyList(fakeCoordConn, productName, nil)
	if err != nil {
		t.Error(err)
	}

	if len(ps) != 1 || ps[0].ID != "proxy_1" {
		t.Error("create proxy error")
	}

	err = SetProxyStatus(fakeCoordConn, productName, pi.ID, PROXY_STATE_ONLINE)
	if err != nil {
		t.Error(err)
	}

	p, err := GetProxyInfo(fakeCoordConn, productName, pi.ID)
	if err != nil {
		t.Error(err)
	}

	if p.State != PROXY_STATE_ONLINE {
		t.Error("change status error")
	}
}
