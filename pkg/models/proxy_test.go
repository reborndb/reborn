// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"testing"

	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

var (
	productName = "unit_test"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testModelSuite{})

type testModelSuite struct {
}

func (s *testModelSuite) SetUpSuite(c *C) {
}

func (s *testModelSuite) TearDownSuite(c *C) {
}

func (s *testModelSuite) TestProxy(c *C) {
	log.Info("[TestProxy][start]")
	fakeCoordConn := zkhelper.NewConn()

	path := GetSlotBasePath(productName)
	children, _, _ := fakeCoordConn.Children(path)
	c.Assert(len(children), Equals, 0)

	g := NewServerGroup(productName, 1)
	g.Create(fakeCoordConn)

	// test create new group
	_, err := ServerGroups(fakeCoordConn, productName)
	c.Assert(err, IsNil)

	ok, err := g.Exists(fakeCoordConn)
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)

	s1 := NewServer(SERVER_TYPE_MASTER, "localhost:1111")

	g.AddServer(fakeCoordConn, s1)

	err = InitSlotSet(fakeCoordConn, productName, 1024)
	c.Assert(err, IsNil)

	children, _, err = fakeCoordConn.Children(path)
	c.Assert(err, IsNil)
	c.Assert(len(children), Equals, 1024)

	sl, err := GetSlot(fakeCoordConn, productName, 1)
	c.Assert(err, IsNil)
	c.Assert(sl.GroupId, Equals, -1)

	err = SetSlotRange(fakeCoordConn, productName, 0, 1023, 1, SLOT_STATUS_ONLINE)
	c.Assert(err, IsNil)

	pi := &ProxyInfo{
		ID:    "proxy_1",
		Addr:  "localhost:1234",
		State: PROXY_STATE_OFFLINE,
	}

	_, err = CreateProxyInfo(fakeCoordConn, productName, pi)
	c.Assert(err, IsNil)

	ps, err := ProxyList(fakeCoordConn, productName, nil)
	c.Assert(err, IsNil)
	c.Assert(len(ps), Equals, 1)
	c.Assert(ps[0].ID, Equals, "proxy_1")

	err = SetProxyStatus(fakeCoordConn, productName, pi.ID, PROXY_STATE_ONLINE)
	c.Assert(err, IsNil)

	p, err := GetProxyInfo(fakeCoordConn, productName, pi.ID)
	c.Assert(err, IsNil)
	c.Assert(p.State, Equals, PROXY_STATE_ONLINE)

	fakeCoordConn.Close()
	log.Info("[TestProxy][end]")
}
