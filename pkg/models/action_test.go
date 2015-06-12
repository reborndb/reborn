// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/reborndb/reborn/pkg/utils"

	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	. "gopkg.in/check.v1"
)

func waitForProxyMarkOffline(coordConn zkhelper.Conn, proxyName string) {
	_, _, c, _ := coordConn.GetW(path.Join(GetProxyPath(productName), proxyName))

	<-c

	// test action need response, if proxy not responsed, then marked offline
	info, _ := GetProxyInfo(coordConn, productName, proxyName)
	if info.State == PROXY_STATE_MARK_OFFLINE {
		SetProxyStatus(coordConn, productName, proxyName, PROXY_STATE_OFFLINE)
	}
}

func (s *testModelSuite) TestProxyOfflineInWaitActionReceiver(c *C) {
	log.Info("[TestProxyOfflineInWaitActionReceiver][start]")
	fakeCoordConn := zkhelper.NewConn()

	proxyNum := 4
	for i := 1; i <= proxyNum; i++ {
		CreateProxyInfo(fakeCoordConn, productName, &ProxyInfo{
			ID:    strconv.Itoa(i),
			State: PROXY_STATE_ONLINE,
		})
		go waitForProxyMarkOffline(fakeCoordConn, strconv.Itoa(i))
	}

	lst, _ := ProxyList(fakeCoordConn, productName, nil)
	c.Assert(len(lst), Equals, proxyNum)

	go func() {
		time.Sleep(500 * time.Millisecond)
		actionPath := path.Join(GetActionResponsePath(productName), fakeCoordConn.Seq2Str(1))
		// create test response for proxy 4, means proxy 1,2,3 are timeout
		fakeCoordConn.Create(path.Join(actionPath, "4"), nil,
			0, zkhelper.DefaultFileACLs())
	}()

	err := NewActionWithTimeout(fakeCoordConn, productName, ACTION_TYPE_SLOT_CHANGED, nil, "desc", true, 3*1000)
	if c.Check(err, NotNil) {
		c.Assert(err.Error(), Equals, ErrReceiverTimeout.Error())
	}

	for i := 1; i <= proxyNum-1; i++ {
		info, _ := GetProxyInfo(fakeCoordConn, productName, strconv.Itoa(i))
		c.Assert(info.State, Equals, PROXY_STATE_OFFLINE)
	}

	fakeCoordConn.Close()
	log.Info("[TestProxyOfflineInWaitActionReceiver][end]")
}

func (s *testModelSuite) TestNewAction(c *C) {
	log.Info("[TestNewAction][start]")
	fakeCoordConn := zkhelper.NewConn()

	err := NewAction(fakeCoordConn, productName, ACTION_TYPE_SLOT_CHANGED, nil, "desc", false)
	c.Assert(err, IsNil)

	prefix := GetWatchActionPath(productName)
	exist, _, err := fakeCoordConn.Exists(prefix)
	c.Assert(exist, Equals, true)
	c.Assert(err, IsNil)

	// test if response node exists
	d, _, err := fakeCoordConn.Get(prefix + "/0000000001")
	c.Assert(err, IsNil)

	// test get action data
	d, _, err = fakeCoordConn.Get(GetActionResponsePath(productName) + "/0000000001")
	c.Assert(err, IsNil)

	var action Action
	json.Unmarshal(d, &action)
	c.Assert(action.Desc, Equals, "desc")
	c.Assert(action.Type, Equals, ACTION_TYPE_SLOT_CHANGED)

	fakeCoordConn.Close()
	log.Info("[TestNewAction][end]")
}

func (s *testModelSuite) TestForceRemoveLock(c *C) {
	log.Info("[TestForceRemoveLock][start]")
	fakeCoordConn := zkhelper.NewConn()

	zkLock := utils.GetCoordLock(fakeCoordConn, productName)
	c.Assert(zkLock, NotNil)

	zkLock.Lock("force remove lock")
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/LOCK", productName)
	children, _, err := fakeCoordConn.Children(coordPath)
	c.Assert(err, IsNil)
	c.Assert(len(children), Not(Equals), 0)

	ForceRemoveLock(fakeCoordConn, productName)
	children, _, err = fakeCoordConn.Children(coordPath)
	c.Assert(err, IsNil)
	c.Assert(len(children), Equals, 0)

	fakeCoordConn.Close()
	log.Info("[TestForceRemoveLock][end]")
}
