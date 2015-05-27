// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	log "github.com/ngaut/logging"

	"github.com/juju/errors"
	"github.com/ngaut/zkhelper"

	"github.com/reborndb/reborn/pkg/utils"
)

var (
	productName = "unit_test"
)

func waitForProxyMarkOffline(coordConn zkhelper.Conn, proxyName string) {
	_, _, c, _ := coordConn.GetW(path.Join(GetProxyPath(productName), proxyName))
	<-c
	info, _ := GetProxyInfo(coordConn, productName, proxyName)
	if info.State == PROXY_STATE_MARK_OFFLINE {
		SetProxyStatus(coordConn, productName, proxyName, PROXY_STATE_OFFLINE)
	}
}

func TestProxyOfflineInWaitActionReceiver(t *testing.T) {
	log.Info("test proxy offline when waiting action response")
	fakeCoordConn := zkhelper.NewConn()

	for i := 1; i <= 4; i++ {
		CreateProxyInfo(fakeCoordConn, productName, &ProxyInfo{
			Id:    strconv.Itoa(i),
			State: PROXY_STATE_ONLINE,
		})
		go waitForProxyMarkOffline(fakeCoordConn, strconv.Itoa(i))
	}

	lst, _ := ProxyList(fakeCoordConn, productName, nil)
	if len(lst) != 4 {
		t.Error("create proxy info error")
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		actionPath := path.Join(GetActionResponsePath(productName), fakeCoordConn.Seq2Str(1))
		//create test response for proxy 4, means proxy 1,2,3 are timeout
		fakeCoordConn.Create(path.Join(actionPath, "4"), nil,
			0, zkhelper.DefaultFileACLs())
	}()

	err := NewActionWithTimeout(fakeCoordConn, productName, ACTION_TYPE_SLOT_CHANGED, nil, "desc", true, 3*1000)
	if err != nil && err.Error() != ErrReceiverTimeout.Error() {
		t.Error(errors.ErrorStack(err))
	}

	for i := 1; i <= 3; i++ {
		if info, _ := GetProxyInfo(fakeCoordConn, productName, strconv.Itoa(i)); info.State != PROXY_STATE_OFFLINE {
			t.Error("shutdown offline proxy error")
		}
	}
}

func TestNewAction(t *testing.T) {
	fakeCoordConn := zkhelper.NewConn()
	err := NewAction(fakeCoordConn, productName, ACTION_TYPE_SLOT_CHANGED, nil, "desc", false)
	if err != nil {
		t.Error(errors.ErrorStack(err))
	}
	prefix := GetWatchActionPath(productName)
	if exist, _, err := fakeCoordConn.Exists(prefix); !exist {
		t.Error(errors.ErrorStack(err))
	}

	//test if response node exists
	d, _, err := fakeCoordConn.Get(prefix + "/0000000001")
	if err != nil {
		t.Error(errors.ErrorStack(err))
	}

	//test get action data
	d, _, err = fakeCoordConn.Get(GetActionResponsePath(productName) + "/0000000001")
	if err != nil {
		t.Error(errors.ErrorStack(err))
	}

	var action Action
	json.Unmarshal(d, &action)
	if action.Desc != "desc" || action.Type != ACTION_TYPE_SLOT_CHANGED {
		t.Error("create action error")
	}
}

func TestForceRemoveLock(t *testing.T) {
	fakeCoordConn := zkhelper.NewConn()
	zkLock := utils.GetZkLock(fakeCoordConn, productName)
	if zkLock == nil {
		t.Error("create lock error")
	}

	zkLock.Lock("force remove lock")
	zkPath := fmt.Sprintf("/zk/reborn/db_%s/LOCK", productName)
	children, _, err := fakeCoordConn.Children(zkPath)
	if err != nil {
		t.Error(err)
	}
	if len(children) == 0 {
		t.Error("create lock error")
	}
	ForceRemoveLock(fakeCoordConn, productName)
	children, _, err = fakeCoordConn.Children(zkPath)
	if err != nil {
		t.Error(err)
	}
	if len(children) != 0 {
		t.Error("remove lock error")
	}
}
