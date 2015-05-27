package main

import (
	"fmt"
	"testing"

	"github.com/ngaut/zkhelper"
)

var (
	testProductName = "unit_test"
)

func TestMigrateManager(t *testing.T) {
	fakeCoordConn := zkhelper.NewConn()
	mgr := NewMigrateManager(fakeCoordConn, testProductName, nil)
	if mgr == nil {
		t.Error("mgr is null")
	}

	nodePath := fmt.Sprintf("/zk/reborn/db_%s/migrate_manager", testProductName)
	b, _, err := fakeCoordConn.Exists(nodePath)
	if !b || err != nil {
		t.Error("create migrate mgr node error")
	}

	err = mgr.removeNode()
	if err != nil {
		t.Error(err)
	}

	b, _, err = fakeCoordConn.Exists(nodePath)
	if b {
		t.Error("remove migrate mgr node error")
	}
}
