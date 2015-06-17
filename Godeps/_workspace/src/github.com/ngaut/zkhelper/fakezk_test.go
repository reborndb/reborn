// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkhelper

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/ngaut/go-zookeeper/zk"
)

// Make sure Stat implements the interface.
var _ zk.Stat = stat{}

func TestBasic(t *testing.T) {
	conn := NewConn()
	defer conn.Close()

	// Make sure Conn implements the interface.
	var _ Conn = conn
	if _, err := conn.Create("/zk", nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	if _, err := conn.Create("/zk/foo", []byte("foo"), 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}
	data, _, err := conn.Get("/zk/foo")
	if err != nil {
		t.Fatalf("conn.Get: %v", err)
	}
	if string(data) != "foo" {
		t.Errorf("got %q, wanted %q", data, "foo")
	}

	if _, err := conn.Set("/zk/foo", []byte("bar"), -1); err != nil {
		t.Fatalf("conn.Set: %v", err)
	}

	data, _, err = conn.Get("/zk/foo")
	if err != nil {
		t.Fatalf("conn.Get: %v", err)
	}
	if string(data) != "bar" {
		t.Errorf("got %q, wanted %q", data, "bar")
	}

	// Try Set with the wrong version.
	if _, err := conn.Set("/zk/foo", []byte("bar"), 0); err == nil {
		t.Error("conn.Set with a wrong version: expected error")
	}

	// Try Get with a node that doesn't exist.
	if _, _, err := conn.Get("/zk/rabarbar"); err == nil {
		t.Error("conn.Get with a node that doesn't exist: expected error")
	}

	// Try Set with a node that doesn't exist.
	if _, err := conn.Set("/zk/barbarbar", []byte("bar"), -1); err == nil {
		t.Error("conn.Get with a node that doesn't exist: expected error")
	}

	// Try Create with a node that exists.
	if _, err := conn.Create("/zk/foo", []byte("foo"), 0, zk.WorldACL(zk.PermAll)); err == nil {
		t.Errorf("conn.Create with a node that exists: expected error")
	}
	// Try Create with a node whose parents don't exist.
	if _, err := conn.Create("/a/b/c", []byte("foo"), 0, zk.WorldACL(zk.PermAll)); err == nil {
		t.Errorf("conn.Create with a node whose parents don't exist: expected error")
	}

	if err := conn.Delete("/zk/foo", -1); err != nil {
		t.Errorf("conn.Delete: %v", err)
	}
	_, stat, err := conn.Exists("/zk/foo")
	if err != nil {
		t.Errorf("conn.Exists: %v", err)
	}
	if stat != nil {
		t.Errorf("/zk/foo should be deleted, got: %v", stat)
	}

}

func TestChildren(t *testing.T) {
	conn := NewConn()
	defer conn.Close()
	nodes := []string{"/zk", "/zk/foo", "/zk/bar"}
	wantChildren := []string{"bar", "foo"}
	for _, path := range nodes {
		if _, err := conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
			t.Fatalf("conn.Create: %v", err)
		}
	}
	children, _, err := conn.Children("/zk")
	if err != nil {
		t.Fatalf(`conn.Children("/zk"): %v`, err)
	}
	sort.Strings(children)
	if length := len(children); length != 2 {
		t.Errorf("children: got %v, wanted %v", children, wantChildren)
	}

	for i, path := range children {
		if wantChildren[i] != path {
			t.Errorf("children: got %v, wanted %v", children, wantChildren)
			break
		}
	}

}

func TestWatches(t *testing.T) {
	conn := NewConn()
	defer conn.Close()

	if _, err := conn.Create("/zk", nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	if _, err := conn.Create("/zk/foo", []byte("foo"), 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}
	_, _, watch, err := conn.ExistsW("/zk/foo")
	if err != nil {
		t.Errorf("conn.ExistsW: %v", err)
	}

	if err := conn.Delete("/zk/foo", -1); err != nil {
		t.Error(err)
	}

	if err := fireWatch(t, watch); err != nil {
		t.Error(err)
	}

	// Creating a child sends an event to ChildrenW.
	_, _, watch, err = conn.ChildrenW("/zk")
	if err != nil {
		t.Errorf(`conn.ChildrenW("/zk"): %v`, err)
	}
	if _, err := conn.Create("/zk/foo", nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	if err := fireWatch(t, watch); err != nil {
		t.Error(err)
	}
	// Updating sends an event to GetW.

	_, _, watch, err = conn.GetW("/zk")
	if err != nil {
		t.Errorf(`conn.GetW("/zk"): %v`, err)
	}

	if _, err := conn.Set("/zk", []byte("foo"), -1); err != nil {
		t.Errorf("conn.Set /zk: %v", err)
	}

	if err := fireWatch(t, watch); err != nil {
		t.Error(err)
	}

	// Deleting sends an event to ExistsW and to ChildrenW of the
	// parent.
	_, _, watch, err = conn.ExistsW("/zk/foo")
	if err != nil {
		t.Errorf("conn.ExistsW: %v", err)
	}

	_, _, parentWatch, err := conn.ChildrenW("/zk")
	if err != nil {
		t.Errorf(`conn.ChildrenW("/zk"): %v`, err)
	}

	if err := conn.Delete("/zk/foo", -1); err != nil {
		t.Errorf("conn.Delete: %v", err)
	}

	if err := fireWatch(t, watch); err != nil {
		t.Error(err)
	}
	if err := fireWatch(t, parentWatch); err != nil {
		t.Error(err)
	}
}

func fireWatch(t *testing.T, watch <-chan zk.Event) error {
	timer := time.NewTimer(50000 * time.Millisecond)
	select {
	case <-watch:
		// TODO(szopa): Figure out what's the exact type of
		// event.
		return nil
	case <-timer.C:
		return errors.New("timeout")
	}

	return errors.New("timeout")
}

func TestSequence(t *testing.T) {
	conn := NewConn()
	defer conn.Close()
	if _, err := conn.Create("/zk", nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
		t.Fatalf("conn.Create: %v", err)
	}

	newPath, err := conn.Create("/zk/", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}
	if wanted := "/zk/0000000001"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

	newPath, err = conn.Create("/zk/", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}

	if wanted := "/zk/0000000002"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

	if err := conn.Delete("/zk/0000000002", -1); err != nil {
		t.Fatalf("conn.Delete: %v", err)
	}

	newPath, err = conn.Create("/zk/", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}

	if wanted := "/zk/0000000003"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

	newPath, err = conn.Create("/zk/action_", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Errorf("conn.Create: %v", err)
	}

	if wanted := "/zk/action_0000000004"; newPath != wanted {
		t.Errorf("new path: got %q, wanted %q", newPath, wanted)
	}

}

/*
func TestFromFile(t *testing.T) {
	conn := NewConnFromFile(testfiles.Locate("fakezk_test_config.json"))

	keyspaces, _, err := conn.Children("/zk/testing/vt/ns")
	if err != nil {
		t.Errorf("conn.Children: %v", err)
	}
	if len(keyspaces) != 1 || keyspaces[0] != "test_keyspace" {
		t.Errorf("conn.Children returned bad value: %v", keyspaces)
	}

	data, _, err := conn.Get("/zk/testing/vt/ns/test_keyspace")
	if err != nil {
		t.Errorf("conn.Get(/zk/testing/vt/ns/test_keyspace): %v", err)
	}
	if !strings.Contains(string(data), "TabletTypes") {
		t.Errorf("conn.Get(/zk/testing/vt/ns/test_keyspace) returned bad value: %v", data)
	}

	data, _, err = conn.Get("/zk/testing/vt/ns/test_keyspace/0/master")
	if err != nil {
		t.Errorf("conn.Get(/zk/testing/vt/ns/test_keyspace/0/master): %v", err)
	}
	if !strings.Contains(string(data), "NamedPortMap") {
		t.Errorf("conn.Get(/zk/testing/vt/ns/test_keyspace/0/master) returned bad value: %v", data)
	}
}
*/
