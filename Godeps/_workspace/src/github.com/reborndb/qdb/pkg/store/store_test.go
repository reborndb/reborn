// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ngaut/log"
	"github.com/reborndb/qdb/pkg/engine/rocksdb"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct {
	s *Store
}

func (s *testStoreSuite) SetUpSuite(c *C) {
	s.s = testCreateStore(c)
}

func (s *testStoreSuite) TearDownSuite(c *C) {
	if s.s != nil {
		s.s.Close()
		s = nil
	}
}

func (s *testStoreSuite) checkCompact(c *C) {
	err := s.s.CompactAll()
	c.Assert(err, IsNil)
}

func (s *testStoreSuite) checkEmpty(c *C) {
	it := s.s.getIterator()
	defer s.s.putIterator(it)

	it.SeekToFirst()
	c.Assert(it.Error(), IsNil)
	c.Assert(it.Valid(), Equals, false)
}

func testCreateStore(c *C) *Store {
	base := fmt.Sprintf("/tmp/test_qdb/test_store")
	err := os.RemoveAll(base)
	c.Assert(err, IsNil)

	err = os.MkdirAll(base, 0700)
	c.Assert(err, IsNil)

	conf := rocksdb.NewDefaultConfig()
	testdb, err := rocksdb.Open(path.Join(base, "db"), conf, false)
	c.Assert(err, IsNil)

	s := New(testdb)
	return s
}

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

func sleepms(n int) {
	time.Sleep(time.Millisecond * time.Duration(n))
}
