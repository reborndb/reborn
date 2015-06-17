// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package engine_test

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/reborndb/qdb/pkg/engine"
	"github.com/reborndb/qdb/pkg/engine/goleveldb"
	"github.com/reborndb/qdb/pkg/engine/leveldb"
	"github.com/reborndb/qdb/pkg/engine/rocksdb"
	. "gopkg.in/check.v1"
)

func TestEngine(t *testing.T) {
	TestingT(t)
}

type testEngineSuite struct {
}

var _ = Suite(&testEngineSuite{})

func (s *testEngineSuite) testOpen(c *C, name string, conf interface{}) Database {
	path := fmt.Sprintf("/tmp/test_qdb/engine/%s", name)
	db, err := Open(name, path, conf, false)
	if err != nil && strings.Contains(err.Error(), "not registered") {
		c.Skip(err.Error())
	}
	c.Assert(err, IsNil)
	return db
}

func (s *testEngineSuite) testSimple(c *C, db Database) {
	batch := NewBatch()
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		batch.Set(key, value)
	}

	err := db.Commit(batch)
	c.Assert(err, IsNil)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value, err := db.Get(key)
		c.Assert(err, IsNil)
		c.Assert(string(value), Equals, fmt.Sprintf("value_%d", i))
	}

	batch.Reset()
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		batch.Del(key)
	}

	err = db.Commit(batch)
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value, err := db.Get(key)
		c.Assert(err, IsNil)
		c.Assert(value, IsNil)
	}

}

func (s *testEngineSuite) testIterator(c *C, db Database) {
	err := db.Clear()
	c.Assert(err, IsNil)

	batch := NewBatch()
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		batch.Set(key, value)
	}

	err = db.Commit(batch)
	c.Assert(err, IsNil)

	it := db.NewIterator()
	defer it.Close()

	it.SeekToFirst()
	c.Assert(it.Valid(), Equals, true)
	c.Assert(string(it.Key()), Equals, "key_0")
	c.Assert(string(it.Value()), Equals, "value_0")

	for i := 1; i < 10; i++ {
		it.Next()
		c.Assert(it.Valid(), Equals, true)
		c.Assert(string(it.Key()), Equals, fmt.Sprintf("key_%d", i))
		c.Assert(string(it.Value()), Equals, fmt.Sprintf("value_%d", i))
	}

	it.Next()
	c.Assert(it.Valid(), Equals, false)

	it.SeekToLast()
	c.Assert(it.Valid(), Equals, true)
	c.Assert(string(it.Key()), Equals, "key_9")
	c.Assert(string(it.Value()), Equals, "value_9")

	for i := 1; i < 10; i++ {
		it.Prev()
		c.Assert(it.Valid(), Equals, true)
		c.Assert(string(it.Key()), Equals, fmt.Sprintf("key_%d", 9-i))
		c.Assert(string(it.Value()), Equals, fmt.Sprintf("value_%d", 9-i))
	}

	it.Prev()
	c.Assert(it.Valid(), Equals, false)
}

func (s *testEngineSuite) testSnapshot(c *C, db Database) {
	err := db.Clear()
	c.Assert(err, IsNil)

	batch := NewBatch()
	batch.Set([]byte("key"), []byte("value"))

	err = db.Commit(batch)
	c.Assert(err, IsNil)

	snap := db.NewSnapshot()
	defer snap.Close()

	batch.Reset()
	batch.Set([]byte("key"), []byte("value_new"))

	err = db.Commit(batch)
	c.Assert(err, IsNil)

	value, err := snap.Get([]byte("key"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("value"))
}

func (s *testEngineSuite) test(c *C, name string, conf interface{}) {
	db := s.testOpen(c, name, conf)
	defer db.Close()

	s.testSimple(c, db)
	s.testIterator(c, db)
	s.testSnapshot(c, db)
}

func (s *testEngineSuite) TestRocksDB(c *C) {
	s.test(c, "rocksdb", rocksdb.NewDefaultConfig())
}

func (s *testEngineSuite) TestLevelDB(c *C) {
	s.test(c, "leveldb", leveldb.NewDefaultConfig())
}

func (s *testEngineSuite) TestGoLevelDB(c *C) {
	s.test(c, "goleveldb", goleveldb.NewDefaultConfig())
}
