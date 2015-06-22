// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// +build all rocksdb

package rocksdb

import (
	"github.com/juju/errors"
	"github.com/reborndb/qdb/extern/gorocks"
	"github.com/reborndb/qdb/pkg/engine"
)

type Snapshot struct {
	db *RocksDB

	snap *gorocks.Snapshot
	ropt *gorocks.ReadOptions
}

func newSnapshot(db *RocksDB, fillcache bool) *Snapshot {
	snap := db.rkdb.NewSnapshot()
	ropt := gorocks.NewReadOptions()
	ropt.SetFillCache(fillcache)
	ropt.SetSnapshot(snap)
	return &Snapshot{
		db:   db,
		snap: snap,
		ropt: ropt,
	}
}

func (sp *Snapshot) Close() {
	sp.ropt.Close()
	sp.db.rkdb.ReleaseSnapshot(sp.snap)
}

func (sp *Snapshot) NewIterator() engine.Iterator {
	return newIterator(sp.db, sp.ropt)
}

func (sp *Snapshot) Get(key []byte) ([]byte, error) {
	value, err := sp.db.rkdb.Get(sp.ropt, key)
	return value, errors.Trace(err)
}
