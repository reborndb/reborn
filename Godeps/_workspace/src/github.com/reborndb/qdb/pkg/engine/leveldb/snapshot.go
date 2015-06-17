// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// +build all leveldb

package leveldb

import (
	"github.com/juju/errors"
	"github.com/reborndb/qdb/extern/levigo"
	"github.com/reborndb/qdb/pkg/engine"
)

type Snapshot struct {
	db *LevelDB

	snap *levigo.Snapshot
	ropt *levigo.ReadOptions
}

func newSnapshot(db *LevelDB) *Snapshot {
	snap := db.lvdb.NewSnapshot()
	ropt := levigo.NewReadOptions()
	ropt.SetFillCache(false)
	ropt.SetSnapshot(snap)
	return &Snapshot{
		db:   db,
		snap: snap,
		ropt: ropt,
	}
}

func (sp *Snapshot) Close() {
	sp.ropt.Close()
	sp.db.lvdb.ReleaseSnapshot(sp.snap)
}

func (sp *Snapshot) NewIterator() engine.Iterator {
	return newIterator(sp.db, sp.ropt)
}

func (sp *Snapshot) Get(key []byte) ([]byte, error) {
	value, err := sp.db.lvdb.Get(sp.ropt, key)
	return value, errors.Trace(err)
}
