// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package goleveldb

import (
	"github.com/juju/errors"
	"github.com/reborndb/qdb/pkg/engine"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Snapshot struct {
	snap *leveldb.Snapshot
	ropt *opt.ReadOptions
}

func newSnapshot(db *GoLevelDB) *Snapshot {
	snap, _ := db.lvdb.GetSnapshot()
	ropt := &opt.ReadOptions{}
	ropt.DontFillCache = true
	return &Snapshot{
		snap: snap,
		ropt: ropt,
	}
}

func (sp *Snapshot) Close() {
	sp.snap.Release()
}

func (sp *Snapshot) NewIterator() engine.Iterator {
	return &Iterator{
		iter: sp.snap.NewIterator(nil, sp.ropt),
	}
}

func (sp *Snapshot) Get(key []byte) ([]byte, error) {
	value, err := sp.snap.Get(key, sp.ropt)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	return value, errors.Trace(err)
}
