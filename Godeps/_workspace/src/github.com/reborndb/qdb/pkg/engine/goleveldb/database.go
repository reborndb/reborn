// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package goleveldb

import (
	"bytes"
	"fmt"
	"os"

	"github.com/juju/errors"
	"github.com/reborndb/qdb/pkg/engine"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type GoLevelDB struct {
	path string
	lvdb *leveldb.DB
	opts *opt.Options
	ropt *opt.ReadOptions
	wopt *opt.WriteOptions
}

func Open(path string, conf *Config, repair bool) (*GoLevelDB, error) {
	db := &GoLevelDB{}
	if err := db.init(path, conf, repair); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (db *GoLevelDB) init(path string, conf *Config, repair bool) error {
	if conf == nil {
		conf = NewDefaultConfig()
	}

	// Create path if not exists first
	if err := os.MkdirAll(path, 0700); err != nil {
		return errors.Trace(err)
	}

	opts := &opt.Options{}
	opts.ErrorIfMissing = false
	opts.ErrorIfExist = false

	opts.Filter = filter.NewBloomFilter(conf.BloomFilterSize)

	opts.Compression = opt.SnappyCompression

	opts.BlockSize = conf.BlockSize
	opts.WriteBuffer = conf.WriteBufferSize
	opts.OpenFilesCacheCapacity = conf.MaxOpenFiles

	opts.CompactionTableSize = 32 * 1024 * 1024
	opts.WriteL0SlowdownTrigger = 16
	opts.WriteL0PauseTrigger = 64

	db.path = path
	db.opts = opts
	db.ropt = nil
	db.wopt = nil

	if repair {
		if rdb, err := leveldb.RecoverFile(db.path, db.opts); err != nil {
			return errors.Trace(err)
		} else {
			db.lvdb = rdb
			return nil
		}
	}

	var err error
	if db.lvdb, err = leveldb.Open(storage.NewMemStorage(), db.opts); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (db *GoLevelDB) Clear() error {
	if db.lvdb != nil {
		db.lvdb.Close()
		db.lvdb = nil
		db.opts.ErrorIfMissing = false
		db.opts.ErrorIfExist = true

		if err := os.RemoveAll(db.path); err != nil {
			return errors.Trace(err)
		} else if db.lvdb, err = leveldb.OpenFile(db.path, db.opts); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (db *GoLevelDB) Close() {
	if db.lvdb != nil {
		db.lvdb.Close()
		db.lvdb = nil
	}
}

func (db *GoLevelDB) NewIterator() engine.Iterator {
	return &Iterator{
		iter: db.lvdb.NewIterator(nil, db.ropt),
	}
}

func (db *GoLevelDB) NewSnapshot() engine.Snapshot {
	return newSnapshot(db)
}

func (db *GoLevelDB) Get(key []byte) ([]byte, error) {
	value, err := db.lvdb.Get(key, db.ropt)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return value, errors.Trace(err)
}

func (db *GoLevelDB) Commit(bt *engine.Batch) error {
	if bt.OpList.Len() == 0 {
		return nil
	}
	wb := new(leveldb.Batch)

	for e := bt.OpList.Front(); e != nil; e = e.Next() {
		switch op := e.Value.(type) {
		case *engine.BatchOpSet:
			wb.Put(op.Key, op.Value)
		case *engine.BatchOpDel:
			wb.Delete(op.Key)
		default:
			panic(fmt.Sprintf("unsupported batch operation: %+v", op))
		}
	}
	return errors.Trace(db.lvdb.Write(wb, db.wopt))
}

func (db *GoLevelDB) Compact(start, limit []byte) error {
	r := util.Range{Start: start, Limit: limit}
	return db.lvdb.CompactRange(r)
}

func (db *GoLevelDB) Stats() string {
	var b bytes.Buffer
	for _, s := range []string{"leveldb.stats", "leveldb.sstables"} {
		v, _ := db.lvdb.GetProperty(s)
		fmt.Fprintf(&b, "[%s]\n%s\n", s, v)
	}
	return b.String()
}
