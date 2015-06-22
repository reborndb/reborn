// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"

	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

type storeIterator struct {
	engine.Iterator
	serial uint64
}

type storeReader interface {
	getRowValue(key []byte) ([]byte, error)
	getIterator() *storeIterator
	putIterator(it *storeIterator)
}

func loadObjEntry(r storeReader, db uint32, key []byte) (storeRow, *rdb.ObjEntry, error) {
	o, err := loadStoreRow(r, db, key)
	if err != nil || o == nil {
		return o, nil, err
	}
	if o.IsExpired() {
		return o, nil, nil
	}
	if val, err := o.loadObjectValue(r); err != nil {
		return o, nil, err
	} else {
		obj := &rdb.ObjEntry{
			DB:       db,
			Key:      key,
			Value:    val,
			ExpireAt: uint64(o.GetExpireAt()),
		}
		return o, obj, nil
	}
}

func loadBinEntry(r storeReader, db uint32, key []byte) (storeRow, *rdb.BinEntry, error) {
	o, obj, err := loadObjEntry(r, db, key)
	if err != nil || obj == nil {
		return o, nil, err
	}
	if bin, err := obj.BinEntry(); err != nil {
		return o, nil, err
	} else {
		return o, bin, nil
	}
}

func firstKeyUnderSlot(r storeReader, db uint32, slot uint32) ([]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	pfx := EncodeMetaKeyPrefixSlot(db, slot)
	if it.SeekTo(pfx); it.Valid() {
		metaKey := it.Key()
		if !bytes.HasPrefix(metaKey, pfx) {
			return nil, it.Error()
		}
		_, key, err := DecodeMetaKey(metaKey)
		if err != nil {
			return nil, err
		}
		return key, it.Error()
	}
	return nil, it.Error()
}

func allKeysWithTag(r storeReader, db uint32, tag []byte) ([][]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	var keys [][]byte
	pfx := EncodeMetaKeyPrefixTag(db, tag)
	for it.SeekTo(pfx); it.Valid(); it.Next() {
		metaKey := it.Key()
		if !bytes.HasPrefix(metaKey, pfx) {
			break
		}
		_, key, err := DecodeMetaKey(metaKey)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}
