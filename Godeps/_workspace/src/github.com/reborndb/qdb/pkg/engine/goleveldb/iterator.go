// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package goleveldb

import (
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type Iterator struct {
	iter iterator.Iterator
}

func (it *Iterator) Close() {
	it.iter.Release()
}

func (it *Iterator) SeekTo(key []byte) []byte {
	it.iter.Seek(key)
	return key
}

func (it *Iterator) SeekToFirst() {
	it.iter.First()
}

func (it *Iterator) SeekToLast() {
	it.iter.Last()
}

func (it *Iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *Iterator) Next() {
	it.iter.Next()
}

func (it *Iterator) Prev() {
	it.iter.Prev()
}

func (it *Iterator) Key() []byte {
	v := it.iter.Key()
	if v == nil {
		return nil
	}

	return append([]byte{}, v...)
}

func (it *Iterator) Value() []byte {
	v := it.iter.Value()
	if v == nil {
		return nil
	}

	return append([]byte{}, v...)
}

func (it *Iterator) Error() error {
	return it.iter.Error()
}
