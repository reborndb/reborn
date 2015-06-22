// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package engine

type Database interface {
	Close()
	Clear() error
	NewIterator() Iterator
	NewSnapshot() Snapshot
	Commit(bt *Batch) error
	Compact(start, limit []byte) error
	Get(key []byte) ([]byte, error)
	Stats() string
}
