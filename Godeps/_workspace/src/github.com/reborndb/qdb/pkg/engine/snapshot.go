// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package engine

type Snapshot interface {
	Close()
	NewIterator() Iterator
	Get(key []byte) ([]byte, error)
}
