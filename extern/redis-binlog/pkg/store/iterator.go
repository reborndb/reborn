// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

type Iterator interface {
	Close()
	SeekTo(key []byte) []byte
	SeekToFirst()
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Error() error
}
