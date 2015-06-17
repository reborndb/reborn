// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package engine

type Iterator interface {
	Close()
	SeekTo(key []byte) []byte
	SeekToFirst()
	SeekToLast()
	Valid() bool
	Next()
	Prev()
	Key() []byte
	Value() []byte
	Error() error
}
