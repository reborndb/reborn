// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

type setRow struct {
	*storeRowHelper

	Size   int64
	Member []byte
}

func newSetRow(db uint32, key []byte) *setRow {
	o := &setRow{}
	o.lazyInit(db, key, newStoreRowHelper(db, key, SetCode))
	return o
}

func (o *setRow) lazyInit(db uint32, key []byte, h *storeRowHelper) {
	o.storeRowHelper = h
	o.dataKeyRefs = []interface{}{&o.Member}
	o.metaValueRefs = []interface{}{&o.Size}
	o.dataValueRefs = nil
}

func (o *setRow) deleteObject(s *Store, bt *engine.Batch) error {
	it := s.getIterator()
	defer s.putIterator(it)
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		bt.Del(key)
	}
	bt.Del(o.MetaKey())
	return it.Error()
}

func (o *setRow) storeObject(s *Store, bt *engine.Batch, expireat int64, obj interface{}) error {
	set, ok := obj.(rdb.Set)
	if !ok || len(set) == 0 {
		return errors.Trace(ErrObjectValue)
	}
	for i, m := range set {
		if len(m) != 0 {
			continue
		}
		return errArguments("set[%d], len(member) = %d", i, len(m))
	}

	ms := &markSet{}
	for _, o.Member = range set {
		ms.Set(o.Member)
		bt.Set(o.DataKey(), o.DataValue())
	}
	o.Size, o.ExpireAt = ms.Len(), expireat
	bt.Set(o.MetaKey(), o.MetaValue())
	return nil
}

func (o *setRow) loadObjectValue(r storeReader) (interface{}, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	set := make([][]byte, 0, o.Size)
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		sfx := key[len(pfx):]
		if err := o.ParseDataKeySuffix(sfx); err != nil {
			return nil, err
		}
		if err := o.ParseDataValue(it.Value()); err != nil {
			return nil, err
		}
		set = append(set, o.Member)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if o.Size == 0 || int64(len(set)) != o.Size {
		return nil, errors.Errorf("len(set) = %d, set.size = %d", len(set), o.Size)
	}
	return rdb.Set(set), nil
}

func (o *setRow) getMembers(r storeReader, count int64) ([][]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	var members [][]byte
	for pfx := it.SeekTo(o.DataKeyPrefix()); count > 0 && it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		sfx := key[len(pfx):]
		if err := o.ParseDataKeySuffix(sfx); err != nil {
			return nil, err
		}
		if err := o.ParseDataValue(it.Value()); err != nil {
			return nil, err
		}
		if len(o.Member) == 0 {
			return nil, errors.Errorf("len(member) = %d", len(o.Member))
		}
		members = append(members, o.Member)
		count--
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, errors.Errorf("len(members) = %d, set.size = %d", len(members), o.Size)
	}
	return members, nil
}

func (s *Store) loadSetRow(db uint32, key []byte, deleteIfExpired bool) (*setRow, error) {
	o, err := s.loadStoreRow(db, key, deleteIfExpired)
	if err != nil {
		return nil, err
	} else if o != nil {
		x, ok := o.(*setRow)
		if ok {
			return x, nil
		}
		return nil, errors.Trace(ErrNotSet)
	}
	return nil, nil
}

// SADD key member [member ...]
func (s *Store) SAdd(db uint32, args [][]byte) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	key := args[0]
	members := args[1:]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	if o == nil {
		o = newSetRow(db, key)
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, o.Member = range members {
		exists, err := o.TestDataValue(s)
		if err != nil {
			return 0, err
		}
		if !exists {
			ms.Set(o.Member)
		}
		bt.Set(o.DataKey(), o.DataValue())
	}

	n := ms.Len()
	if n != 0 {
		o.Size += n
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	fw := &Forward{DB: db, Op: "SAdd", Args: args}
	return n, s.commit(bt, fw)
}

// SCARD key
func (s *Store) SCard(db uint32, args [][]byte) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	return o.Size, nil
}

// SISMEMBER key member
func (s *Store) SIsMember(db uint32, args [][]byte) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	member := args[1]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}

	o.Member = member
	exists, err := o.TestDataValue(s)
	if err != nil || !exists {
		return 0, err
	} else {
		return 1, nil
	}
}

// SMEMBERS key
func (s *Store) SMembers(db uint32, args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	return o.getMembers(s, o.Size)
}

// SPOP key
func (s *Store) SPop(db uint32, args [][]byte) ([]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	members, err := o.getMembers(s, 1)
	if err != nil || len(members) == 0 {
		return nil, err
	}
	o.Member = members[0]

	bt := engine.NewBatch()
	bt.Del(o.DataKey())
	if o.Size--; o.Size > 0 {
		bt.Set(o.MetaKey(), o.MetaValue())
	} else {
		bt.Del(o.MetaKey())
	}
	fw := &Forward{DB: db, Op: "SRem", Args: [][]byte{key, members[0]}}
	return o.Member, s.commit(bt, fw)
}

// SRANDMEMBER key [count]
func (s *Store) SRandMember(db uint32, args [][]byte) ([][]byte, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, errArguments("len(args) = %d, expect = 1 or 2", len(args))
	}

	key := args[0]

	var count int64 = 1
	var err error
	if len(args) == 2 {
		count, err = ParseInt(args[1])
		if err != nil {
			return nil, errArguments("parse args failed - %s", err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	if count < 0 {
		count += o.Size
	}
	if count > 0 {
		return o.getMembers(s, count)
	} else {
		return nil, nil
	}
}

// SREM key member [member ...]
func (s *Store) SRem(db uint32, args [][]byte) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	key := args[0]
	members := args[1:]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, o.Member = range members {
		if !ms.Has(o.Member) {
			exists, err := o.TestDataValue(s)
			if err != nil {
				return 0, err
			}
			if exists {
				bt.Del(o.DataKey())
				ms.Set(o.Member)
			}
		}
	}

	n := ms.Len()
	if n != 0 {
		if o.Size -= n; o.Size > 0 {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
	}
	fw := &Forward{DB: db, Op: "SRem", Args: args}
	return n, s.commit(bt, fw)
}
