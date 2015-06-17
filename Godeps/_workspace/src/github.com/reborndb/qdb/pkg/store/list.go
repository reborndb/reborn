// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

var (
	ErrNoSuchList = errors.New("no such list")
	ErrOutOfRange = errors.New("index out of range")
)

type listRow struct {
	*storeRowHelper

	Lindex int64
	Rindex int64
	Index  int64
	Value  []byte
}

func newListRow(db uint32, key []byte) *listRow {
	o := &listRow{}
	o.lazyInit(db, key, newStoreRowHelper(db, key, ListCode))
	return o
}

func (o *listRow) lazyInit(db uint32, key []byte, h *storeRowHelper) {
	o.storeRowHelper = h
	o.dataKeyRefs = []interface{}{&o.Index}
	o.metaValueRefs = []interface{}{&o.Lindex, &o.Rindex}
	o.dataValueRefs = []interface{}{&o.Value}
}

func (o *listRow) deleteObject(s *Store, bt *engine.Batch) error {
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

func (o *listRow) storeObject(s *Store, bt *engine.Batch, expireat uint64, obj interface{}) error {
	list, ok := obj.(rdb.List)
	if !ok || len(list) == 0 {
		return errors.Trace(ErrObjectValue)
	}
	for i, e := range list {
		if len(e) != 0 {
			continue
		}
		return errArguments("list[%d], len(value) = %d", i, len(e))
	}

	for i, value := range list {
		o.Index, o.Value = int64(i), value
		bt.Set(o.DataKey(), o.DataValue())
	}
	o.Lindex, o.Rindex = 0, int64(len(list))
	o.ExpireAt = expireat
	bt.Set(o.MetaKey(), o.MetaValue())
	return nil
}

func (o *listRow) loadObjectValue(r storeReader) (interface{}, error) {
	list := make([][]byte, 0, int(o.Rindex-o.Lindex))
	for o.Index = o.Lindex; o.Index < o.Rindex; o.Index++ {
		_, err := o.LoadDataValue(r)
		if err != nil {
			return nil, err
		}
		list = append(list, o.Value)
	}
	return rdb.List(list), nil
}

func (s *Store) loadListRow(db uint32, key []byte, deleteIfExpired bool) (*listRow, error) {
	o, err := s.loadStoreRow(db, key, deleteIfExpired)
	if err != nil {
		return nil, err
	} else if o != nil {
		x, ok := o.(*listRow)
		if ok {
			return x, nil
		}
		return nil, errors.Trace(ErrNotList)
	}
	return nil, nil
}

// LINDEX key index
func (s *Store) LIndex(db uint32, args ...interface{}) ([]byte, error) {
	if len(args) != 2 {
		return nil, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var index int64
	for i, ref := range []interface{}{&key, &index} {
		if err := parseArgument(args[i], ref); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	o, err := s.loadListRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	o.Index = adjustIndex(index, o.Lindex, o.Rindex)
	if o.Index >= o.Lindex && o.Index < o.Rindex {
		_, err := o.LoadDataValue(s)
		if err != nil {
			return nil, err
		}
		return o.Value, nil
	} else {
		return nil, nil
	}
}

// LLEN key
func (s *Store) LLen(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	var key []byte
	for i, ref := range []interface{}{&key} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadListRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	return o.Rindex - o.Lindex, nil
}

// LRANGE key beg end
func (s *Store) LRange(db uint32, args ...interface{}) ([][]byte, error) {
	if len(args) != 3 {
		return nil, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var beg, end int64
	for i, ref := range []interface{}{&key, &beg, &end} {
		if err := parseArgument(args[i], ref); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadListRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	beg = maxIntValue(adjustIndex(beg, o.Lindex, o.Rindex), o.Lindex)
	end = minIntValue(adjustIndex(end, o.Lindex, o.Rindex), o.Rindex-1)
	if beg <= end {
		values := make([][]byte, 0, end-beg+1)
		for o.Index = beg; o.Index <= end; o.Index++ {
			_, err := o.LoadDataValue(s)
			if err != nil {
				return nil, err
			}
			values = append(values, o.Value)
		}
		return values, nil
	} else {
		return nil, nil
	}
}

// LSET key index value
func (s *Store) LSet(db uint32, args ...interface{}) error {
	if len(args) != 3 {
		return errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, value []byte
	var index int64
	for i, ref := range []interface{}{&key, &index, &value} {
		if err := parseArgument(args[i], ref); err != nil {
			return errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	o, err := s.loadListRow(db, key, true)
	if err != nil {
		return err
	}

	if o == nil {
		return errors.Trace(ErrNoSuchList)
	}

	o.Index = adjustIndex(index, o.Lindex, o.Rindex)
	if o.Index >= o.Lindex && o.Index < o.Rindex {
		o.Value = value
		bt := engine.NewBatch()
		bt.Set(o.DataKey(), o.DataValue())
		fw := &Forward{DB: db, Op: "LSet", Args: args}
		return s.commit(bt, fw)
	} else {
		return errors.Trace(ErrOutOfRange)
	}
}

// LTRIM key beg end
func (s *Store) LTrim(db uint32, args ...interface{}) error {
	if len(args) != 3 {
		return errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var beg, end int64
	for i, ref := range []interface{}{&key, &beg, &end} {
		if err := parseArgument(args[i], ref); err != nil {
			return errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	o, err := s.loadListRow(db, key, true)
	if err != nil || o == nil {
		return err
	}

	beg = maxIntValue(adjustIndex(beg, o.Lindex, o.Rindex), o.Lindex)
	end = minIntValue(adjustIndex(end, o.Lindex, o.Rindex), o.Rindex-1)
	if beg == o.Lindex && end == o.Rindex-1 {
		return nil
	}

	bt := engine.NewBatch()
	if beg <= end {
		for o.Index = o.Lindex; o.Index < beg; o.Index++ {
			bt.Del(o.DataKey())
		}
		for o.Index = o.Rindex - 1; o.Index > end; o.Index-- {
			bt.Del(o.DataKey())
		}
		o.Lindex, o.Rindex = beg, end+1
		bt.Set(o.MetaKey(), o.MetaValue())
	} else {
		for o.Index = o.Lindex; o.Index < o.Rindex; o.Index++ {
			bt.Del(o.DataKey())
		}
		bt.Del(o.MetaKey())
	}
	fw := &Forward{DB: db, Op: "LTrim", Args: args}
	return s.commit(bt, fw)
}

// LPOP key
func (s *Store) LPop(db uint32, args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	var key []byte
	for i, ref := range []interface{}{&key} {
		if err := parseArgument(args[i], ref); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadListRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	o.Index = o.Lindex
	if _, err := o.LoadDataValue(s); err != nil {
		return nil, err
	} else {
		bt := engine.NewBatch()
		bt.Del(o.DataKey())
		if o.Lindex++; o.Lindex < o.Rindex {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
		fw := &Forward{DB: db, Op: "LPop", Args: args}
		return o.Value, s.commit(bt, fw)
	}
}

// RPOP key
func (s *Store) RPop(db uint32, args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	var key []byte
	for i, ref := range []interface{}{&key} {
		if err := parseArgument(args[i], ref); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadListRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	o.Index = o.Rindex - 1
	if _, err := o.LoadDataValue(s); err != nil {
		return nil, err
	} else {
		bt := engine.NewBatch()
		bt.Del(o.DataKey())
		if o.Rindex--; o.Lindex < o.Rindex {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
		fw := &Forward{DB: db, Op: "RPop", Args: args}
		return o.Value, s.commit(bt, fw)
	}
}

// LPUSH key value [value ...]
func (s *Store) LPush(db uint32, args ...interface{}) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	var key []byte
	var values = make([][]byte, len(args)-1)
	if err := parseArgument(args[0], &key); err != nil {
		return 0, errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(values); i++ {
		if err := parseArgument(args[i+1], &values[i]); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i+1, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.lpush(db, key, true, values...)
}

// LPUSHX key value
func (s *Store) LPushX(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, value []byte
	for i, ref := range []interface{}{&key, &value} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.lpush(db, key, false, value)
}

// RPUSH key value [value ...]
func (s *Store) RPush(db uint32, args ...interface{}) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	var key []byte
	var values = make([][]byte, len(args)-1)
	if err := parseArgument(args[0], &key); err != nil {
		return 0, errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(values); i++ {
		if err := parseArgument(args[i+1], &values[i]); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i+1, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.rpush(db, key, true, values...)
}

// RPUSHX key value
func (s *Store) RPushX(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, value []byte
	for i, ref := range []interface{}{&key, &value} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.rpush(db, key, false, value)
}

func (s *Store) lpush(db uint32, key []byte, create bool, values ...[]byte) (int64, error) {
	o, err := s.loadListRow(db, key, true)
	if err != nil {
		return 0, err
	}

	if o == nil {
		if !create {
			return 0, nil
		}
		o = newListRow(db, key)
	}

	fw := &Forward{DB: db, Op: "LPush", Args: []interface{}{key}}
	bt := engine.NewBatch()
	for _, value := range values {
		o.Lindex--
		o.Index, o.Value = o.Lindex, value
		bt.Set(o.DataKey(), o.DataValue())
		fw.Args = append(fw.Args, value)
	}
	bt.Set(o.MetaKey(), o.MetaValue())
	return o.Rindex - o.Lindex, s.commit(bt, fw)
}

func (s *Store) rpush(db uint32, key []byte, create bool, values ...[]byte) (int64, error) {
	o, err := s.loadListRow(db, key, true)
	if err != nil {
		return 0, err
	}

	if o == nil {
		if !create {
			return 0, nil
		}
		o = newListRow(db, key)
	}

	fw := &Forward{DB: db, Op: "RPush", Args: []interface{}{key}}
	bt := engine.NewBatch()
	for _, value := range values {
		o.Index, o.Value = o.Rindex, value
		o.Rindex++
		bt.Set(o.DataKey(), o.DataValue())
		fw.Args = append(fw.Args, value)
	}
	bt.Set(o.MetaKey(), o.MetaValue())
	return o.Rindex - o.Lindex, s.commit(bt, fw)
}
