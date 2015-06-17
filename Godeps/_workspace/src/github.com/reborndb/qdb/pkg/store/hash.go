// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"math"

	"github.com/juju/errors"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

type hashRow struct {
	*storeRowHelper

	Size  int64
	Field []byte
	Value []byte
}

func newHashRow(db uint32, key []byte) *hashRow {
	o := &hashRow{}
	o.lazyInit(db, key, newStoreRowHelper(db, key, HashCode))
	return o
}

func (o *hashRow) lazyInit(db uint32, key []byte, h *storeRowHelper) {
	o.storeRowHelper = h
	o.dataKeyRefs = []interface{}{&o.Field}
	o.metaValueRefs = []interface{}{&o.Size}
	o.dataValueRefs = []interface{}{&o.Value}
}

func (o *hashRow) deleteObject(s *Store, bt *engine.Batch) error {
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

func (o *hashRow) storeObject(s *Store, bt *engine.Batch, expireat uint64, obj interface{}) error {
	hash, ok := obj.(rdb.Hash)
	if !ok || len(hash) == 0 {
		return errors.Trace(ErrObjectValue)
	}
	for i, e := range hash {
		if e == nil {
			return errArguments("hash[%d] is nil", i)
		}
		if len(e.Field) == 0 || len(e.Value) == 0 {
			return errArguments("hash[%d], len(field) = %d, len(value) = %d", i, len(e.Field), len(e.Value))
		}
	}

	ms := &markSet{}
	for _, e := range hash {
		o.Field, o.Value = e.Field, e.Value
		ms.Set(o.Field)
		bt.Set(o.DataKey(), o.DataValue())
	}
	o.Size, o.ExpireAt = ms.Len(), expireat
	bt.Set(o.MetaKey(), o.MetaValue())
	return nil
}

func (o *hashRow) loadObjectValue(r storeReader) (interface{}, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	hash := make([]*rdb.HashElement, 0, o.Size)
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
		hash = append(hash, &rdb.HashElement{Field: o.Field, Value: o.Value})
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if o.Size == 0 || int64(len(hash)) != o.Size {
		return nil, errors.Errorf("len(hash) = %d, hash.size = %d", len(hash), o.Size)
	}
	return rdb.Hash(hash), nil
}

func (o *hashRow) getAllFields(r storeReader) ([][]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	var fields [][]byte
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		sfx := key[len(pfx):]
		if err := o.ParseDataKeySuffix(sfx); err != nil {
			return nil, err
		}
		if len(o.Field) == 0 {
			return nil, errors.Errorf("len(field) = %d", len(o.Field))
		}
		fields = append(fields, o.Field)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if len(fields) == 0 || int64(len(fields)) != o.Size {
		return nil, errors.Errorf("len(fields) = %d, hash.size = %d", len(fields), o.Size)
	}
	return fields, nil
}

func (o *hashRow) getAllValues(r storeReader) ([][]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	var values [][]byte
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		if err := o.ParseDataValue(it.Value()); err != nil {
			return nil, err
		}
		if len(o.Value) == 0 {
			return nil, errors.Errorf("len(value) = %d", len(o.Value))
		}
		values = append(values, o.Value)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if len(values) == 0 || int64(len(values)) != o.Size {
		return nil, errors.Errorf("len(values) = %d, hash.size = %d", len(values), o.Size)
	}
	return values, nil
}

func (s *Store) loadHashRow(db uint32, key []byte, deleteIfExpired bool) (*hashRow, error) {
	o, err := s.loadStoreRow(db, key, deleteIfExpired)
	if err != nil {
		return nil, err
	} else if o != nil {
		x, ok := o.(*hashRow)
		if ok {
			return x, nil
		}
		return nil, errors.Trace(ErrNotHash)
	}
	return nil, nil
}

// HGETALL key
func (s *Store) HGetAll(db uint32, args ...interface{}) ([][]byte, error) {
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

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	x, err := o.loadObjectValue(s)
	if err != nil || x == nil {
		return nil, err
	}

	eles := x.(rdb.Hash)
	rets := make([][]byte, len(eles)*2)
	for i, e := range eles {
		rets[i*2], rets[i*2+1] = e.Field, e.Value
	}
	return rets, nil
}

// HDEL key field [field ...]
func (s *Store) HDel(db uint32, args ...interface{}) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	var key []byte
	var fields = make([][]byte, len(args)-1)
	if err := parseArgument(args[0], &key); err != nil {
		return 0, errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(fields); i++ {
		if err := parseArgument(args[i+1], &fields[i]); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i+1, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, o.Field = range fields {
		if !ms.Has(o.Field) {
			exists, err := o.TestDataValue(s)
			if err != nil {
				return 0, err
			}
			if exists {
				bt.Del(o.DataKey())
				ms.Set(o.Field)
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
	fw := &Forward{DB: db, Op: "HDel", Args: args}
	return n, s.commit(bt, fw)
}

// HEXISTS key field
func (s *Store) HExists(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, field []byte
	for i, ref := range []interface{}{&key, &field} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}

	o.Field = field
	exists, err := o.TestDataValue(s)
	if err != nil || !exists {
		return 0, err
	} else {
		return 1, nil
	}
}

// HGET key field
func (s *Store) HGet(db uint32, args ...interface{}) ([]byte, error) {
	if len(args) != 2 {
		return nil, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, field []byte
	for i, ref := range []interface{}{&key, &field} {
		if err := parseArgument(args[i], ref); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	o.Field = field
	exists, err := o.LoadDataValue(s)
	if err != nil || !exists {
		return nil, err
	} else {
		return o.Value, nil
	}
}

// HLEN key
func (s *Store) HLen(db uint32, args ...interface{}) (int64, error) {
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

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	return o.Size, nil
}

// HINCRBY key field delta
func (s *Store) HIncrBy(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, field []byte
	var delta int64
	for i, ref := range []interface{}{&key, &field, &delta} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var exists bool = false
	if o != nil {
		o.Field = field
		exists, err = o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
	} else {
		o = newHashRow(db, key)
		o.Field = field
	}

	bt := engine.NewBatch()
	if exists {
		v, err := ParseInt(o.Value)
		if err != nil {
			return 0, err
		}
		delta += v
	} else {
		o.Size++
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	o.Value = FormatInt(delta)
	bt.Set(o.DataKey(), o.DataValue())
	fw := &Forward{DB: db, Op: "HIncrBy", Args: args}
	return delta, s.commit(bt, fw)
}

// HINCRBYFLOAT key field delta
func (s *Store) HIncrByFloat(db uint32, args ...interface{}) (float64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, field []byte
	var delta float64
	for i, ref := range []interface{}{&key, &field, &delta} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var exists bool = false
	if o != nil {
		o.Field = field
		exists, err = o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
	} else {
		o = newHashRow(db, key)
		o.Field = field
	}

	bt := engine.NewBatch()
	if exists {
		v, err := ParseFloat(o.Value)
		if err != nil {
			return 0, err
		}
		delta += v
	} else {
		o.Size++
		bt.Set(o.MetaKey(), o.MetaValue())
	}

	if math.IsNaN(delta) || math.IsInf(delta, 0) {
		return 0, errors.Errorf("increment would produce NaN or Infinity")
	}

	o.Value = FormatFloat(delta)
	bt.Set(o.DataKey(), o.DataValue())
	fw := &Forward{DB: db, Op: "HIncrByFloat", Args: args}
	return delta, s.commit(bt, fw)
}

// HKEYS key
func (s *Store) HKeys(db uint32, args ...interface{}) ([][]byte, error) {
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

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}
	return o.getAllFields(s)
}

// HVALS key
func (s *Store) HVals(db uint32, args ...interface{}) ([][]byte, error) {
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

	o, err := s.loadHashRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}
	return o.getAllValues(s)
}

// HSET key field value
func (s *Store) HSet(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, field, value []byte
	for i, ref := range []interface{}{&key, &field, &value} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var exists bool = false
	if o != nil {
		o.Field = field
		exists, err = o.TestDataValue(s)
		if err != nil {
			return 0, err
		}
	} else {
		o = newHashRow(db, key)
		o.Field = field
	}

	var n int64

	bt := engine.NewBatch()
	if exists {
		n, o.Value = 0, value
		bt.Set(o.DataKey(), o.DataValue())
	} else {
		o.Size++
		n, o.Value = 1, value
		bt.Set(o.DataKey(), o.DataValue())
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	fw := &Forward{DB: db, Op: "HSet", Args: args}
	return n, s.commit(bt, fw)
}

// HSETNX key field value
func (s *Store) HSetNX(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key, field, value []byte
	for i, ref := range []interface{}{&key, &field, &value} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var exists bool = false
	if o != nil {
		o.Field = field
		exists, err = o.TestDataValue(s)
		if err != nil {
			return 0, err
		}
		if exists {
			return 0, nil
		}
	} else {
		o = newHashRow(db, key)
		o.Field = field
	}

	o.Size++
	o.Value = value
	bt := engine.NewBatch()
	bt.Set(o.DataKey(), o.DataValue())
	bt.Set(o.MetaKey(), o.MetaValue())
	fw := &Forward{DB: db, Op: "HSet", Args: args}
	return 1, s.commit(bt, fw)
}

// HMSET key field value [field value ...]
func (s *Store) HMSet(db uint32, args ...interface{}) error {
	if len(args) == 1 || len(args)%2 != 1 {
		return errArguments("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	var key []byte
	var eles = make([]*rdb.HashElement, len(args)/2)
	if err := parseArgument(args[0], &key); err != nil {
		return errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(eles); i++ {
		e := &rdb.HashElement{}
		if err := parseArgument(args[i*2+1], &e.Field); err != nil {
			return errArguments("parse args[%d] failed, %s", i*2+1, err)
		}
		if err := parseArgument(args[i*2+2], &e.Value); err != nil {
			return errArguments("parse args[%d] failed, %s", i*2+2, err)
		}
		eles[i] = e
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil {
		return err
	}

	if o == nil {
		o = newHashRow(db, key)
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, e := range eles {
		o.Field, o.Value = e.Field, e.Value
		exists, err := o.TestDataValue(s)
		if err != nil {
			return err
		}
		if !exists {
			ms.Set(o.Field)
		}
		bt.Set(o.DataKey(), o.DataValue())
	}

	n := ms.Len()
	if n != 0 {
		o.Size += n
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	fw := &Forward{DB: db, Op: "HMSet", Args: args}
	return s.commit(bt, fw)
}

// HMGET key field [field ...]
func (s *Store) HMGet(db uint32, args ...interface{}) ([][]byte, error) {
	if len(args) < 2 {
		return nil, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	var key []byte
	var fields = make([][]byte, len(args)-1)
	if err := parseArgument(args[0], &key); err != nil {
		return nil, errArguments("parse args[%d] failed, %s", 0, err)
	}
	for i := 0; i < len(fields); i++ {
		if err := parseArgument(args[i+1], &fields[i]); err != nil {
			return nil, errArguments("parse args[%d] failed, %s", i+1, err)
		}
	}
	var values = make([][]byte, len(fields))

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadHashRow(db, key, true)
	if err != nil {
		return nil, err
	}

	if o != nil {
		for i, field := range fields {
			o.Field = field
			exists, err := o.LoadDataValue(s)
			if err != nil {
				return nil, err
			}
			if exists {
				values[i] = o.Value
			}
		}
	}
	return values, nil
}
