// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

const (
	MaxExpireAt = 1e15
)

func (s *Store) loadStoreRow(db uint32, key []byte, deleteIfExpired bool) (storeRow, error) {
	o, err := loadStoreRow(s, db, key)
	if err != nil || o == nil {
		return nil, err
	}
	if deleteIfExpired && o.IsExpired() {
		bt := engine.NewBatch()
		if err := o.deleteObject(s, bt); err != nil {
			return nil, err
		}
		fw := &Forward{DB: db, Op: "Del", Args: []interface{}{key}}
		return nil, s.commit(bt, fw)
	}
	return o, nil
}

func (s *Store) deleteIfExists(bt *engine.Batch, db uint32, key []byte) (bool, error) {
	o, err := s.loadStoreRow(db, key, false)
	if err != nil || o == nil {
		return false, err
	}
	return true, o.deleteObject(s, bt)
}

// DEL key [key ...]
func (s *Store) Del(db uint32, args ...interface{}) (int64, error) {
	if len(args) == 0 {
		return 0, errArguments("len(args) = %d, expect != 0", len(args))
	}

	keys := make([][]byte, len(args))
	for i := 0; i < len(keys); i++ {
		if err := parseArgument(args[i], &keys[i]); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", err)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	for _, key := range keys {
		_, err := s.loadStoreRow(db, key, true)
		if err != nil {
			return 0, err
		}
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, key := range keys {
		if !ms.Has(key) {
			exists, err := s.deleteIfExists(bt, db, key)
			if err != nil {
				return 0, err
			}
			if exists {
				ms.Set(key)
			}
		}
	}
	fw := &Forward{DB: db, Op: "Del", Args: args}
	return ms.Len(), s.commit(bt, fw)
}

// DUMP key
func (s *Store) Dump(db uint32, args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	var key []byte
	for i, ref := range []interface{}{&key} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadStoreRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	} else {
		x, err := o.loadObjectValue(s)
		if err != nil {
			return nil, err
		}
		return x, nil
	}
}

// TYPE key
func (s *Store) Type(db uint32, args ...interface{}) (ObjectCode, error) {
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

	o, err := s.loadStoreRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	return o.Code(), nil
}

// EXISTS key
func (s *Store) Exists(db uint32, args ...interface{}) (int64, error) {
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

	o, err := s.loadStoreRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	} else {
		return 1, nil
	}
}

// TTL key
func (s *Store) TTL(db uint32, args ...interface{}) (int64, error) {
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

	v, err := s.getExpireTTLms(db, key)
	if err != nil || v < 0 {
		return v, err
	}
	return v / 1e3, nil
}

// PTTL key
func (s *Store) PTTL(db uint32, args ...interface{}) (int64, error) {
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

	return s.getExpireTTLms(db, key)
}

func (s *Store) getExpireTTLms(db uint32, key []byte) (int64, error) {
	o, err := s.loadStoreRow(db, key, true)
	if err != nil {
		return 0, err
	}
	if o == nil {
		return -2, nil
	} else {
		ttlms, _ := ExpireAtToTTLms(o.GetExpireAt())
		return ttlms, nil
	}
}

func (s *Store) setExpireAt(db uint32, key []byte, expireat uint64) (int64, error) {
	o, err := s.loadStoreRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	bt := engine.NewBatch()
	if !IsExpired(expireat) {
		o.SetExpireAt(expireat)
		bt.Set(o.MetaKey(), o.MetaValue())
		fw := &Forward{DB: db, Op: "PExpireAt", Args: []interface{}{key, expireat}}
		return 1, s.commit(bt, fw)
	} else {
		_, err := s.deleteIfExists(bt, db, key)
		if err != nil {
			return 0, err
		}
		fw := &Forward{DB: db, Op: "Del", Args: []interface{}{key}}
		return 1, s.commit(bt, fw)
	}
}

// PERSIST key
func (s *Store) Persist(db uint32, args ...interface{}) (int64, error) {
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

	o, err := s.loadStoreRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	if o.GetExpireAt() == 0 {
		return 0, nil
	}

	fw := &Forward{DB: db, Op: "Persist", Args: args}
	bt := engine.NewBatch()
	o.SetExpireAt(0)
	bt.Set(o.MetaKey(), o.MetaValue())
	return 1, s.commit(bt, fw)
}

// EXPIRE key seconds
func (s *Store) Expire(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var ttls int64
	for i, ref := range []interface{}{&key, &ttls} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	expireat := uint64(0)
	if v, ok := TTLsToExpireAt(ttls); ok && v > 0 {
		expireat = v
	} else {
		return 0, errArguments("invalid ttls = %d", ttls)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.setExpireAt(db, key, expireat)
}

// PEXPIRE key milliseconds
func (s *Store) PExpire(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var ttlms int64
	for i, ref := range []interface{}{&key, &ttlms} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	expireat := uint64(0)
	if v, ok := TTLmsToExpireAt(ttlms); ok && v > 0 {
		expireat = v
	} else {
		return 0, errArguments("invalid ttlms = %d", ttlms)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.setExpireAt(db, key, expireat)
}

// EXPIREAT key timestamp
func (s *Store) ExpireAt(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var timestamp uint64
	for i, ref := range []interface{}{&key, &timestamp} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}
	if timestamp > MaxExpireAt/1e3 {
		return 0, errArguments("parse timestamp = %d", timestamp)
	}

	expireat := uint64(1)
	if timestamp != 0 {
		expireat = timestamp * 1e3
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.setExpireAt(db, key, expireat)
}

// PEXPIREAT key milliseconds-timestamp
func (s *Store) PExpireAt(db uint32, args ...interface{}) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect = 2", len(args))
	}

	var key []byte
	var mtimestamp uint64
	for i, ref := range []interface{}{&key, &mtimestamp} {
		if err := parseArgument(args[i], ref); err != nil {
			return 0, errArguments("parse args[%d] failed, %s", i, err)
		}
	}
	if mtimestamp > MaxExpireAt {
		return 0, errArguments("parse mtimestamp = %d", mtimestamp)
	}

	expireat := uint64(1)
	if mtimestamp != 0 {
		expireat = mtimestamp
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	return s.setExpireAt(db, key, expireat)
}

// RESTORE key ttlms value
func (s *Store) Restore(db uint32, args ...interface{}) error {
	if len(args) != 3 {
		return errArguments("len(args) = %d, expect = 3", len(args))
	}

	var key, value []byte
	var ttlms int64
	for i, ref := range []interface{}{&key, &ttlms, &value} {
		if err := parseArgument(args[i], ref); err != nil {
			return errArguments("parse args[%d] failed, %s", i, err)
		}
	}

	expireat := uint64(0)
	if ttlms != 0 {
		if v, ok := TTLmsToExpireAt(ttlms); ok && v > 0 {
			expireat = v
		} else {
			return errArguments("parse ttlms = %d", ttlms)
		}
	}

	obj, err := rdb.DecodeDump(value)
	if err != nil {
		return err
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	fw := &Forward{DB: db, Op: "Restore", Args: args}
	bt := engine.NewBatch()
	if err := s.restore(bt, db, key, expireat, obj); err != nil {
		return err
	}
	return s.commit(bt, fw)
}

func (s *Store) restore(bt *engine.Batch, db uint32, key []byte, expireat uint64, obj interface{}) error {
	_, err := s.deleteIfExists(bt, db, key)
	if err != nil {
		return err
	}

	if !IsExpired(expireat) {
		var o storeRow
		switch obj.(type) {
		default:
			return errors.Trace(ErrObjectValue)
		case rdb.String:
			o = newStringRow(db, key)
		case rdb.Hash:
			o = newHashRow(db, key)
		case rdb.List:
			o = newListRow(db, key)
		case rdb.ZSet:
			o = newZSetRow(db, key)
		case rdb.Set:
			o = newSetRow(db, key)
		}
		return o.storeObject(s, bt, expireat, obj)
	}

	log.Debugf("restore an expired object, db = %d, key = %v, expireat = %d", db, key, expireat)
	return nil
}

func (s *Store) CompactAll() error {
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()
	log.Infof("store is compacting all...")
	if err := s.compact([]byte{MetaCode}, []byte{MetaCode + 1}); err != nil {
		return err
	}
	if err := s.compact([]byte{DataCode}, []byte{DataCode + 1}); err != nil {
		return err
	}
	log.Infof("store is compacted")
	return nil
}

func (s *Store) Info() (string, error) {
	if err := s.acquire(); err != nil {
		return "", err
	}
	defer s.release()

	return s.db.Stats(), nil
}

func nowms() uint64 {
	return uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
}

func ExpireAtToTTLms(expireat uint64) (int64, bool) {
	switch {
	case expireat > MaxExpireAt:
		return -1, false
	case expireat == 0:
		return -1, true
	default:
		if now := nowms(); now < expireat {
			return int64(expireat - now), true
		} else {
			return 0, true
		}
	}
}

func TTLsToExpireAt(ttls int64) (uint64, bool) {
	if ttls < 0 || ttls > MaxExpireAt/1e3 {
		return 0, false
	}
	return TTLmsToExpireAt(ttls * 1e3)
}

func TTLmsToExpireAt(ttlms int64) (uint64, bool) {
	if ttlms < 0 || ttlms > MaxExpireAt {
		return 0, false
	}
	expireat := nowms() + uint64(ttlms)
	if expireat > MaxExpireAt {
		return 0, false
	}
	return expireat, true
}
