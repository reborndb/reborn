// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/ngaut/log"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

const (
	MaxSlotNum = 1024
)

func HashTag(key []byte) []byte {
	part := key
	if i := bytes.IndexByte(part, '{'); i != -1 {
		part = part[i+1:]
	} else {
		return key
	}
	if i := bytes.IndexByte(part, '}'); i != -1 {
		return part[:i]
	} else {
		return key
	}
}

func HashTagToSlot(tag []byte) uint32 {
	return crc32.ChecksumIEEE(tag) % MaxSlotNum
}

func HashKeyToSlot(key []byte) ([]byte, uint32) {
	tag := HashTag(key)
	return tag, HashTagToSlot(tag)
}

// SLOTSINFO [start] [count]
func (s *Store) SlotsInfo(db uint32, args [][]byte) (map[uint32]int64, error) {
	if len(args) > 2 {
		return nil, errArguments("len(args) = %d, expect <= 2", len(args))
	}

	var start, count int64 = 0, MaxSlotNum
	var err error
	switch len(args) {
	case 2:
		count, err = ParseInt(args[1])
		if err != nil {
			return nil, errArguments("parse args failed - %s", err)
		}

		fallthrough
	case 1:
		start, err = ParseInt(args[0])
		if err != nil {
			return nil, errArguments("parse args failed - %s", err)
		}
	}
	limit := start + count

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	m := make(map[uint32]int64)
	for slot := uint32(start); slot < uint32(limit) && slot < MaxSlotNum; slot++ {
		if key, err := firstKeyUnderSlot(s, db, slot); err != nil {
			return nil, err
		} else if key != nil {
			m[slot] = 1
		} else {
			m[slot] = 0
		}
	}
	return m, nil
}

// SLOTSRESTORE key ttlms value [key ttlms value ...]
func (s *Store) SlotsRestore(db uint32, args [][]byte) error {
	if len(args) == 0 || len(args)%3 != 0 {
		return errArguments("len(args) = %d, expect != 0 && mod 3 = 0", len(args))
	}

	objs := make([]*rdb.ObjEntry, len(args)/3)
	for i := 0; i < len(objs); i++ {
		key := args[i*3]
		ttlms, err := ParseInt(args[i*3+1])
		if err != nil {
			return errArguments("parse args failed - %s", err)
		}
		value := args[i*3+2]

		expireat := int64(0)
		if ttlms != 0 {
			if v, ok := TTLmsToExpireAt(ttlms); ok && v > 0 {
				expireat = v
			} else {
				return errArguments("parse args[%d] ttlms = %d", i*3+1, ttlms)
			}
		}

		obj, err := rdb.DecodeDump(value)
		if err != nil {
			return errArguments("decode args[%d] failed, %s", i*3+2, err)
		}

		objs[i] = &rdb.ObjEntry{
			DB:       db,
			Key:      key,
			ExpireAt: uint64(expireat),
			Value:    obj,
		}
	}

	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	ms := &markSet{}
	bt := engine.NewBatch()
	for i := len(objs) - 1; i >= 0; i-- {
		e := objs[i]
		if ms.Has(e.Key) {
			log.Debugf("[%d] restore batch, db = %d, key = %v, ignore", i, e.DB, e.Key)
			continue
		} else {
			log.Debugf("[%d] restore batch, db = %d, key = %v", i, e.DB, e.Key)
		}
		if err := s.restore(bt, e.DB, e.Key, int64(e.ExpireAt), e.Value); err != nil {
			log.Warningf("restore object failed, db = %d, key = %v, err = %s", e.DB, e.Key, err)
			return err
		}
		ms.Set(e.Key)
	}
	fw := &Forward{DB: db, Op: "SlotsRestore", Args: args}
	return s.commit(bt, fw)
}

// SLOTSMGRTSLOT host port timeout slot
func (s *Store) SlotsMgrtSlot(db uint32, args [][]byte) (int64, error) {
	if len(args) != 4 {
		return 0, errArguments("len(args) = %d, expect = 4", len(args))
	}

	host := string(args[0])
	port, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	ttlms, err := ParseInt(args[2])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	slot, err := ParseUint(args[3])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}

	var timeout = time.Duration(ttlms) * time.Millisecond
	if slot >= MaxSlotNum {
		return 0, errArguments("slot = %d", slot)
	}
	if timeout == 0 {
		timeout = time.Second
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	log.Debugf("migrate slot, addr = %s, timeout = %d, db = %d, slot = %d", addr, timeout, db, slot)

	key, err := firstKeyUnderSlot(s, db, uint32(slot))
	if err != nil || key == nil {
		return 0, err
	}
	return s.migrateOne(addr, timeout, db, key)
}

// SLOTSMGRTTAGSLOT host port timeout slot
func (s *Store) SlotsMgrtTagSlot(db uint32, args [][]byte) (int64, error) {
	if len(args) != 4 {
		return 0, errArguments("len(args) = %d, expect = 4", len(args))
	}

	host := string(args[0])
	port, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	ttlms, err := ParseInt(args[2])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	slot, err := ParseUint(args[3])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}

	var timeout = time.Duration(ttlms) * time.Millisecond
	if slot >= MaxSlotNum {
		return 0, errArguments("slot = %d", slot)
	}
	if timeout == 0 {
		timeout = time.Second
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	log.Debugf("migrate slot with tag, addr = %s, timeout = %d, db = %d, slot = %d", addr, timeout, db, slot)

	key, err := firstKeyUnderSlot(s, db, uint32(slot))
	if err != nil || key == nil {
		return 0, err
	}

	if tag := HashTag(key); len(tag) == len(key) {
		return s.migrateOne(addr, timeout, db, key)
	} else {
		return s.migrateTag(addr, timeout, db, tag)
	}
}

// SLOTSMGRTONE host port timeout key
func (s *Store) SlotsMgrtOne(db uint32, args [][]byte) (int64, error) {
	if len(args) != 4 {
		return 0, errArguments("len(args) = %d, expect = 4", len(args))
	}

	host := string(args[0])
	port, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	ttlms, err := ParseInt(args[2])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	key := args[3]

	var timeout = time.Duration(ttlms) * time.Millisecond
	if timeout == 0 {
		timeout = time.Second
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	log.Debugf("migrate one, addr = %s, timeout = %d, db = %d, key = %v", addr, timeout, db, key)

	return s.migrateOne(addr, timeout, db, key)
}

// SLOTSMGRTTAGONE host port timeout key
func (s *Store) SlotsMgrtTagOne(db uint32, args [][]byte) (int64, error) {
	if len(args) != 4 {
		return 0, errArguments("len(args) = %d, expect = 4", len(args))
	}

	host := string(args[0])
	port, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	ttlms, err := ParseInt(args[2])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	key := args[3]

	var timeout = time.Duration(ttlms) * time.Millisecond
	if timeout == 0 {
		timeout = time.Second
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	log.Debugf("migrate one with tag, addr = %s, timeout = %d, db = %d, key = %v", addr, timeout, db, key)

	if tag := HashTag(key); len(tag) == len(key) {
		return s.migrateOne(addr, timeout, db, key)
	} else {
		return s.migrateTag(addr, timeout, db, tag)
	}
}

func (s *Store) migrateOne(addr string, timeout time.Duration, db uint32, key []byte) (int64, error) {
	n, err := s.migrate(addr, timeout, db, key)
	if err != nil {
		log.Errorf("migrate one failed - %s", err)
		return 0, err
	}
	return n, nil
}

func (s *Store) migrateTag(addr string, timeout time.Duration, db uint32, tag []byte) (int64, error) {
	keys, err := allKeysWithTag(s, db, tag)
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	n, err := s.migrate(addr, timeout, db, keys...)
	if err != nil {
		log.Errorf("migrate tag failed - %s", err)
		return 0, err
	}
	return n, nil
}

func (s *Store) migrate(addr string, timeout time.Duration, db uint32, keys ...[]byte) (int64, error) {
	var rows []storeRow
	var bins []*rdb.BinEntry

	for i, key := range keys {
		o, bin, err := loadBinEntry(s, db, key)
		if err != nil {
			return 0, err
		}
		if o == nil {
			log.Debugf("[%d] missing, db = %d, key = %v", i, db, key)
			continue
		}

		rows = append(rows, o)
		if bin != nil {
			log.Debugf("[%d] migrate, db = %d, key = %v, expireat = %d", i, db, key, o.GetExpireAt())
			bins = append(bins, bin)
		} else {
			log.Debugf("[%d] expired, db = %d, key = %v, expireat = %d", i, db, key, o.GetExpireAt())
		}
	}

	if len(bins) != 0 {
		if err := doMigrate(addr, timeout, db, bins); err != nil {
			return 0, err
		}
	}

	if len(rows) == 0 {
		return 0, nil
	}

	bt := engine.NewBatch()
	for _, o := range rows {
		if err := o.deleteObject(s, bt); err != nil {
			return 0, err
		}
	}
	fw := &Forward{DB: db, Op: "Del"}
	for _, key := range keys {
		fw.Args = append(fw.Args, key)
	}
	return int64(len(rows)), s.commit(bt, fw)
}
