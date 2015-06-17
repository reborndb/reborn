// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// SLOTSRESTORE key ttlms value [key ttlms value ...]
func SlotsRestoreCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) == 0 || len(args)%3 != 0 {
		return toRespErrorf("len(args) = %d, expect != 0 && mod 3 == 0", len(args))
	}

	if err := s.Store().SlotsRestore(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// SLOTSMGRTSLOT host port timeout slot
func SlotsMgrtSlotCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 4", len(args))
	}

	if n, err := s.Store().SlotsMgrtSlot(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		resp.AppendInt(n)
		if n != 0 {
			resp.AppendInt(1)
		} else {
			resp.AppendInt(0)
		}
		return resp, nil
	}
}

// SLOTSMGRTTAGSLOT host port timeout slot
func SlotsMgrtTagSlotCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 4", len(args))
	}

	if n, err := s.Store().SlotsMgrtTagSlot(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		resp.AppendInt(n)
		if n != 0 {
			resp.AppendInt(1)
		} else {
			resp.AppendInt(0)
		}
		return resp, nil
	}
}

// SLOTSMGRTONE host port timeout key
func SlotsMgrtOneCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 4", len(args))
	}

	if n, err := s.Store().SlotsMgrtOne(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SLOTSMGRTTAGONE host port timeout key
func SlotsMgrtTagOneCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 4 {
		return toRespErrorf("len(args) = %d, expect = 4", len(args))
	}

	if n, err := s.Store().SlotsMgrtTagOne(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SLOTSINFO [start [count]]
func SlotsInfoCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) > 2 {
		return toRespErrorf("len(args) = %d, expect <= 2", len(args))
	}

	if m, err := s.Store().SlotsInfo(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for i := uint32(0); i < store.MaxSlotNum; i++ {
			v, ok := m[i]
			if ok {
				s := redis.NewArray()
				s.AppendInt(int64(i))
				s.AppendInt(v)
				resp.Append(s)
			}
		}
		return resp, nil
	}
}

// SLOTSHASHKEY key [key...]
func SlotsHashKeyCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) == 0 {
		return toRespErrorf("len(args) = %d, expect != 1", len(args))
	}

	resp := redis.NewArray()
	for _, key := range args {
		_, slot := store.HashKeyToSlot(key)
		resp.AppendInt(int64(slot))
	}
	return resp, nil
}

func init() {
	Register("slotsrestore", SlotsRestoreCmd)
	Register("slotsmgrtslot", SlotsMgrtSlotCmd)
	Register("slotsmgrttagslot", SlotsMgrtTagSlotCmd)
	Register("slotsmgrtone", SlotsMgrtOneCmd)
	Register("slotsmgrttagone", SlotsMgrtTagOneCmd)
	Register("slotsinfo", SlotsInfoCmd)
	Register("slotshashkey", SlotsHashKeyCmd)
}
