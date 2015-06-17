// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import redis "github.com/reborndb/go/redis/resp"

// LINDEX key index
func LIndexCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	if v, err := s.Store().LIndex(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(v), nil
	}
}

// LLEN key
func LLenCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if n, err := s.Store().LLen(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// LRANGE key beg end
func LRangeCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if a, err := s.Store().LRange(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// LSET key index value
func LSetCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if err := s.Store().LSet(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// LTRIM key beg end
func LTrimCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 3 {
		return toRespErrorf("len(args) = %d, expect = 3", len(args))
	}

	if err := s.Store().LTrim(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// LPOP key
func LPopCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if v, err := s.Store().LPop(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(v), nil
	}
}

// RPOP key
func RPopCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	if v, err := s.Store().RPop(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(v), nil
	}
}

// LPUSH key value [value ...]
func LPushCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := s.Store().LPush(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// LPUSHX key value [value ...]
func LPushXCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := s.Store().LPushX(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// RPUSH key value [value ...]
func RPushCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := s.Store().RPush(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// LPUSHX key value [value ...]
func RPushXCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) < 2 {
		return toRespErrorf("len(args) = %d, expect >= 2", len(args))
	}

	if n, err := s.Store().RPushX(s.DB(), iconvert(args)...); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

func init() {
	Register("lindex", LIndexCmd)
	Register("llen", LLenCmd)
	Register("lrange", LRangeCmd)
	Register("lset", LSetCmd)
	Register("ltrim", LTrimCmd)
	Register("lpop", LPopCmd)
	Register("rpop", RPopCmd)
	Register("lpush", LPushCmd)
	Register("lpushx", LPushXCmd)
	Register("rpush", RPushCmd)
	Register("rpushx", RPushXCmd)
}
