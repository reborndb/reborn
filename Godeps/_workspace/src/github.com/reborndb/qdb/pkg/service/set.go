// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import redis "github.com/reborndb/go/redis/resp"

// SADD key member [member ...]
func SAddCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().SAdd(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SCARD key
func SCardCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().SCard(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// SISMEMBER key member
func SIsMemberCmd(s Session, args [][]byte) (redis.Resp, error) {
	if x, err := s.Store().SIsMember(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(x), nil
	}
}

// SMEMBERS key
func SMembersCmd(s Session, args [][]byte) (redis.Resp, error) {
	if a, err := s.Store().SMembers(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// SPOP key
func SPopCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().SPop(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(v), nil
	}
}

// SRANDMEMBER key [count]
func SRandMemberCmd(s Session, args [][]byte) (redis.Resp, error) {
	if a, err := s.Store().SRandMember(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// SREM key member [member ...]
func SRemCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().SRem(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

func init() {
	Register("sadd", SAddCmd)
	Register("scard", SCardCmd)
	Register("sismember", SIsMemberCmd)
	Register("smembers", SMembersCmd)
	Register("spop", SPopCmd)
	Register("srandmember", SRandMemberCmd)
	Register("srem", SRemCmd)
}
