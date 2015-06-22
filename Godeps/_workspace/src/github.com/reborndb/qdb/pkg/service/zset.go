// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// ZGETALL key
func ZGetAllCmd(s Session, args [][]byte) (redis.Resp, error) {
	if a, err := s.Store().ZGetAll(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range a {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZCARD key
func ZCardCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().ZCard(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZADD key score member [score member ...]
func ZAddCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().ZAdd(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZREM key member [member ...]
func ZRemCmd(s Session, args [][]byte) (redis.Resp, error) {
	if n, err := s.Store().ZRem(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(n), nil
	}
}

// ZSCORE key member
func ZScoreCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, ok, err := s.Store().ZScore(s.DB(), args); err != nil {
		return toRespError(err)
	} else if !ok {
		return redis.NewBulkBytes(nil), nil
	} else {
		return redis.NewBulkBytes(store.FormatFloat(v)), nil
	}
}

// ZINCRBY key delta member
func ZIncrByCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZIncrBy(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewBulkBytes(store.FormatFloat(v)), nil
	}
}

// ZCOUNT key min max
func ZCountCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZCount(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZLEXCOUNT key min max
func ZLexCountCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZLexCount(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZRANGE key start stop [WITHSCORES]
func ZRangeCmd(s Session, args [][]byte) (redis.Resp, error) {
	if ay, err := s.Store().ZRange(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZREVRANGE key start stop [WITHSCORES]
func ZRevRangeCmd(s Session, args [][]byte) (redis.Resp, error) {
	if ay, err := s.Store().ZRevRange(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZRANGEBYLEX key start stop [LIMIT offset count]
func ZRangeByLexCmd(s Session, args [][]byte) (redis.Resp, error) {
	if ay, err := s.Store().ZRangeByLex(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZREVRANGEBYLEX key start stop [LIMIT offset count]
func ZRevRangeByLexCmd(s Session, args [][]byte) (redis.Resp, error) {
	if ay, err := s.Store().ZRevRangeByLex(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func ZRangeByScoreCmd(s Session, args [][]byte) (redis.Resp, error) {
	if ay, err := s.Store().ZRangeByScore(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZREVRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func ZRevRangeByScoreCmd(s Session, args [][]byte) (redis.Resp, error) {
	if ay, err := s.Store().ZRevRangeByScore(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		resp := redis.NewArray()
		for _, v := range ay {
			resp.AppendBulkBytes(v)
		}
		return resp, nil
	}
}

// ZRANK key member
func ZRankCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZRank(s.DB(), args); err != nil {
		return toRespError(err)
	} else if v >= 0 {
		return redis.NewInt(v), nil
	} else {
		return redis.NewBulkBytes(nil), nil
	}
}

// ZREVRANK key member
func ZRevRankCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZRevRank(s.DB(), args); err != nil {
		return toRespError(err)
	} else if v >= 0 {
		return redis.NewInt(v), nil
	} else {
		return redis.NewBulkBytes(nil), nil
	}
}

// ZREMRANGEBYLEX key min max
func ZRemRangeByLexCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZRemRangeByLex(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZREMRANGEBYRANK key start stop
func ZRemRangeByRankCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZRemRangeByRank(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

// ZREMRANGEBYSCORE key min max
func ZRemRangeByScoreCmd(s Session, args [][]byte) (redis.Resp, error) {
	if v, err := s.Store().ZRemRangeByScore(s.DB(), args); err != nil {
		return toRespError(err)
	} else {
		return redis.NewInt(v), nil
	}
}

func init() {
	Register("zgetall", ZGetAllCmd)
	Register("zcard", ZCardCmd)
	Register("zadd", ZAddCmd)
	Register("zrem", ZRemCmd)
	Register("zscore", ZScoreCmd)
	Register("zincrby", ZIncrByCmd)
	Register("zcount", ZCountCmd)
	Register("zlexcount", ZLexCountCmd)
	Register("zrange", ZRangeCmd)
	Register("zrevrange", ZRevRangeCmd)
	Register("zrangebylex", ZRangeByLexCmd)
	Register("zrevrangebylex", ZRevRangeByLexCmd)
	Register("zrangebyscore", ZRangeByScoreCmd)
	Register("zrevrangebyscore", ZRevRangeByScoreCmd)
	Register("zrank", ZRankCmd)
	Register("zrevrank", ZRevRankCmd)
	Register("zremrangebylex", ZRemRangeByLexCmd)
	Register("zremrangebyrank", ZRemRangeByRankCmd)
	Register("zremrangebyscore", ZRemRangeByScoreCmd)
}
