// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/juju/errors"
	log "github.com/ngaut/logging"
	respcoding "github.com/ngaut/resp"
)

const (
	RedisPoolMaxIdleNum        = 5
	RedisPoolIdleTimeoutSecond = 2400

	MultiOperatorNum = 128
)

type MultiOperator struct {
	q    chan *MulOp
	pool *redis.Pool
}

type MulOp struct {
	op     string
	keys   [][]byte
	result *[]byte
	wait   chan error
}

type pair struct {
	key []byte
	pos int
}

func getSlotMap(keys [][]byte) map[int][]*pair {
	slotmap := make(map[int][]*pair)
	for i, k := range keys { //get slots
		slot := mapKey2Slot(k)
		vec, exist := slotmap[slot]
		if !exist {
			vec = make([]*pair, 0)
		}
		vec = append(vec, &pair{key: k, pos: i})

		slotmap[slot] = vec
	}

	return slotmap
}

func newMultiOperator(server string, auth string) *MultiOperator {
	oper := &MultiOperator{q: make(chan *MulOp, MultiOperatorNum)}
	oper.pool = newPool(server, auth)
	for i := 0; i < MultiOperatorNum/2; i++ {
		go oper.work()
	}

	return oper
}

func newPool(server, auth string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     RedisPoolMaxIdleNum,
		IdleTimeout: RedisPoolIdleTimeoutSecond * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if len(auth) > 0 {
				if ok, err := c.Do("AUTH", auth); err != nil {
					c.Close()
					return nil, err
				} else if ok != "OK" {
					c.Close()
					return nil, errors.Errorf("auth err, need OK but got %s", ok)
				}
			}
			return c, err
		},
	}
}

func (oper *MultiOperator) handleMultiOp(op string, keys [][]byte, result *[]byte) error {
	wait := make(chan error, 1)
	oper.q <- &MulOp{op: op, keys: keys, result: result, wait: wait}
	return <-wait
}

func (oper *MultiOperator) work() {
	for mop := range oper.q {
		switch mop.op {
		case "MGET":
			oper.mget(mop)
		case "MSET":
			oper.mset(mop)
		case "DEL":
			oper.del(mop)
		}
	}
}

func (oper *MultiOperator) mgetResults(mop *MulOp) ([]byte, error) {
	slotmap := getSlotMap(mop.keys)
	results := make([]interface{}, len(mop.keys))
	conn := oper.pool.Get()
	defer conn.Close()
	for _, vec := range slotmap {
		req := make([]interface{}, 0, len(vec))
		for _, p := range vec {
			req = append(req, p.key)
		}

		replys, err := redis.Values(conn.Do("mget", req...))
		if err != nil {
			return nil, errors.Trace(err)
		}

		for i, reply := range replys {
			if reply != nil {
				results[vec[i].pos] = reply
			} else {
				results[vec[i].pos] = nil
			}
		}
	}

	b, err := respcoding.Marshal(results)
	return b, errors.Trace(err)
}

func (oper *MultiOperator) mget(mop *MulOp) {
	start := time.Now()
	defer func() {
		if sec := time.Since(start).Seconds(); sec > 2 {
			log.Warning("too long to do mget", sec)
		}
	}()

	b, err := oper.mgetResults(mop)
	if err != nil {
		mop.wait <- errors.Trace(err)
		return
	}
	*mop.result = b
	mop.wait <- errors.Trace(err)
}

func (oper *MultiOperator) delResults(mop *MulOp) ([]byte, error) {
	var results int64
	conn := oper.pool.Get()
	defer conn.Close()
	for _, k := range mop.keys {
		n, err := conn.Do("del", k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		results += n.(int64)
	}

	b, err := respcoding.Marshal(int(results))
	return b, errors.Trace(err)
}

func (oper *MultiOperator) msetResults(mop *MulOp) ([]byte, error) {
	// add mop.keys len check
	keysNum := len(mop.keys)
	if keysNum%2 != 0 {
		return nil, errors.Errorf("bad number of keys for mset command - %d", keysNum)
	}

	conn := oper.pool.Get()
	defer conn.Close()
	for i := 0; i < keysNum; i += 2 {
		_, err := conn.Do("set", mop.keys[i], mop.keys[i+1]) //change mset to set
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return OK_BYTES, nil
}

func (oper *MultiOperator) mset(mop *MulOp) {
	start := time.Now()
	defer func() { //todo:extra function
		if sec := time.Since(start).Seconds(); sec > 2 {
			log.Warning("too long to do del", sec)
		}
	}()

	b, err := oper.msetResults(mop)
	if err != nil {
		mop.wait <- errors.Trace(err)
		return
	}

	*mop.result = b
	mop.wait <- errors.Trace(err)
}

func (oper *MultiOperator) del(mop *MulOp) {
	start := time.Now()
	defer func() { //todo:extra function
		if sec := time.Since(start).Seconds(); sec > 2 {
			log.Warning("too long to do del", sec)
		}
	}()

	b, err := oper.delResults(mop)
	if err != nil {
		mop.wait <- errors.Trace(err)
		return
	}

	*mop.result = b
	mop.wait <- errors.Trace(err)
}
