// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"strings"

	"github.com/ngaut/log"
	redis "github.com/reborndb/go/redis/resp"
)

var globalCommands = make(map[string]CommandFunc)

func register(name string, f CommandFunc) {
	funcName := strings.ToLower(name)
	if _, ok := globalCommands[funcName]; ok {
		log.Fatalf("%s has been registered", name)
	}

	globalCommands[funcName] = f
}

// return a common RESP
type CommandFunc func(s Session, args [][]byte) (redis.Resp, error)

// return int64 RESP, or error RESP if err is not nil
type CommandIntFunc func(s Session, args [][]byte) (int64, error)

// return bulk string RESP, or error RESP if err is not nil
type CommandBulkStringFunc func(s Session, args [][]byte) ([]byte, error)

// return simple string RESP, or error RESP if err is not nil
type CommandSimpleStringFunc func(s Session, args [][]byte) (string, error)

// return array RESP, or error RESP if err is not nil
type CommandArrayFunc func(s Session, args [][]byte) ([][]byte, error)

// return OK simple string RESP if error is nil, or error RESP if err is not nil
type CommandOKFunc func(s Session, args [][]byte) error

func Register(name string, f CommandFunc) {
	register(name, f)
}

func RegisterIntReply(name string, f CommandIntFunc) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewInt(r), nil
	}

	register(name, v)
}

func RegisterBulkReply(name string, f CommandBulkStringFunc) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewBulkBytes(r), nil
	}

	register(name, v)
}

func RegisterStringReply(name string, f CommandSimpleStringFunc) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewString(r), nil
	}

	register(name, v)
}

func RegisterArrayReply(name string, f CommandArrayFunc) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		r, err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		ay := redis.NewArray()
		for _, b := range r {
			ay.AppendBulkBytes(b)
		}
		return ay, nil
	}

	register(name, v)
}

func RegisterOKReply(name string, f CommandOKFunc) {
	v := func(s Session, args [][]byte) (redis.Resp, error) {
		err := f(s, args)
		if err != nil {
			return toRespError(err)
		}
		return redis.NewString("OK"), nil

	}

	register(name, v)
}
