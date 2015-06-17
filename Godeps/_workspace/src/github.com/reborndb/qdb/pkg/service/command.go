// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"fmt"
	"strings"

	redis "github.com/reborndb/go/redis/resp"
)

type CommandFunc func(s Session, args [][]byte) (redis.Resp, error)

var globalCommands = make(map[string]CommandFunc)

func Register(name string, f CommandFunc) {
	funcName := strings.ToLower(name)
	if _, ok := globalCommands[funcName]; ok {
		panic(fmt.Sprintf("%s has been registered", name))
	}

	globalCommands[funcName] = f
}
