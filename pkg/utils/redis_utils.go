// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"net"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/juju/errors"
)

const (
	RedisConnConnectTimeout = 1 * time.Second
	RedisConnReadTimeout    = 1 * time.Second
	RedisConnWriteTimeout   = 1 * time.Second
)

func newRedisConn(addr string, auth string) (redis.Conn, error) {
	c, err := redis.DialTimeout("tcp", addr, RedisConnConnectTimeout, RedisConnReadTimeout, RedisConnWriteTimeout)
	if err != nil {
		return nil, err
	}

	if len(auth) > 0 {
		if _, err = c.Do("AUTH", auth); err != nil {
			c.Close()
			return nil, err
		}
	}

	return c, nil
}

// get redis's slot size
func SlotsInfo(addr string, fromSlot int, toSlot int, auth string) (map[int]int, error) {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	var (
		reply []interface{}
		val   []interface{}
	)

	reply, err = redis.Values(c.Do("SLOTSINFO", fromSlot, toSlot-fromSlot+1))
	if err != nil {
		return nil, err
	}

	ret := map[int]int{}
	for {
		if reply == nil || len(reply) == 0 {
			break
		}
		if reply, err = redis.Scan(reply, &val); err != nil {
			return nil, err
		}
		var slot, keyCount int
		_, err := redis.Scan(val, &slot, &keyCount)
		if err != nil {
			return nil, err
		}
		ret[slot] = keyCount
	}

	return ret, nil
}

func GetRedisStat(addr string, auth string) (map[string]string, error) {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	ret, err := redis.String(c.Do("INFO"))
	if err != nil {
		return nil, err
	}

	m := map[string]string{}
	lines := strings.Split(ret, "\n")
	for _, line := range lines {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) == 2 {
			k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
			m[k] = v
		}
	}

	var reply []string

	reply, err = redis.Strings(c.Do("CONFIG", "GET", "MAXMEMORY"))
	if err != nil {
		return nil, err
	}

	// we got result
	if len(reply) == 2 {
		if reply[1] != "0" {
			m["MAXMEMORY"] = reply[1]
		} else {
			m["MAXMEMORY"] = "∞"
		}
	}

	return m, nil
}

func GetRedisConfig(addr string, configName string, auth string) (string, error) {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return "", err
	}
	defer c.Close()

	ret, err := redis.Strings(c.Do("CONFIG", "GET", configName))
	if err != nil {
		return "", err
	}

	if len(ret) == 2 {
		return ret[1], nil
	}

	return "", nil
}

func SlaveOf(slave string, master string, auth string) error {
	if master == slave {
		return errors.New("can not slave of itself")
	}

	c, err := newRedisConn(slave, auth)
	if err != nil {
		return errors.Trace(err)
	}
	defer c.Close()

	host, port, err := net.SplitHostPort(master)
	if err != nil {
		return errors.Trace(err)
	}

	// Todo
	// Maybe we should set master auth for slave

	_, err = c.Do("SLAVEOF", host, port)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func SlaveNoOne(addr string, auth string) error {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return errors.Trace(err)
	}
	defer c.Close()

	_, err = c.Do("SLAVEOF", "NO", "ONE")
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func Ping(addr string, auth string) error {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return errors.Trace(err)
	}
	defer c.Close()

	pong, err := redis.String(c.Do("PING"))
	if err != nil {
		return errors.Trace(err)
	} else if pong != "PONG" {
		return errors.Errorf("ping needs pong, but we got %s", pong)
	}
	return nil
}

func GetRedisInfo(addr string, section string, auth string) (string, error) {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer c.Close()

	if len(section) > 0 {
		return redis.String(c.Do("INFO", section))
	} else {
		return redis.String(c.Do("INFO"))
	}
}

func GetRole(addr string, auth string) (string, error) {
	c, err := newRedisConn(addr, auth)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer c.Close()

	ay, err := redis.Values(c.Do("ROLE"))
	if err != nil {
		return "", errors.Trace(err)
	} else if len(ay) == 0 {
		return "", errors.Errorf("invalid reply for ROLE command")
	}

	// this first line is role type
	role, ok := ay[0].([]byte)
	if !ok {
		return "", errors.Errorf("invalid reply in first element for ROLE command")
	}
	return string(role), nil
}
