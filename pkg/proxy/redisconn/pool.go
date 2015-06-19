// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redisconn

import (
	"container/list"
	"sync"
	"time"
)

const PoolIdleTimeoutSecond = 120

type CreateConnFunc func(addr string) (*Conn, error)

type poolConn struct {
	c    *Conn
	last time.Time
}

type Pool struct {
	m sync.Mutex

	f          CreateConnFunc
	addr       string
	capability int
	conns      *list.List
}

func NewPool(addr string, capability int, f CreateConnFunc) *Pool {
	p := new(Pool)
	p.f = f
	p.addr = addr
	p.capability = capability

	p.conns = list.New()

	return p
}

func (p *Pool) GetConn() (*Conn, error) {
	p.m.Lock()
	defer p.m.Unlock()

	n := time.Now()
	for p.conns.Len() > 0 {
		e := p.conns.Front()
		p.conns.Remove(e)

		c := e.Value.(*poolConn)
		if n.Sub(c.last) > time.Duration(PoolIdleTimeoutSecond)*time.Second {
			c.c.Close()
		} else {
			return c.c, nil
		}
	}

	return p.f(p.addr)
}

func (p *Pool) PutConn(c *Conn) {
	if c == nil || c.closed {
		return
	}

	p.m.Lock()
	defer p.m.Unlock()

	for p.conns.Len() >= p.capability {
		e := p.conns.Front()
		p.conns.Remove(e)
		e.Value.(*poolConn).c.Close()
	}

	p.conns.PushBack(&poolConn{c, time.Now()})
}

func (p *Pool) Clear() {
	p.m.Lock()
	defer p.m.Unlock()

	for p.conns.Len() > 0 {
		e := p.conns.Front()
		p.conns.Remove(e)
		e.Value.(*poolConn).c.Close()
	}
}

type Pools struct {
	m sync.Mutex

	capability int

	mpools map[string]*Pool

	f CreateConnFunc
}

func NewPools(capability int, f CreateConnFunc) *Pools {
	p := new(Pools)
	p.f = f
	p.capability = capability
	p.mpools = make(map[string]*Pool)
	return p
}

func (p *Pools) GetConn(addr string) (*Conn, error) {
	p.m.Lock()
	pool, ok := p.mpools[addr]
	if !ok {
		pool = NewPool(addr, p.capability, p.f)
		p.mpools[addr] = pool
	}
	p.m.Unlock()

	return pool.GetConn()
}

func (p *Pools) PutConn(c *Conn) {
	if c == nil || c.closed {
		return
	}

	p.m.Lock()
	pool, ok := p.mpools[c.addr]
	p.m.Unlock()
	if !ok {
		c.Close()
	} else {
		pool.PutConn(c)
	}
}

func (p *Pools) ClearPool(addr string) {
	p.m.Lock()
	pool, ok := p.mpools[addr]
	p.m.Unlock()
	if !ok {
		return
	}

	pool.Clear()
}

func (p *Pools) Clear() {
	p.m.Lock()
	defer p.m.Unlock()

	for _, pool := range p.mpools {
		pool.Clear()
	}

	p.mpools = map[string]*Pool{}
}
