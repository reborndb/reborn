// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package atomic2

import (
	"sync"
	"sync/atomic"
	"time"
)

type Int64 int64

func (a *Int64) Get() int64 {
	return atomic.LoadInt64((*int64)(a))
}

func (a *Int64) Set(v int64) {
	atomic.StoreInt64((*int64)(a), v)
}

func (a *Int64) Reset() int64 {
	return atomic.SwapInt64((*int64)(a), 0)
}

func (a *Int64) Add(v int64) int64 {
	return atomic.AddInt64((*int64)(a), v)
}

func (a *Int64) Sub(v int64) int64 {
	return a.Add(-v)
}

func (a *Int64) Incr() int64 {
	return a.Add(1)
}

func (a *Int64) Decr() int64 {
	return a.Add(-1)
}

func (a *Int64) CompareAndSwap(oldval, newval int64) (swapped bool) {
	return atomic.CompareAndSwapInt64((*int64)(a), oldval, newval)
}

type String struct {
	mu  sync.Mutex
	str string
}

func (s *String) Set(str string) {
	s.mu.Lock()
	s.str = str
	s.mu.Unlock()
}

func (s *String) Get() string {
	s.mu.Lock()
	str := s.str
	s.mu.Unlock()
	return str
}

func (s *String) CompareAndSwap(oldval, newval string) (swqpped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.str == oldval {
		s.str = newval
		return true
	}
	return false
}

func (s *String) String() string {
	return s.Get()
}

type Duration int64

func (d *Duration) Add(duration time.Duration) time.Duration {
	return time.Duration(atomic.AddInt64((*int64)(d), int64(duration)))
}

func (d *Duration) Set(duration time.Duration) {
	atomic.StoreInt64((*int64)(d), int64(duration))
}

func (d *Duration) Get() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(d)))
}

func (d *Duration) CompareAndSwap(oldval, newval time.Duration) (swapped bool) {
	return atomic.CompareAndSwapInt64((*int64)(d), int64(oldval), int64(newval))
}
