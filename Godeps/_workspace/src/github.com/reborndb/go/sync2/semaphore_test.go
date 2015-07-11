// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package sync2

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSync2Suite{})

type testSync2Suite struct {
}

func (s *testSync2Suite) TestSemaNoTimeout(c *C) {
	sp := NewSemaphore(1)
	sp.Acquire()
	released := false

	go func() {
		time.Sleep(10 * time.Millisecond)
		released = true
		sp.Release()
	}()

	sp.Acquire()
	c.Assert(released, Equals, true)
}

func (s *testSync2Suite) TestSemaTimeout(c *C) {
	sp := NewSemaphore(1)
	sp.Acquire()

	go func() {
		time.Sleep(10 * time.Millisecond)
		sp.Release()
	}()

	ok := sp.AcquireTimeout(5 * time.Millisecond)
	c.Assert(ok, Equals, false)

	time.Sleep(10 * time.Millisecond)
	ok = sp.AcquireTimeout(5 * time.Millisecond)
	c.Assert(ok, Equals, true)
}
