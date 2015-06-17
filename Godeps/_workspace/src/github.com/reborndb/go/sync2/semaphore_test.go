// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package sync2

import (
	"testing"
	"time"
)

func TestSemaNoTimeout(t *testing.T) {
	s := NewSemaphore(1)
	s.Acquire()
	released := false
	go func() {
		time.Sleep(10 * time.Millisecond)
		released = true
		s.Release()
	}()
	s.Acquire()
	if !released {
		t.Errorf("want true, got false")
	}
}

func TestSemaTimeout(t *testing.T) {
	s := NewSemaphore(1)
	s.Acquire()
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.Release()
	}()
	if ok := s.AcquireTimeout(5 * time.Millisecond); ok {
		t.Errorf("want false, got true")
	}
	time.Sleep(10 * time.Millisecond)
	if ok := s.AcquireTimeout(5 * time.Millisecond); !ok {
		t.Errorf("want true, got false")
	}
}
