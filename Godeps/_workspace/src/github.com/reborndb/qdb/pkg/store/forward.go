// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"github.com/ngaut/log"
)

type Forward struct {
	DB   uint32
	Op   string
	Args [][]byte
}

type ForwardHandler func(f *Forward) error

// Register the handler that will be called before db storage commit
func (s *Store) RegPreCommitHandler(h ForwardHandler) {
	if err := s.acquire(); err != nil {
		return
	}
	defer s.release()

	s.preCommitHandlers = append(s.preCommitHandlers, h)
}

// Register the handler that will be called after db storage committed
func (s *Store) RegPostCommitHandler(h ForwardHandler) {
	if err := s.acquire(); err != nil {
		return
	}
	defer s.release()

	s.postCommitHandlers = append(s.postCommitHandlers, h)
}

func (s *Store) travelPreCommitHandlers(f *Forward) {
	for _, h := range s.preCommitHandlers {
		if err := h(f); err != nil {
			log.Warningf("handle WillCommitHandler err - %s", err)
		}
	}
}

func (s *Store) travelPostCommitHandlers(f *Forward) {
	for _, h := range s.postCommitHandlers {
		if err := h(f); err != nil {
			log.Warningf("handle DidCommitHandler err - %s", err)
		}
	}
}
