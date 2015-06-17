// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"container/list"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/qdb/pkg/engine"
)

var (
	ErrClosed = errors.New("store has been closed")
)

type Store struct {
	mu sync.Mutex
	db engine.Database

	splist list.List
	itlist list.List
	serial uint64

	preCommitHandlers  []ForwardHandler
	postCommitHandlers []ForwardHandler
}

func New(db engine.Database) *Store {
	s := &Store{db: db}

	s.preCommitHandlers = make([]ForwardHandler, 0)
	s.postCommitHandlers = make([]ForwardHandler, 0)

	return s
}

func (s *Store) Acquire() error {
	return s.acquire()
}

func (s *Store) Release() {
	s.release()
}

func (s *Store) acquire() error {
	s.mu.Lock()
	if s.db != nil {
		return nil
	}
	s.mu.Unlock()
	return errors.Trace(ErrClosed)
}

func (s *Store) release() {
	s.mu.Unlock()
}

func (s *Store) commit(bt *engine.Batch, fw *Forward) error {
	if bt.Len() == 0 {
		return nil
	}

	s.travelPreCommitHandlers(fw)

	if err := s.db.Commit(bt); err != nil {
		log.Warningf("store commit failed - %s", err)
		return err
	}
	for i := s.itlist.Len(); i != 0; i-- {
		v := s.itlist.Remove(s.itlist.Front()).(*storeIterator)
		v.Close()
	}
	s.serial++

	s.travelPostCommitHandlers(fw)

	return nil
}

func (s *Store) getRowValue(key []byte) ([]byte, error) {
	return s.db.Get(key)
}

func (s *Store) getIterator() (it *storeIterator) {
	if e := s.itlist.Front(); e != nil {
		return s.itlist.Remove(e).(*storeIterator)
	}
	return &storeIterator{
		Iterator: s.db.NewIterator(),
		serial:   s.serial,
	}
}

func (s *Store) putIterator(it *storeIterator) {
	if it.serial == s.serial && it.Error() == nil {
		s.itlist.PushFront(it)
	} else {
		it.Close()
	}
}

func (s *Store) Close() {
	if err := s.acquire(); err != nil {
		return
	}
	defer s.release()
	log.Infof("store is closing ...")
	for i := s.splist.Len(); i != 0; i-- {
		v := s.splist.Remove(s.splist.Front()).(*StoreSnapshot)
		v.Close()
	}
	for i := s.itlist.Len(); i != 0; i-- {
		v := s.itlist.Remove(s.itlist.Front()).(*storeIterator)
		v.Close()
	}
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
	log.Infof("store is closed")
}

func (s *Store) NewSnapshot() (*StoreSnapshot, error) {
	return s.NewSnapshotFunc(nil)
}

// New a snapshot and then call f if not nil
func (s *Store) NewSnapshotFunc(f func()) (*StoreSnapshot, error) {
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()
	sp := &StoreSnapshot{sp: s.db.NewSnapshot()}
	s.splist.PushBack(sp)
	log.Infof("store create new snapshot, address = %p", sp)

	if f != nil {
		f()
	}

	return sp, nil
}

func (s *Store) ReleaseSnapshot(sp *StoreSnapshot) {
	if err := s.acquire(); err != nil {
		return
	}
	defer s.release()
	log.Infof("store release snapshot, address = %p", sp)
	for i := s.splist.Len(); i != 0; i-- {
		v := s.splist.Remove(s.splist.Front()).(*StoreSnapshot)
		if v != sp {
			s.splist.PushBack(v)
		}
	}
	sp.Close()
}

func (s *Store) Reset() error {
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()
	log.Infof("store is reseting...")
	for i := s.splist.Len(); i != 0; i-- {
		v := s.splist.Remove(s.splist.Front()).(*StoreSnapshot)
		v.Close()
	}
	for i := s.itlist.Len(); i != 0; i-- {
		v := s.itlist.Remove(s.itlist.Front()).(*storeIterator)
		v.Close()
	}
	if err := s.db.Clear(); err != nil {
		s.db.Close()
		s.db = nil
		log.Errorf("store reset failed - %s", err)
		return err
	} else {
		s.serial++
		log.Infof("store is reset")
		return nil
	}
}

func (s *Store) compact(start, limit []byte) error {
	if err := s.db.Compact(start, limit); err != nil {
		log.Errorf("store compact failed - %s", err)
		return err
	} else {
		return nil
	}
}

func errArguments(format string, v ...interface{}) error {
	err := errors.Errorf(format, v...)
	log.Warningf("call store function with invalid arguments - %s", err)
	return err
}
