// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/go/atomic2"
	"github.com/reborndb/go/io/ioutils"
	"github.com/reborndb/go/io/pipe"
	"github.com/reborndb/go/redis/rdb"
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

// BGSAVE
func BgsaveCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 {
		return toRespErrorf("len(args) = %d, expect = 0", len(args))
	}

	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	if ok := c.h.bgSaveSem.AcquireTimeout(time.Second); !ok {
		return toRespErrorf("wait others do bgsave timeout")
	}
	defer c.h.bgSaveSem.Release()

	sp, err := c.Store().NewSnapshot()
	if err != nil {
		return toRespError(err)
	}
	defer c.Store().ReleaseSnapshot(sp)

	if err := c.h.bgsaveTo(sp, c.h.config.DumpPath); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

// BGSAVETO path
func BgsaveToCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 1 {
		return toRespErrorf("len(args) = %d, expect = 1", len(args))
	}

	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	if ok := c.h.bgSaveSem.AcquireTimeout(time.Second); !ok {
		return toRespErrorf("wait others do bgsave timeout")
	}
	defer c.h.bgSaveSem.Release()

	sp, err := c.Store().NewSnapshot()
	if err != nil {
		return toRespError(err)
	}
	defer c.Store().ReleaseSnapshot(sp)

	if err := c.h.bgsaveTo(sp, string(args[0])); err != nil {
		return toRespError(err)
	} else {
		return redis.NewString("OK"), nil
	}
}

func (h *Handler) bgsaveTo(sp *store.StoreSnapshot, path string) error {
	h.counters.bgsave.Add(1)
	defer h.counters.bgsave.Sub(1)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	buf := bufio.NewWriterSize(f, 1024*1024)
	enc := rdb.NewEncoder(buf)

	if err := enc.EncodeHeader(); err != nil {
		return err
	}

	ncpu := runtime.GOMAXPROCS(0)
	cron := time.Millisecond * time.Duration(100)
	for {
		objs, more, err := sp.LoadObjCron(cron, ncpu, 1024)
		if err != nil {
			return err
		} else {
			for _, obj := range objs {
				if err := enc.EncodeObject(obj.DB, obj.Key, obj.ExpireAt, obj.Value); err != nil {
					return err
				}
			}
		}
		if !more {
			break
		}
	}

	if err := enc.EncodeFooter(); err != nil {
		return err
	}

	if err := errors.Trace(buf.Flush()); err != nil {
		return err
	}
	return errors.Trace(f.Close())
}

// SLAVEOF host port
func SlaveOfCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	addr := fmt.Sprintf("%s:%s", string(args[0]), string(args[1]))
	log.Infof("set slave of %s", addr)

	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	var cc *conn
	var err error
	if strings.ToLower(addr) != "no:one" {
		if cc, err = c.h.replicationConnectMaster(addr); err != nil {
			return toRespError(errors.Trace(err))
		}
	}

	select {
	case <-c.h.signal:
		if cc != nil {
			cc.Close()
		}
		return toRespErrorf("sync master has been closed")
	case c.h.master <- cc:
		<-c.h.slaveofReply
		return redis.NewString("OK"), nil
	}
}

func (h *Handler) replicationConnectMaster(addr string) (*conn, error) {
	h.masterConnState.Set(masterConnConnecting)
	nc, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// do AUTH if possible
	c := newConn(nc, h, 0)
	if len(h.config.MasterAuth) > 0 {
		if err = c.doMustOK("AUTH", h.config.MasterAuth); err != nil {
			c.Close()
			return nil, errors.Trace(err)
		}
	}

	if err := c.ping(); err != nil {
		c.Close()
		return nil, errors.Trace(err)
	}

	log.Infof("ping master %s ok", addr)

	seps := strings.Split(h.config.Listen, ":")
	if len(seps) == 2 {
		if err := c.doMustOK("REPLCONF", "listening-port", seps[1]); err != nil {
			c.Close()
			return nil, errors.Trace(err)
		}
	} else {
		log.Errorf("server listening addr %s has invalid port", h.config.Listen)
	}
	h.masterConnState.Set(masterConnConnected)
	return c, nil
}

const infinityDelay = 10 * 365 * 24 * 3600 * time.Second

func (h *Handler) daemonSyncMaster() {
	var last *conn
	lost := make(chan int, 0)

	h.masterRunID = "?"
	h.syncOffset.Set(-1)
	h.masterConnState.Set(masterConnNone)

	retryTimer := time.NewTimer(infinityDelay)
	defer retryTimer.Stop()

	var err error
LOOP:
	for exists := false; !exists; {
		var c *conn
		needSlaveofReply := false
		select {
		case <-lost:
			h.masterConnState.Set(masterConnConnect)
			// here means replication conn was broken, we will reconnect it
			last = nil
			h.syncSince.Set(0)

			log.Infof("replication connection from master %s was broken, try reconnect 1s later", h.masterAddr.Get())
			retryTimer.Reset(time.Second)
			continue LOOP
		case <-h.signal:
			exists = true
		case c = <-h.master:
			needSlaveofReply = true
		case <-retryTimer.C:
			log.Infof("retry connect to master %s", h.masterAddr.Get())
			c, err = h.replicationConnectMaster(h.masterAddr.Get())
			if err != nil {
				log.Errorf("repliaction retry connect master %s err, try 1s later again - %s", h.masterAddr.Get(), err)
				retryTimer.Reset(time.Second)
				continue LOOP
			}
		}

		retryTimer.Reset(infinityDelay)

		if last != nil {
			last.Close()
			<-lost
		}
		last = c
		if c != nil {
			masterAddr := c.nc.RemoteAddr().String()

			syncOffset := h.syncOffset.Get()
			if masterAddr == h.masterAddr.Get() && h.masterRunID != "?" {
				// sync same master with last synchronization
				syncOffset++
			} else {
				// last sync master is not same
				h.masterRunID = "?"
				h.syncOffset.Set(-1)
				syncOffset = -1
			}

			h.masterAddr.Set(masterAddr)

			go func(syncOffset int64) {
				defer func() {
					lost <- 0
				}()
				defer c.Close()
				err := h.psync(c, h.masterRunID, syncOffset)
				log.Warningf("slave %s do psync err - %s", c, err)
			}(syncOffset)

			h.syncSince.Set(time.Now().UnixNano() / int64(time.Millisecond))
			log.Infof("slaveof %s", h.masterAddr.Get())
		} else {
			h.masterAddr.Set("")
			h.syncOffset.Set(-1)
			h.masterRunID = "?"
			h.syncSince.Set(0)
			log.Infof("slaveof no one")
		}

		if needSlaveofReply {
			h.slaveofReply <- struct{}{}
		}
	}
}

func (h *Handler) parseFullResyncReply(resp string) (string, int64) {
	seps := strings.Split(resp, " ")
	if len(seps) != 3 || len(seps[1]) != 40 {
		log.Errorf("master %s returns invalid fullresync format %s", h.masterAddr, resp)
	}

	masterRunID := seps[1]
	initailSyncOffset, err := strconv.ParseInt(seps[2], 10, 64)
	if err != nil {
		log.Errorf("master %s returns invalid fullresync offset, err: %v", h.masterAddr, err)
		initailSyncOffset = -1
	}
	return masterRunID, initailSyncOffset
}

func (h *Handler) readSyncRDBSize(c *conn) (int64, error) {
	// wait rdb size line
	line, err := c.readLine()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if line[0] != '$' {
		return 0, errors.Errorf("invalid full sync response, rsp = '%s'", line)
	}

	n, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.Errorf("invalid full sync response = '%s', error = '%s', n = %d", line, err, n)
	}

	return n, nil
}

func (h *Handler) psync(c *conn, masterRunID string, syncOffset int64) error {
	// first, we send PSYNC command
	resp, err := c.prePSync(masterRunID, syncOffset)
	if err != nil {
		return errors.Trace(err)
	}

	resp = strings.ToLower(resp)
	rdbSize := int64(0)

	if resp == "+continue" {
		// do parital Resynchronization
		log.Infof("master %s support psync, start from %d now", h.masterAddr.Get(), syncOffset)
	} else {
		initialSyncOffset := int64(-1)
		h.syncOffset.Set(-1)

		if strings.HasPrefix(resp, "+fullresync") {
			// go here we need full resync
			h.masterRunID, initialSyncOffset = h.parseFullResyncReply(resp)
			log.Infof("start fullresync from %d", initialSyncOffset)
			h.syncOffset.Set(initialSyncOffset)
		} else {
			// here master does not support PSYNC, we use SYNC instead
			log.Errorf("master %s doesn't support PSYNC, reply is %s, try SYNC", h.masterAddr.Get(), resp)

			if err = c.sendCommand("SYNC"); err != nil {
				return errors.Trace(err)
			}
		}

		rdbSize, err = h.readSyncRDBSize(c)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return h.startSyncFromMaster(c, rdbSize)
}

func (h *Handler) openSyncPipe() (pipe.Reader, pipe.Writer) {
	filePath := h.config.SyncFilePath
	fileSize := h.config.SyncFileSize
	buffSize := h.config.SyncBuffSize

	var file *os.File
	if filePath != "" {
		f, err := pipe.OpenFile(filePath, false)
		if err != nil {
			log.Errorf("open pipe file '%s' failed - %s", filePath, err)
		} else {
			file = f
		}
	}

	pr, pw := pipe.PipeFile(buffSize, fileSize, file)

	return pr, pw
}

func (h *Handler) startSyncFromMaster(c *conn, size int64) error {
	defer func() {
		h.counters.syncTotalBytes.Set(0)
		h.counters.syncCacheBytes.Set(0)
	}()

	pr, pw := h.openSyncPipe()
	defer pr.Close()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	wg.Add(1)
	go func(r io.Reader) {
		defer wg.Done()
		defer pw.Close()
		p := make([]byte, 8192)
		for {
			deadline := time.Now().Add(time.Minute)
			if err := c.nc.SetReadDeadline(deadline); err != nil {
				pr.CloseWithError(errors.Trace(err))
				return
			}
			n, err := r.Read(p)
			if err != nil {
				pr.CloseWithError(errors.Trace(err))
				return
			}

			h.counters.syncTotalBytes.Add(int64(n))
			s := p[:n]
			for len(s) != 0 {
				n, err := pw.Write(s)
				if err != nil {
					pr.CloseWithError(errors.Trace(err))
					return
				}
				s = s[n:]
			}
		}
	}(c.r)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			time.Sleep(time.Millisecond * 200)
			n, err := pr.Buffered()
			if err != nil {
				return
			}
			h.counters.syncCacheBytes.Set(int64(n))
		}
	}()

	var counter atomic2.Int64
	c.r = bufio.NewReader(ioutils.NewCountReader(pr, &counter))

	if size > 0 {
		// we need full sync first
		if err := c.Store().Reset(); err != nil {
			return errors.Trace(err)
		}

		h.masterConnState.Set(masterConnSync)
		log.Infof("sync rdb file size = %d bytes\n", size)
		if err := h.doSyncRDB(c, size); err != nil {
			return errors.Trace(err)
		}
		log.Infof("sync rdb done")
	}

	h.masterConnState.Set(masterConnConnected)
	return h.doSyncFromMater(c, &counter)
}

func (h *Handler) doSyncFromMater(c *conn, counter *atomic2.Int64) error {
	c.authenticated = true

	lastACKTime := time.Now()
	for {
		readTotalSize := counter.Get()

		if _, err := c.handleRequest(h); err != nil {
			return errors.Trace(err)
		}

		if h.syncOffset.Get() != -1 {
			h.syncOffset.Add(counter.Get() - readTotalSize)

			n := time.Now()
			if n.Sub(lastACKTime) > time.Second {
				lastACKTime = n
				// this command has no reply
				if err := c.sendCommand("REPLCONF", "ACK", h.syncOffset.Get()); err != nil {
					log.Errorf("send REPLCONF ACK %d err - %s", h.syncOffset.Get(), err)
				}
			}
		}
	}

	return nil
}

func (h *Handler) doSyncRDB(c *conn, size int64) error {
	defer h.counters.syncRdbRemains.Set(0)
	h.counters.syncRdbRemains.Set(size)

	r := ioutils.NewCountReader(c.r, nil)
	l := rdb.NewLoader(r)
	if err := l.Header(); err != nil {
		return err
	}

	ncpu := runtime.GOMAXPROCS(0)
	errs := make(chan error, ncpu)

	var lock sync.Mutex
	var flag atomic2.Int64
	loadNextEntry := func() (*rdb.BinEntry, error) {
		lock.Lock()
		defer lock.Unlock()
		if flag.Get() != 0 {
			return nil, nil
		}
		entry, err := l.NextBinEntry()
		if err != nil || entry == nil {
			flag.Set(1)
			return nil, err
		}
		return entry, nil
	}

	for i := 0; i < ncpu; i++ {
		go func() {
			defer flag.Set(1)
			for {
				entry, err := loadNextEntry()
				if err != nil || entry == nil {
					errs <- err
					return
				}
				db, key, value := entry.DB, entry.Key, entry.Value
				ttlms := int64(0)
				if entry.ExpireAt != 0 {
					if v, ok := store.ExpireAtToTTLms(int64(entry.ExpireAt)); ok && v > 0 {
						ttlms = v
					} else {
						ttlms = 1
					}
				}
				if err := c.Store().SlotsRestore(db, [][]byte{key, store.FormatInt(ttlms), value}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	for {
		select {
		case <-time.After(time.Second):
			h.counters.syncRdbRemains.Set(size - r.Count())
		case err := <-errs:
			for i := 1; i < cap(errs); i++ {
				e := <-errs
				if err == nil && e != nil {
					err = e
				}
			}
			if err != nil {
				return err
			}
			return l.Footer()
		}
	}
}

func init() {
	Register("bgsave", BgsaveCmd)
	Register("bgsaveto", BgsaveToCmd)
	Register("slaveof", SlaveOfCmd)
}
