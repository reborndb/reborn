// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/go/bytesize"
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/go/ring"
	"github.com/reborndb/qdb/pkg/store"
)

func (h *Handler) initReplication(bl *store.Store) error {
	h.repl.Lock()
	defer h.repl.Unlock()

	h.repl.slaves = make(map[*conn]chan struct{})

	h.repl.lastSelectDB.Set(int64(math.MaxUint32))

	bl.RegPostCommitHandler(h.replicationFeedSlaves)

	go func() {
		for {
			pingPeriod := time.Duration(h.config.ReplPingSlavePeriod) * time.Second
			select {
			case <-h.signal:
				return
			case <-time.After(pingPeriod):
				f := &store.Forward{Op: "PING",
					DB:   uint32(h.repl.lastSelectDB.Get()),
					Args: nil}
				if err := h.replicationFeedSlaves(f); err != nil {
					// ping slaves
					log.Errorf("ping slaves error - %s", err)
				}
			}
		}
	}()

	return nil
}

func (h *Handler) closeReplication() error {
	h.repl.Lock()
	defer h.repl.Unlock()

	// notice all slave to quit replication
	for c, ch := range h.repl.slaves {
		delete(h.repl.slaves, c)
		close(ch)
	}

	// need wait all slave replication done later???

	return h.destoryReplicationBacklog()
}

func (h *Handler) createReplicationBacklog() error {
	var err error
	bufSize := h.config.ReplBacklogSize

	// minimal backlog bufsize is 1MB
	if bufSize < bytesize.MB {
		bufSize = bytesize.MB
	}

	start := time.Now()
	if path := h.config.ReplBacklogFilePath; len(path) == 0 {
		h.repl.backlogBuf, err = ring.NewMemRing(bufSize)
	} else {
		h.repl.backlogBuf, err = ring.NewFileRing(path, bufSize)
	}

	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("create backlog buf with size %d cost %s", bufSize, time.Now().Sub(start).String())

	h.repl.backlogBuf.Reset()

	// Increment the global replication offset by one to make sure
	// we will not PSYNC with any previos slave.
	h.repl.masterOffset++

	// To make sure we don't have any data in replication buffer.
	h.repl.backlogOffset = h.repl.masterOffset + 1

	return nil
}

func (h *Handler) destoryReplicationBacklog() error {
	if h.repl.backlogBuf == nil {
		return nil
	}

	err := h.repl.backlogBuf.Close()
	h.repl.backlogBuf = nil
	return errors.Trace(err)
}

func (h *Handler) feedReplicationBacklog(buf []byte) error {
	h.repl.masterOffset += int64(len(buf))

	_, err := h.repl.backlogBuf.Write(buf)
	if err != nil {
		log.Errorf("write replication backlog err, reset - %s", err)
		h.destoryReplicationBacklog()
		return errors.Trace(err)
	}

	// set the offset of the first byte in the backlog
	h.repl.backlogOffset = h.repl.masterOffset - int64(h.repl.backlogBuf.Len()) + 1

	return nil
}

func (h *Handler) respEncodeStoreForward(f *store.Forward) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("*%d\r\n", len(f.Args)+1))
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Op), f.Op))

	for _, arg := range f.Args {
		switch t := arg.(type) {
		case []byte:
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(t)))
			buf.Write(t)
			buf.WriteString("\r\n")
		case string:
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(t)))
			buf.WriteString(t)
			buf.WriteString("\r\n")
		default:
			str := fmt.Sprintf("%v", t)
			buf.WriteString(fmt.Sprintf("$%d\r\n", len(str)))
			buf.WriteString(str)
			buf.WriteString("\r\n")
		}
	}
	return buf.Bytes(), nil
}

func (h *Handler) replicationFeedSlaves(f *store.Forward) error {
	h.repl.Lock()
	defer h.repl.Unlock()

	r := &h.repl
	if r.backlogBuf == nil && len(r.slaves) == 0 {
		return nil
	}

	if r.backlogBuf == nil {
		if err := h.createReplicationBacklog(); err != nil {
			return errors.Trace(err)
		}
	}

	if r.lastSelectDB.Get() != int64(f.DB) {
		selectCmd, _ := redis.EncodeToBytes(redis.NewRequest("SELECT", f.DB))

		// write SELECT into backlog
		if err := h.feedReplicationBacklog(selectCmd); err != nil {
			return errors.Trace(err)
		}

		r.lastSelectDB.Set(int64(f.DB))
	}

	// encode Forward with RESP format, then write into backlog
	if buf, err := h.respEncodeStoreForward(f); err != nil {
		return errors.Trace(err)
	} else if err = h.feedReplicationBacklog(buf); err != nil {
		return errors.Trace(err)
	}

	// notice slaves replication backlog has new data, need to sync
	if err := h.replicationNoticeSlavesSyncing(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (h *Handler) replicationNoticeSlavesSyncing() error {
	for _, ch := range h.repl.slaves {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return nil
}

// REPLCONF listening-port port / ack sync-offset
func ReplConfCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	switch strings.ToLower(string(args[0])) {
	case "listening-port":
		if port, err := strconv.ParseInt(string(args[1]), 10, 16); err != nil {
			return toRespErrorf("invalid port REPLCONF listening-port, err: %v", err)
		} else {
			c.listeningPort.Set(int64(port))
		}
	case "ack":
		if ack, err := strconv.ParseInt(string(args[1]), 10, 64); err != nil {
			return toRespErrorf("invalid port REPLCONF ACK, err: %v", err)
		} else {
			c.backlogACKOffset.Set(ack)
			c.backlogACKTime.Set(time.Now().Unix())
			// ACK will not reply anything
			return nil, nil
		}
	default:
		return toRespErrorf("Unrecognized REPLCONF option:%s", args[0])
	}

	return redis.NewString("OK"), nil
}

// SYNC
func SyncCmd(s Session, args [][]byte) (redis.Resp, error) {
	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	return c.h.handleSyncCommand("sync", c, args)
}

// PSYNC run-id sync-offset
func PSyncCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 2 {
		return toRespErrorf("len(args) = %d, expect = 2", len(args))
	}

	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	return c.h.handleSyncCommand("psync", c, args)
}

func (h *Handler) handleSyncCommand(opt string, c *conn, args [][]byte) (redis.Resp, error) {
	if h.isSlave(c) {
		// ignore SYNC if already slave
		return nil, nil
	}

	if opt == "psync" {
		// first try whether full resync or not
		need, syncOffset := h.needFullReSync(c, args)
		if !need {
			// write CONTINUE and resume replication
			if err := c.writeRESP(redis.NewString("CONTINUE")); err != nil {
				log.Errorf("reply slave %s psync CONTINUE err - %s", c, err)
				c.Close()
				return nil, errors.Trace(err)
			}

			h.counters.syncPartialOK.Add(1)

			h.startSlaveReplication(c, syncOffset)
			return nil, nil
		}

		// we must handle full resync
		if err := h.replicationReplyFullReSync(c); err != nil {
			return nil, errors.Trace(err)
		}

		// slave will use ? to force resync, this is not error
		if !bytes.Equal(args[0], []byte{'?'}) {
			h.counters.syncPartialErr.Add(1)
		}
	}

	offset, resp, err := h.replicationSlaveFullSync(c)
	if err != nil {
		return resp, errors.Trace(err)
	}

	h.startSlaveReplication(c, offset)

	return nil, nil
}

func (h *Handler) replicationReplyFullReSync(c *conn) error {
	// lock all to get the current master replication offset
	if err := c.Store().Acquire(); err != nil {
		return errors.Trace(err)
	}

	syncOffset := h.repl.masterOffset
	if h.repl.backlogBuf == nil {
		// we will increment the master offset by one when backlog buffer created
		syncOffset++
	}
	c.Store().Release()

	if err := c.writeRESP(redis.NewString(fmt.Sprintf("FULLRESYNC %s %d", h.runID, syncOffset))); err != nil {
		log.Errorf("reply slave %s psync FULLRESYNC err - %s", c, err)
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

// if full sync ok, return sync offset for later backlog syncing
func (h *Handler) replicationSlaveFullSync(c *conn) (syncOffset int64, resp redis.Resp, err error) {
	// after bgsave, we must send this RDB to slave,
	// so we don't allow others do bgsave before full sync done.
	if ok := h.bgSaveSem.AcquireTimeout(time.Minute); !ok {
		resp, err = toRespErrorf("wait others do bgsave timeout")
		return
	}
	defer h.bgSaveSem.Release()

	// now begin full sync
	h.counters.syncFull.Add(1)

	var rdb *os.File
	rdb, syncOffset, err = h.replicationBgSave()
	if err != nil {
		resp, err = toRespError(err)
		return
	}
	defer rdb.Close()

	// send rdb to slave
	st, _ := rdb.Stat()

	rdbSize := st.Size()

	if err = c.writeRDBFrom(rdbSize, rdb); err != nil {
		// close this connection here???
		log.Errorf("slave %s sync rdb err - %s", c, err)
		c.Close()
		return
	}

	return syncOffset, nil, nil
}

// if no need full resync, returns false and sync offset
func (h *Handler) needFullReSync(c *conn, args [][]byte) (bool, int64) {
	masterRunID := args[0]

	if !bytes.EqualFold(masterRunID, h.runID) {
		if !bytes.Equal(masterRunID, []byte{'?'}) {
			log.Infof("Partial resynchronization not accepted, runid mismatch, server is %s, but client is %s", h.runID, masterRunID)
		} else {
			log.Infof("Full resync requested by slave.")
		}
		return true, 0
	}

	syncOffset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		log.Errorf("PSYNC parse sync offset err, try full resync - %s", err)
		return true, 0
	}

	r := &h.repl

	h.repl.RLock()
	defer h.repl.RUnlock()

	if r.backlogBuf == nil || syncOffset < r.backlogOffset ||
		syncOffset > (r.backlogOffset+int64(r.backlogBuf.Len())) {
		log.Infof("unable to partial resync with the slave for lack of backlog, slave offset %d", syncOffset)
		if syncOffset > r.masterOffset {
			log.Infof("slave tried to PSYNC with an offset %d larger than master offset %d", syncOffset, r.masterOffset)
		}

		return true, 0
	}

	return false, syncOffset
}

func (h *Handler) startSlaveReplication(c *conn, syncOffset int64) {
	c.syncOffset.Set(syncOffset)

	// we may not receive any data, so ignore timeout
	c.timeout = 0

	c.backlogACKTime.Set(time.Now().Unix())

	ch := make(chan struct{}, 1)
	ch <- struct{}{}

	h.repl.Lock()
	h.repl.slaves[c] = ch
	h.repl.Unlock()

	go func(c *conn, ch chan struct{}) {
		defer func() {
			h.removeSlave(c)
			c.Close()
		}()

		buf := make([]byte, bytesize.MB)

		for {
			select {
			case <-h.signal:
				return
			case _, ok := <-ch:
				if !ok {
					return
				}

				for {
					n, err := h.replicationSlaveSyncBacklog(c, buf)
					if err != nil {
						log.Errorf("sync slave err, close replication - %s", err)
						return
					} else if n < len(buf) {
						// we now sync all backlog, wait new incoming
						break
					}
				}

			}
		}
	}(c, ch)
}

func (h *Handler) replicationSlaveSyncBacklog(c *conn, buf []byte) (int, error) {
	h.repl.RLock()
	defer h.repl.RUnlock()

	offset := c.syncOffset.Get()

	r := &h.repl

	if r.backlogBuf == nil {
		return 0, nil
	}

	start := r.backlogOffset
	end := r.backlogOffset + int64(r.backlogBuf.Len())

	if offset < start || offset > end {
		// we can not read data from this offset in backlog buffer
		// lag behind too much, so a better way is to stop the replication and re-fullsync again

		return 0, fmt.Errorf("slave %s has invalid sync offset %d, not in [%d, %d]", c, offset, start, end)
	}

	// read data into buf
	n, err := h.repl.backlogBuf.ReadAt(buf, offset-start)
	if err != nil {
		return 0, fmt.Errorf("slave %s read backlog data err %v", c, err)
	}

	if n == 0 {
		// no more data to read
		return 0, nil
	}

	// use write timeout here, now 5s, later, use config
	c.nc.SetWriteDeadline(time.Now().Add(5 * time.Second))

	if err = c.writeRaw(buf[0:n]); err != nil {
		return 0, fmt.Errorf("slave %s sync backlog data err %v", c, err)
	}

	c.syncOffset.Add(int64(n))

	return n, nil
}

func (h *Handler) isSlave(c *conn) bool {
	h.repl.RLock()
	defer h.repl.RUnlock()
	_, ok := h.repl.slaves[c]

	return ok
}

func (h *Handler) removeSlave(c *conn) {
	h.repl.Lock()
	defer h.repl.Unlock()

	ch, ok := h.repl.slaves[c]
	if ok {
		delete(h.repl.slaves, c)
		close(ch)
	}
}

func (h *Handler) replicationBgSave() (*os.File, int64, error) {
	// need to improve later
	syncOffset := new(int64)
	sp, err := h.store.NewSnapshotFunc(func() {
		offset := h.repl.masterOffset
		// we will sync from masterOffset + 1
		*syncOffset = offset + 1
		if h.repl.backlogBuf == nil {
			// we will create backlog buffer and increment master offset by one later
			*syncOffset = offset + 2
		}

		h.repl.lastSelectDB.Set(int64(math.MaxUint32))
	})
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer h.store.ReleaseSnapshot(sp)

	path := h.config.DumpPath
	if err := h.bgsaveTo(sp, path); err != nil {
		return nil, 0, errors.Trace(err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	return f, *syncOffset, nil
}

const (
	masterConnNone       = "none"       // no replication
	masterConnConnect    = "connect"    // must connect master
	masterConnConnecting = "connecting" // connecting to master
	masterConnSync       = "sync"       // rdb syncing
	masterConnConnected  = "connected"  // connected to master
)

// ROLE
func RoleCmd(s Session, args [][]byte) (redis.Resp, error) {
	if len(args) != 0 {
		return toRespErrorf("len(args) = %d, expect = 0", len(args))
	}

	c, _ := s.(*conn)
	if c == nil {
		return nil, errors.New("invalid connection")
	}

	ay := redis.NewArray()
	if masterAddr := c.h.masterAddr.Get(); masterAddr == "" {
		// master
		ay.Append(redis.NewBulkBytesWithString("master"))
		c.h.repl.RLock()
		defer c.h.repl.RUnlock()

		ay.Append(redis.NewInt(c.h.repl.masterOffset))
		slaves := redis.NewArray()
		for slave, _ := range c.h.repl.slaves {
			a := redis.NewArray()
			if addr := slave.nc.RemoteAddr(); addr == nil {
				continue
			} else {
				a.Append(redis.NewBulkBytesWithString(strings.Split(addr.String(), ":")[0]))
			}
			a.Append(redis.NewBulkBytesWithString(fmt.Sprintf("%d", slave.listeningPort.Get())))
			a.Append(redis.NewBulkBytesWithString(fmt.Sprintf("%d", slave.syncOffset.Get())))
			slaves.Append(a)
		}

		ay.Append(slaves)
	} else {
		// slave
		ay.Append(redis.NewBulkBytesWithString("slave"))
		seps := strings.Split(masterAddr, ":")
		if len(seps) == 2 {
			port, err := strconv.ParseInt(seps[1], 10, 16)
			if err != nil {
				return toRespError(err)
			}
			ay.Append(redis.NewBulkBytesWithString(seps[0]))
			ay.Append(redis.NewInt(int64(port)))
		} else {
			return toRespErrorf("invalid master addr, must ip:port, but %s", masterAddr)
		}
		ay.Append(redis.NewBulkBytesWithString(c.h.masterConnState.Get()))
		ay.Append(redis.NewInt(c.h.syncOffset.Get()))
	}
	return ay, nil
}

func init() {
	Register("replconf", ReplConfCmd)
	Register("sync", SyncCmd)
	Register("psync", PSyncCmd)
	Register("role", RoleCmd)
}
