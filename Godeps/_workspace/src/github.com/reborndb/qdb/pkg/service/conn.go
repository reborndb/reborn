// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/go/atomic2"
	redis "github.com/reborndb/go/redis/resp"
	"github.com/reborndb/qdb/pkg/store"
)

type conn struct {
	r *bufio.Reader
	w *bufio.Writer

	wLock sync.Mutex

	db uint32
	nc net.Conn
	h  *Handler

	// summary for this connection
	summ    string
	timeout time.Duration

	// replication sync offset
	syncOffset atomic2.Int64

	// replication backlog ACK offset
	backlogACKOffset atomic2.Int64

	// slave listening port
	listeningPort atomic2.Int64

	// replication ACK time, using unix time
	backlogACKTime atomic2.Int64

	authenticated bool
}

func newConn(nc net.Conn, h *Handler, timeout int) *conn {
	c := &conn{
		nc: nc,
		h:  h,
	}

	c.r = bufio.NewReader(nc)
	c.w = bufio.NewWriter(nc)
	c.summ = fmt.Sprintf("<local> %s -- %s <remote>", nc.LocalAddr(), nc.RemoteAddr())
	c.timeout = time.Duration(timeout) * time.Second
	c.authenticated = false

	return c
}

func (c *conn) String() string {
	return c.summ
}

func (c *conn) serve(h *Handler) error {
	defer func() {
		h.removeSlave(c)
		h.removeConn(c)
		c.Close()
	}()

	h.addConn(c)

	for {
		response, err := c.handleRequest(h)
		if err != nil {
			return errors.Trace(err)
		} else if response == nil {
			continue
		}

		if c.timeout > 0 {
			deadline := time.Now().Add(c.timeout)
			if err := c.nc.SetWriteDeadline(deadline); err != nil {
				return errors.Trace(err)
			}
		}

		if err = c.writeRESP(response); err != nil {
			return errors.Trace(err)
		}
	}
}

func (c *conn) handleRequest(h *Handler) (redis.Resp, error) {
	if c.timeout > 0 {
		deadline := time.Now().Add(c.timeout)
		if err := c.nc.SetReadDeadline(deadline); err != nil {
			return nil, errors.Trace(err)
		}
	}
	request, err := redis.DecodeRequest(c.r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if request.Type() == redis.TypePing {
		return nil, nil
	}

	h.counters.commands.Add(1)
	response, err := c.dispatch(h, request)
	if err != nil {
		h.counters.commandsFailed.Add(1)
		b, _ := redis.EncodeToBytes(request)
		log.Warningf("handle commands failed, conn = %s, request = '%s', err = %s", c, base64.StdEncoding.EncodeToString(b), err)
	}

	return response, nil
}

func (c *conn) dispatch(h *Handler, request redis.Resp) (redis.Resp, error) {
	cmd, args, err := redis.ParseArgs(request)
	if err != nil {
		return toRespError(err)
	}

	// check cmd first, then auth
	if f := h.htable[cmd]; f == nil {
		return toRespErrorf("unknown command %s", cmd)
	} else {
		if len(h.config.Auth) > 0 && !c.authenticated && strings.ToLower(cmd) != "auth" {
			return toRespErrorf("NOAUTH Authentication required")
		}

		return f(c, args)
	}
}

// read a RESP line, return buffer ignoring \r\n
// sometimes, only \n is a valid RESP line, we will ignore this
func (c *conn) readLine() (line []byte, err error) {
	// if we read too many \n only, maybe something is wrong.
	for i := 0; i < 100; i++ {
		line, err = c.r.ReadSlice('\n')
		if err != nil {
			return nil, errors.Trace(err)
		} else if line[0] == '\n' {
			// only \n one line, try again
			continue
		}
		break
	}

	i := len(line) - 2
	if i < 0 || line[i] != '\r' {
		return nil, errors.New("bad resp line terminator")
	}
	return line[:i], nil
}

func (c *conn) ping() error {
	deadline := time.Now().Add(time.Second * 5)
	if err := c.nc.SetDeadline(deadline); err != nil {
		return errors.Trace(err)
	}

	if err := c.writeRESP(redis.NewRequest("PING")); err != nil {
		return errors.Trace(err)
	}

	if rsp, err := c.readLine(); err != nil {
		return errors.Trace(err)
	} else if strings.ToLower(string(rsp)) != "+pong" {
		return errors.Errorf("invalid response of command ping: %s", rsp)
	}

	return nil
}

// send PSYNC command and return the first reply line from master
func (c *conn) prePSync(masterRunID string, syncOffset int64) (string, error) {
	deadline := time.Now().Add(time.Second * 5)
	if err := c.nc.SetDeadline(deadline); err != nil {
		return "", errors.Trace(err)
	}

	if err := c.writeRESP(redis.NewRequest("PSYNC", masterRunID, syncOffset)); err != nil {
		return "", errors.Trace(err)
	}

	rsp, err := c.readLine()
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(rsp), nil
}

func (c *conn) sendCommand(cmd string, args ...interface{}) error {
	deadline := time.Now().Add(time.Second * 5)
	if err := c.nc.SetWriteDeadline(deadline); err != nil {
		return errors.Trace(err)
	}

	return c.writeRESP(redis.NewRequest(cmd, args...))
}

func (c *conn) doMustOK(cmd string, args ...interface{}) error {
	deadline := time.Now().Add(time.Second * 5)
	if err := c.nc.SetDeadline(deadline); err != nil {
		return errors.Trace(err)
	}

	if err := c.sendCommand(cmd, args...); err != nil {
		return errors.Trace(err)
	}

	rsp, err := c.readLine()
	if err != nil {
		return errors.Trace(err)
	}

	if strings.ToLower(string(rsp)) != "+ok" {
		return errors.Errorf("response is not ok but %s", rsp)
	}

	return nil
}

func (c *conn) writeRESP(resp redis.Resp) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	if err := redis.Encode(c.w, resp); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(c.w.Flush())
}

func (c *conn) writeRDBFrom(size int64, r io.Reader) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	c.w.WriteString(fmt.Sprintf("$%d\r\n", size))

	if n, err := io.CopyN(c.w, r, size); err != nil {
		return errors.Trace(err)
	} else if n != size {
		return errors.Trace(io.ErrShortWrite)
	}

	return errors.Trace(c.w.Flush())
}

func (c *conn) writeRaw(buf []byte) error {
	c.wLock.Lock()
	defer c.wLock.Unlock()

	c.w.Write(buf)

	return errors.Trace(c.w.Flush())
}

func (c *conn) Close() {
	c.nc.Close()
	c = nil
}

func (c *conn) DB() uint32 {
	return c.db
}

func (c *conn) SetDB(db uint32) {
	c.db = db
}

func (c *conn) Store() *store.Store {
	return c.h.store
}
