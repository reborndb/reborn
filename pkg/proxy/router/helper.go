// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/proxy/group"
	"github.com/reborndb/reborn/pkg/proxy/parser"
	"github.com/reborndb/reborn/pkg/proxy/redisconn"

	"github.com/juju/errors"
	topo "github.com/ngaut/go-zookeeper/zk"
	stats "github.com/ngaut/gostats"
	log "github.com/ngaut/logging"
	respcoding "github.com/ngaut/resp"
)

var blackList = []string{
	"KEYS", "MOVE", "OBJECT", "RENAME", "RENAMENX", "SORT", "SCAN", "BITOP" /*"MGET",*/ /* "MSET",*/, "MSETNX", "SCAN",
	"BLPOP", "BRPOP", "BRPOPLPUSH", "PSUBSCRIBE，PUBLISH", "PUNSUBSCRIBE", "SUBSCRIBE", "RANDOMKEY",
	"UNSUBSCRIBE", "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH", "SCRIPT EXISTS", "SCRIPT FLUSH", "SCRIPT KILL",
	"SCRIPT LOAD" /*, "AUTH" , "ECHO"*/ /*"QUIT",*/ /*"SELECT",*/, "BGREWRITEAOF", "BGSAVE", "CLIENT KILL", "CLIENT LIST",
	"CONFIG GET", "CONFIG SET", "CONFIG RESETSTAT", "DBSIZE", "DEBUG OBJECT", "DEBUG SEGFAULT", "FLUSHALL", "FLUSHDB",
	"LASTSAVE", "MONITOR", "SAVE", "SHUTDOWN", "SLAVEOF", "SLOWLOG", "SYNC", "TIME", "SLOTSMGRTONE", "SLOTSMGRT",
	"SLOTSDEL",
}

var (
	blackListCommand = make(map[string]struct{})
	OK_BYTES         = []byte("+OK\r\n")
)

func init() {
	for _, k := range blackList {
		blackListCommand[k] = struct{}{}
	}
}

func allowOp(op string) bool {
	_, black := blackListCommand[op]
	return !black
}

func isMulOp(op string) bool {
	if op == "MGET" || op == "DEL" || op == "MSET" {
		return true
	}

	return false
}

func validSlot(i int) bool {
	if i < 0 || i >= models.DEFAULT_SLOT_NUM {
		return false
	}

	return true
}

func writeCommand(c *redisconn.Conn, cmd string, args ...interface{}) error {
	return parser.WriteCommand(c, cmd, args...)
}

func doCommand(c *redisconn.Conn, cmd string, args ...interface{}) (*parser.Resp, error) {
	writeCommand(c, cmd, args...)
	if err := c.Flush(); err != nil {
		return nil, errors.Trace(err)
	}

	resp, err := parser.Parse(c.BufioReader())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func doCommandMustOK(c *redisconn.Conn, cmd string, args ...interface{}) error {
	resp, err := doCommand(c, cmd, args...)
	if err != nil {
		return errors.Trace(err)
	}

	if !bytes.Equal(resp.Raw, OK_BYTES) {
		return errors.Errorf("resp returns raw %s, not OK", resp.Raw)
	}
	return nil
}

func writeMigrateKeyCmd(c *redisconn.Conn, addr string, timeoutMs int, keys ...[]byte) error {
	hostPort := strings.Split(addr, ":")
	if len(hostPort) != 2 {
		return errors.Errorf("invalid address " + addr)
	}

	for _, key := range keys {
		err := writeCommand(c, "slotsmgrttagone", hostPort[0], hostPort[1], int(timeoutMs), key)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(c.Flush())
}

type DeadlineReadWriter interface {
	io.ReadWriter
	SetWriteDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
}

func handleSpecCommand(cmd string, keys [][]byte, timeout int) ([]byte, bool, bool, error) {
	var b []byte
	shouldClose := false
	switch cmd {
	case "PING":
		b = []byte("+PONG\r\n")
	case "QUIT":
		b = OK_BYTES
		shouldClose = true
	case "SELECT":
		b = OK_BYTES
	case "AUTH":
		b = OK_BYTES
	case "ECHO":
		if len(keys) > 0 {
			var err error
			b, err = respcoding.Marshal(string(keys[0]))
			if err != nil {
				return nil, true, false, errors.Trace(err)
			}
		} else {
			return nil, true, false, nil
		}
	}

	if len(b) > 0 {
		return b, shouldClose, true, nil
	}

	return b, shouldClose, false, nil
}

func write2Client(redisReader *bufio.Reader, clientWriter io.Writer) (redisErr error, clientErr error) {
	resp, err := parser.Parse(redisReader)
	if err != nil {
		return errors.Trace(err), errors.Trace(err)
	}

	err = resp.WriteTo(clientWriter)
	return nil, errors.Trace(err)
}

type BufioDeadlineReadWriter interface {
	DeadlineReadWriter
	BufioReader() *bufio.Reader
}

func forward(c DeadlineReadWriter, redisConn BufioDeadlineReadWriter, resp *parser.Resp, timeout int) (redisErr error, clientErr error) {
	redisReader := redisConn.BufioReader()
	if err := redisConn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Second)); err != nil {
		return errors.Trace(err), errors.Trace(err)
	}
	if err := resp.WriteTo(redisConn); err != nil {
		return errors.Trace(err), errors.Trace(err)
	}

	if err := redisConn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Second)); err != nil {
		return errors.Trace(err), errors.Trace(err)
	}

	if err := c.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Second)); err != nil {
		return nil, errors.Trace(err)
	}

	// read and parse redis response
	return write2Client(redisReader, c)
}

func StringsContain(s []string, key string) bool {
	for _, val := range s {
		if val == key { //need our resopnse
			return true
		}
	}

	return false
}

func getRespOpKeys(c *session) (*parser.Resp, []byte, [][]byte, error) {
	resp, err := parser.Parse(c.r) // read client request
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	op, keys, err := resp.GetOpKeys()
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	if len(keys) == 0 {
		keys = [][]byte{[]byte("fakeKey")}
	}

	return resp, op, keys, nil
}

func filter(opstr string, keys [][]byte, c *session, timeoutSec int) (rawresp []byte, next bool, err error) {
	if !allowOp(opstr) {
		errmsg, err := respcoding.Marshal(fmt.Errorf("%s not allowed", opstr))
		if err != nil {
			log.Fatal("should never happend", opstr)
		}
		return errmsg, false, errors.New(string(errmsg))
	}

	buf, shouldClose, handled, err := handleSpecCommand(opstr, keys, timeoutSec)
	if shouldClose { //quit command
		return buf, false, errors.Trace(io.EOF)
	}
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if handled {
		return buf, false, nil
	}

	return nil, true, nil
}

func GetEventPath(evt interface{}) string {
	return evt.(topo.Event).Path
}

func CheckUlimit(min int) {
	ulimitN, err := exec.Command("/bin/sh", "-c", "ulimit -n").Output()
	if err != nil {
		log.Warning("get ulimit failed", err)
	}

	n, err := strconv.Atoi(strings.TrimSpace(string(ulimitN)))
	if err != nil || n < min {
		log.Fatalf("ulimit too small: %d, should be at least %d", n, min)
	}
}

func GetOriginError(err *errors.Err) error {
	if err != nil {
		if err.Cause() == nil && err.Underlying() == nil {
			return err
		} else {
			return err.Underlying()
		}
	}

	return err
}

func recordResponseTime(c *stats.Counters, d time.Duration) {
	switch {
	case d < 5:
		c.Add("0-5ms", 1)
	case d >= 5 && d < 10:
		c.Add("5-10ms", 1)
	case d >= 10 && d < 50:
		c.Add("10-50ms", 1)
	case d >= 50 && d < 200:
		c.Add("50-200ms", 1)
	case d >= 200 && d < 1000:
		c.Add("200-1000ms", 1)
	case d >= 1000 && d < 5000:
		c.Add("1000-5000ms", 1)
	case d >= 5000 && d < 10000:
		c.Add("5000-10000ms", 1)
	default:
		c.Add("10000ms+", 1)
	}
}

type killEvent struct {
	done chan error
}

type Slot struct {
	slotInfo    *models.Slot
	groupInfo   *models.ServerGroup
	dst         *group.Group
	migrateFrom *group.Group
}

type onSuicideFun func() error

func needResponse(receivers []string, self models.ProxyInfo) bool {
	var pi models.ProxyInfo
	for _, v := range receivers {
		err := json.Unmarshal([]byte(v), &pi)
		if err != nil {
			//is it old version of dashboard
			if v == self.ID {
				return true
			}
			return false
		}

		if pi.ID == self.ID && pi.Pid == self.Pid && pi.StartAt == self.StartAt {
			return true
		}
	}

	return false
}

func isTheSameSlot(keys [][]byte) bool {
	if len(keys) == 1 {
		return true
	}

	firstSlot := -1
	for _, k := range keys {
		if firstSlot == -1 {
			firstSlot = mapKey2Slot(k)
		} else {
			if firstSlot != mapKey2Slot(k) {
				return false
			}
		}
	}

	return true
}
