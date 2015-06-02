package redisconn

import (
	"bufio"
	"net"
	"time"
)

//not thread-safe
type Conn struct {
	addr       string
	nc         net.Conn
	closed     bool
	r          *bufio.Reader
	w          *bufio.Writer
	netTimeout int //second
}

const defaultBufSize = 4096

func NewConnection(addr string, netTimeout int) (*Conn, error) {
	return NewConnectionWithSize(addr, netTimeout, defaultBufSize, defaultBufSize)
}

func NewConnectionWithSize(addr string, netTimeout int, readSize int, writeSize int) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Duration(netTimeout)*time.Second)
	if err != nil {
		return nil, err
	}

	return &Conn{
		addr:       addr,
		nc:         conn,
		closed:     false,
		r:          bufio.NewReaderSize(conn, readSize),
		w:          bufio.NewWriterSize(conn, writeSize),
		netTimeout: netTimeout,
	}, nil
}

//require read to use bufio
func (c *Conn) Read(p []byte) (int, error) {
	panic("not allowed")
}

func (c *Conn) Flush() error {
	return c.w.Flush()
}

func (c *Conn) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *Conn) BufioReader() *bufio.Reader {
	return c.r
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.nc.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.nc.SetReadDeadline(t)
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.nc.SetDeadline(t)
}

func (c *Conn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.nc.Close()
}
