// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package pipe

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/reborndb/go/bytesize"
	"github.com/reborndb/go/errors2"
	"github.com/reborndb/go/io/ioutils"
	. "gopkg.in/check.v1"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testIoPipeSuite{})

type testIoPipeSuite struct {
}

func (s *testIoPipeSuite) openPipe(c *C, fileName string) (Reader, Writer) {
	buffSize := bytesize.KB * 8
	fileSize := bytesize.MB * 32
	if fileName == "" {
		return PipeSize(buffSize)
	} else {
		f, err := OpenFile(fileName, false)
		c.Assert(err, IsNil)
		return PipeFile(buffSize, fileSize, f)
	}
}

func (s *testIoPipeSuite) testPipe1(c *C, fileName string) {
	r, w := s.openPipe(c, fileName)

	ss := "Hello world!!"

	go func(data []byte) {
		_, err := ioutils.WriteFull(w, data)
		c.Assert(err, IsNil)
		c.Assert(w.Close(), IsNil)
	}([]byte(ss))

	buf := make([]byte, 64)
	n, err := ioutils.ReadFull(r, buf)
	c.Assert(errors2.ErrorEqual(err, io.EOF), Equals, true)
	c.Assert(n, Equals, len(ss))
	c.Assert(string(buf[:n]), Equals, ss)
	c.Assert(r.Close(), IsNil)
}

func (s *testIoPipeSuite) TestPipe1(c *C) {
	s.testPipe1(c, "")
	s.testPipe1(c, "/tmp/pipe.test")
}

func (s *testIoPipeSuite) testPipe2(c *C, fileName string) {
	r, w := s.openPipe(c, fileName)

	cc := 1024 * 128
	ss := "Hello world!!"

	go func() {
		for i := 0; i < cc; i++ {
			m := fmt.Sprintf("[%d]%s ", i, ss)
			_, err := ioutils.WriteFull(w, []byte(m))
			c.Assert(err, IsNil)
		}
		c.Assert(w.Close(), IsNil)
	}()

	time.Sleep(time.Millisecond * 100)

	buf := make([]byte, len(ss)*cc*2)
	n, err := ioutils.ReadFull(r, buf)
	c.Assert(errors2.ErrorEqual(err, io.EOF), Equals, true)

	buf = buf[:n]
	for i := 0; i < cc; i++ {
		m := fmt.Sprintf("[%d]%s ", i, ss)
		c.Assert(len(buf) >= len(m), Equals, true)
		c.Assert(string(buf[:len(m)]), Equals, m)
		buf = buf[len(m):]
	}

	c.Assert(len(buf), Equals, 0)
	c.Assert(r.Close(), IsNil)
}

func (s *testIoPipeSuite) TestPipe2(c *C) {
	s.testPipe2(c, "")
	s.testPipe2(c, "/tmp/pipe.test")
}

func (s *testIoPipeSuite) testPipe3(c *C, fileName string) {
	r, w := s.openPipe(c, fileName)

	ch := make(chan int)

	size := 4096

	go func() {
		buf := make([]byte, size)
		for {
			n, err := r.Read(buf)

			if errors2.ErrorEqual(err, io.EOF) {
				break
			}

			c.Assert(err, IsNil)
			ch <- n
		}

		c.Assert(r.Close(), IsNil)
		ch <- 0
	}()

	go func() {
		buf := make([]byte, size)
		for i := 1; i < size; i++ {
			n, err := ioutils.WriteFull(w, buf[:i])
			c.Assert(err, IsNil)
			c.Assert(n, Equals, i)
		}
		c.Assert(w.Close(), IsNil)
	}()

	sum := 0
	for i := 1; i < size; i++ {
		sum += i
	}
	for {
		n := <-ch
		if n == 0 {
			break
		}
		sum -= n
	}

	c.Assert(sum, Equals, 0)
}

func (s *testIoPipeSuite) TestPipe3(c *C) {
	s.testPipe3(c, "")
	s.testPipe3(c, "/tmp/pipe.test")
}

func (s *testIoPipeSuite) testPipe4(c *C, fileName string) {
	r, w := s.openPipe(c, fileName)

	key := []byte("spinlock aes-128")

	block := aes.BlockSize
	count := 1024 * 1024 * 128 / block

	go func() {
		buf := make([]byte, count*block)
		m, err := aes.NewCipher(key)
		c.Assert(err, IsNil)

		for i := 0; i < len(buf); i++ {
			buf[i] = byte(i)
		}

		e := cipher.NewCBCEncrypter(m, make([]byte, block))
		e.CryptBlocks(buf, buf)

		n, err := ioutils.WriteFull(w, buf)
		c.Assert(err, IsNil)
		c.Assert(w.Close(), IsNil)
		c.Assert(n, Equals, len(buf))
	}()

	buf := make([]byte, count*block)
	m, err := aes.NewCipher(key)
	c.Assert(err, IsNil)

	_, err = ioutils.ReadFull(r, buf)
	c.Assert(err, IsNil)

	e := cipher.NewCBCDecrypter(m, make([]byte, block))
	e.CryptBlocks(buf, buf)

	for i := 0; i < len(buf); i++ {
		// make gocheck faster
		if buf[i] != byte(i) {
			c.Assert(buf[i], Equals, byte(i))
		}
	}

	_, err = ioutils.ReadFull(r, buf)
	c.Assert(errors2.ErrorEqual(err, io.EOF), Equals, true)
	c.Assert(r.Close(), IsNil)
}

func (s *testIoPipeSuite) TestPipe4(c *C) {
	s.testPipe4(c, "")
	s.testPipe4(c, "/tmp/pipe.test")
}

type pipeTest struct {
	async   bool
	err     error
	witherr bool
}

func (p pipeTest) String() string {
	return fmt.Sprintf("async=%v err=%v witherr=%v", p.async, p.err, p.witherr)
}

var pipeTests = []pipeTest{
	{true, nil, false},
	{true, nil, true},
	{true, io.ErrShortWrite, true},
	{false, nil, false},
	{false, nil, true},
	{false, io.ErrShortWrite, true},
}

func (s *testIoPipeSuite) delayClose(c *C, closer Closer, ch chan int, u pipeTest) {
	time.Sleep(time.Millisecond * 100)
	var err error
	if u.witherr {
		err = closer.CloseWithError(u.err)
	} else {
		err = closer.Close()
	}

	c.Assert(err, IsNil)
	ch <- 0
}

func (s *testIoPipeSuite) TestPipeReadClose(c *C) {
	for _, u := range pipeTests {
		r, w := Pipe()
		ch := make(chan int, 1)

		if u.async {
			go s.delayClose(c, w, ch, u)
		} else {
			s.delayClose(c, w, ch, u)
		}

		buf := make([]byte, 64)
		n, err := r.Read(buf)
		<-ch

		expect := u.err
		if expect == nil {
			expect = io.EOF
		}

		c.Assert(errors2.ErrorEqual(err, expect), Equals, true)
		c.Assert(n, Equals, 0)
		c.Assert(r.Close(), IsNil)
	}
}

func (s *testIoPipeSuite) TestPipeReadClose2(c *C) {
	r, w := Pipe()
	ch := make(chan int, 1)

	go s.delayClose(c, r, ch, pipeTest{})

	n, err := r.Read(make([]byte, 64))
	<-ch

	c.Assert(errors2.ErrorEqual(err, io.ErrClosedPipe), Equals, true)

	c.Assert(n, Equals, 0)
	c.Assert(w.Close(), IsNil)
}

func (s *testIoPipeSuite) TestPipeWriteClose(c *C) {
	for _, u := range pipeTests {
		r, w := Pipe()
		ch := make(chan int, 1)

		if u.async {
			go s.delayClose(c, r, ch, u)
		} else {
			s.delayClose(c, r, ch, u)
		}
		<-ch

		n, err := ioutils.WriteFull(w, []byte("hello, world"))
		expect := u.err
		if expect == nil {
			expect = io.ErrClosedPipe
		}

		c.Assert(errors2.ErrorEqual(err, expect), Equals, true)

		c.Assert(n, Equals, 0)
		c.Assert(w.Close(), IsNil)
	}
}

func (s *testIoPipeSuite) TestWriteEmpty(c *C) {
	r, w := Pipe()

	go func() {
		_, err := w.Write([]byte{})
		c.Assert(err, IsNil)
		c.Assert(w.Close(), IsNil)
	}()

	buf := make([]byte, 4096)
	n, err := ioutils.ReadFull(r, buf)
	c.Assert(errors2.ErrorEqual(err, io.EOF), Equals, true)
	c.Assert(n, Equals, 0)
	c.Assert(r.Close(), IsNil)
}

func (s *testIoPipeSuite) TestWriteNil(c *C) {
	r, w := Pipe()

	go func() {
		_, err := w.Write(nil)
		c.Assert(err, IsNil)
		c.Assert(w.Close(), IsNil)
	}()

	buf := make([]byte, 4096)
	n, err := ioutils.ReadFull(r, buf)
	c.Assert(errors2.ErrorEqual(err, io.EOF), Equals, true)
	c.Assert(n, Equals, 0)
	c.Assert(r.Close(), IsNil)
}

func (s *testIoPipeSuite) TestWriteAfterWriterClose(c *C) {
	r, w := Pipe()

	ss := "hello"

	errs := make(chan error)

	go func() {
		_, err := ioutils.WriteFull(w, []byte(ss))
		c.Assert(err, IsNil)
		c.Assert(w.Close(), IsNil)

		_, err = w.Write([]byte("world"))
		errs <- err
	}()

	buf := make([]byte, 4096)
	n, err := ioutils.ReadFull(r, buf)
	c.Assert(errors2.ErrorEqual(err, io.EOF), Equals, true)
	c.Assert(string(buf[:n]), Equals, ss)

	err = <-errs
	c.Assert(errors2.ErrorEqual(err, io.ErrClosedPipe), Equals, true)
	c.Assert(r.Close(), IsNil)
}
