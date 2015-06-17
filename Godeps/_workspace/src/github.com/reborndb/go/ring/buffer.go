// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package ring

import (
	"io"
	"os"
	"path"
)

type buffer interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type memBuffer []byte

func newMemBuffer(size int) (buffer, error) {
	buf := make([]byte, size)
	return memBuffer(buf), nil
}

func (b memBuffer) ReadAt(p []byte, offset int64) (n int, err error) {
	n = copy(p, b[offset:])
	if n != len(p) {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (b memBuffer) WriteAt(p []byte, offset int64) (n int, err error) {
	n = copy(b[offset:], p)
	if n != len(p) {
		err = io.ErrShortWrite
	}
	return
}

func (b memBuffer) Close() error {
	return nil
}

func newFileBuffer(name string, size int) (buffer, error) {
	dir := path.Dir(name)
	os.MkdirAll(dir, 0700)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	if err = f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}
