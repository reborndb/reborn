// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package ring

import (
	"fmt"
	"io"
)

type Ring struct {
	buf buffer

	// current offset
	offset int

	// actual data size
	length int

	// total size
	size int
}

// Read data from the ring buffer at offset
// return n is the read data length, it can be less than buffer p size
func (r *Ring) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset < 0 || offset > int64(r.length) {
		return 0, fmt.Errorf("invalid offset %d, not in [0, %d]", offset, r.length)
	}

	// Get oldest data offset
	beginOffset := (r.offset + r.size - r.length) % r.size

	// Get read offset
	readOffset := (beginOffset + int(offset)) % r.size

	length := r.length - int(offset)
	if length > len(p) {
		length = len(p)
	}

	n = length

	for length > 0 {
		thisLen := length

		left := r.size - readOffset
		if left < length {
			// read cross-boundary
			thisLen = left
		}

		var nn int
		if nn, err = r.buf.ReadAt(p[0:thisLen], int64(readOffset)); err != nil && err != io.EOF {
			return
		} else if err == io.EOF && nn != thisLen {
			return
		} else {
			err = nil
		}

		p = p[thisLen:]

		length -= thisLen
		readOffset = 0
	}

	return
}

// Write data into ring buffer, if the data size is bigger than ring capability,
// old data will be overwritten.
func (r *Ring) Write(p []byte) (n int, err error) {
	n = len(p)
	length := len(p)
	for length > 0 {
		thisLen := r.size - r.offset
		if thisLen > length {
			thisLen = length
		}

		if _, err = r.buf.WriteAt(p[0:thisLen], int64(r.offset)); err != nil {
			return
		}

		r.offset += thisLen
		if r.offset == r.size {
			r.offset = 0
		}

		p = p[thisLen:]
		length -= thisLen
		r.length += thisLen
	}

	if r.length > r.size {
		r.length = r.size
	}
	return
}

// Current data length
func (r *Ring) Len() int {
	return r.length
}

// Ring buffer size
func (r *Ring) Size() int {
	return r.size
}

// Returns current offset
func (r *Ring) Offset() int {
	return r.offset
}

func (r *Ring) Reset() {
	r.offset = 0
	r.length = 0
}

func (r *Ring) Close() error {
	return r.buf.Close()
}

func NewMemRing(size int) (*Ring, error) {
	buf, _ := newMemBuffer(size)
	return newBufferRing(buf, size)
}

func NewFileRing(name string, size int) (*Ring, error) {
	buf, err := newFileBuffer(name, size)
	if err != nil {
		return nil, err
	}

	return newBufferRing(buf, size)
}

func newBufferRing(buf buffer, size int) (*Ring, error) {
	r := new(Ring)

	r.buf = buf
	r.size = size
	r.Reset()

	return r, nil
}
