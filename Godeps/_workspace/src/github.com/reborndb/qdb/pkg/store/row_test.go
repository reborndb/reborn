// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"math"

	. "gopkg.in/check.v1"
)

func (s *testStoreSuite) TestEncodeSimple(c *C) {
	defer func() {
		c.Assert(recover(), IsNil)
	}()
	w := NewBufWriter(nil)
	var b byte = 0xfc
	var cc ObjectCode = StringCode
	var f = 3.14
	var u32 uint32 = 1 << 31
	var i64 = int64(u32 - 1)
	var u64 = uint64(1<<63 - 1)
	refs := []interface{}{b, cc, &f, &u32, &i64, &u64}
	encodeRawBytes(w, refs...)

	b, cc, f = 0, ObjectCode(0), 0
	u32 = 0
	i64 = 0
	u64 = 0

	r := NewBufReader(w.Bytes())
	err := decodeRawBytes(r, nil, refs...)
	c.Assert(err, IsNil)
	c.Assert(r.Len(), Equals, 0)
	c.Assert(refs[0].(byte), Equals, byte(0xfc))
	c.Assert(refs[1].(ObjectCode), Equals, StringCode)
	c.Assert(math.Abs(f-3.14) < 1e-9, Equals, true)
	c.Assert(u32, Equals, uint32(1<<31))
	c.Assert(i64, Equals, int64(u32-1))
	c.Assert(u64, Equals, uint64(1<<63-1))
}

func (s *testStoreSuite) TestEncodeBytes(c *C) {
	defer func() {
		c.Assert(recover(), IsNil)
	}()
	w := NewBufWriter(nil)
	b := make([]byte, 1024)
	for i := 0; i < len(b); i++ {
		b[i] = byte((i + 1) * i)
	}
	vb := make([]byte, len(b))
	eb := make([]byte, len(b))
	copy(vb, b)
	copy(eb, b)
	refs := []interface{}{&vb, &eb}
	encodeRawBytes(w, refs...)

	vb, eb = nil, nil

	r := NewBufReader(w.Bytes())
	err := decodeRawBytes(r, nil, refs...)
	c.Assert(err, IsNil)
	c.Assert(r.Len(), Equals, 0)
	c.Assert(bytes.Equal(b, vb), Equals, true)
	c.Assert(bytes.Equal(b, eb), Equals, true)
}
