// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"math"
	"sort"

	. "gopkg.in/check.v1"
)

type bytesSlice [][]byte

func (s bytesSlice) Len() int {
	return len(s)
}

func (s bytesSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s bytesSlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

func testEncodeInt64(i int64) []byte {
	b := NewBufWriter(nil)
	b.WriteInt64(i)
	return b.Bytes()
}

func testDecodeInt64(b []byte) int64 {
	r := NewBufReader(b)
	i, _ := r.ReadInt64()
	return i
}

func testInt64Codec(c *C, i int64) {
	c.Assert(testDecodeInt64(testEncodeInt64(i)), Equals, i)
}

func testEncodeUint64(i uint64) []byte {
	b := NewBufWriter(nil)
	b.WriteUint64(i)
	return b.Bytes()
}

func testDecodeUint64(b []byte) uint64 {
	r := NewBufReader(b)
	i, _ := r.ReadUint64()
	return i
}

func testUint64Codec(c *C, i uint64) {
	c.Assert(testDecodeUint64(testEncodeUint64(i)), Equals, i)
}

func testEncodeFloat(f float64) []byte {
	b := NewBufWriter(nil)
	b.WriteFloat64(f)
	return b.Bytes()
}

func testDecodeFloat(b []byte) float64 {
	r := NewBufReader(b)
	f, _ := r.ReadFloat64()
	return f
}

func testFloatLexSort(c *C, src []float64, check []float64) {
	ay := make(bytesSlice, 0, len(src))

	for _, f := range src {
		ay = append(ay, testEncodeFloat(f))
	}

	sort.Sort(ay)

	for i, b := range ay {
		f := testDecodeFloat(b)
		c.Assert(f, Equals, check[i])
	}
}

func testFloatCodec(c *C, f float64) {
	c.Assert(testDecodeFloat(testEncodeFloat(f)), Equals, f)
}

func (s *testStoreSuite) TestInt64Codec(c *C) {
	testInt64Codec(c, int64(0))
	testInt64Codec(c, int64(-123))
	testInt64Codec(c, int64(123))
}

func (s *testStoreSuite) TestUint64Codec(c *C) {
	testUint64Codec(c, uint64(0))
	testUint64Codec(c, uint64(123))
}

func (s *testStoreSuite) TestFloatCodec(c *C) {
	testFloatCodec(c, float64(1.0))
	testFloatCodec(c, float64(-1.0))
	testFloatCodec(c, float64(0))
	testFloatCodec(c, float64(-3.14))
	testFloatCodec(c, float64(3.14))
	testFloatCodec(c, math.MaxFloat64)
	testFloatCodec(c, -math.MaxFloat64)
	testFloatCodec(c, math.SmallestNonzeroFloat64)
	testFloatCodec(c, -math.SmallestNonzeroFloat64)
	testFloatCodec(c, math.Inf(1))
	testFloatCodec(c, math.Inf(-1))
}

func (s *testStoreSuite) TestFloatLexCodec(c *C) {
	testFloatLexSort(c,
		[]float64{1, -1},
		[]float64{-1, 1})

	testFloatLexSort(c,
		[]float64{-2, -3.14, 3.14},
		[]float64{-3.14, -2, 3.14})

	testFloatLexSort(c,
		[]float64{-2.0, -3.0, 1},
		[]float64{-3.0, -2.0, 1})

	testFloatLexSort(c,
		[]float64{1.0, 0, -10.0, math.MaxFloat64, -math.MaxFloat64},
		[]float64{-math.MaxFloat64, -10.0, 0, 1.0, math.MaxFloat64})

	testFloatLexSort(c,
		[]float64{math.Inf(1), math.Inf(-1), 1.1, -1.1, 0},
		[]float64{math.Inf(-1), -1.1, 0, 1.1, math.Inf(1)})

	testFloatLexSort(c,
		[]float64{math.Inf(1), math.Inf(-1), -math.MaxFloat64, math.MaxFloat64, 1.1, -1.1, 0},
		[]float64{math.Inf(-1), -math.MaxFloat64, -1.1, 0, 1.1, math.MaxFloat64, math.Inf(1)})
}
