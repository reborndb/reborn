// Copyright (c) 2014 José Carlos Nieto, https://menteslibres.net/xiam
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package resp

import (
	"bytes"
	"errors"
	"testing"
)

var respEncoder = encoder{}
var respDecoder = decoder{}

var (
	errTestFailed    = errors.New("Test failed.")
	errErrorExpected = errors.New("An error was expected.")
)

func TestReadLine(t *testing.T) {
	var test []byte
	var err error
	var n int

	if test, n, err = respDecoder.readLine([]byte("+OK\r\n")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal([]byte("+OK"), test) == false {
		t.Fatal(errTestFailed)
	}

	if n != 5 {
		t.Fatal(errTestFailed)
	}

	if test, n, err = respDecoder.readLine([]byte("+OK")); err == nil {
		t.Fatal(errErrorExpected)
	}

	if test != nil {
		t.Fatal(errTestFailed)
	}

	if n != 0 {
		t.Fatal(errTestFailed)
	}
}

func TestDecodeString(t *testing.T) {
	var test *Message
	var encoded []byte
	var err error

	// Simple "OK" string
	encoded = []byte("+OK\r\n")

	if test, err = respDecoder.decode(encoded); err != nil {
		t.Fatal(err)
	}

	if test.Status != "OK" {
		t.Fatal(errTestFailed)
	}

	// Two encoded strings, must get the first one.
	encoded = []byte("+OK\r\n+NO\r\n")

	if test, err = respDecoder.decode(encoded); err != nil {
		t.Fatal(err)
	}

	if test.Status != "OK" {
		t.Fatal(errTestFailed)
	}

	// String with a special character.
	encoded = []byte("+OK\r+NO\r\n")

	if test, err = respDecoder.decode(encoded); err != nil {
		t.Fatal(err)
	}

	if test.Status != "OK\r+NO" {
		t.Fatal(errTestFailed)
	}
}

func TestDecodeError(t *testing.T) {
	var test *Message
	var encoded []byte
	var err error

	// Simple "Error Message" error
	encoded = []byte("-Error Message\r\n")

	if test, err = respDecoder.decode(encoded); err != nil {
		t.Fatal(err)
	}

	if test.Error.Error() != "Error Message" {
		t.Fatal(errTestFailed)
	}
}

func TestDecodeInteger(t *testing.T) {
	var test *Message
	var encoded []byte
	var err error

	// Positive integer.
	encoded = []byte(":123\r\n")

	if test, err = respDecoder.decode(encoded); err != nil {
		t.Fatal(err)
	}

	if test.Integer != 123 {
		t.Fatal(errTestFailed)
	}

	// Negative integer.
	encoded = []byte(":-123\r\n")

	if test, err = respDecoder.decode(encoded); err != nil {
		t.Fatal(err)
	}

	if test.Integer != -123 {
		t.Fatal(errTestFailed)
	}

	// Wrong formatting
	encoded = []byte(":-12.3\r\n")

	if test, err = respDecoder.decode(encoded); err == nil {
		t.Fatal(errErrorExpected)
	}

	if test != nil {
		t.Fatal(errTestFailed)
	}

	// Wrong formatting
	encoded = []byte(":-12a3\r\n")

	if test, err = respDecoder.decode(encoded); err == nil {
		t.Fatal(errErrorExpected)
	}

	if test != nil {
		t.Fatal(errTestFailed)
	}

}

func TestDecodeBulk(t *testing.T) {
	var test *Message
	var err error

	// "foobar" string.
	if test, err = respDecoder.decode([]byte("$6\r\nfoobar\r\n")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(test.Bytes, []byte("foobar")) == false {
		t.Fatal(errTestFailed)
	}

	// "foo\r\nbar" string.
	if test, err = respDecoder.decode([]byte("$8\r\nfoo\r\nbar\r\n")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(test.Bytes, []byte("foo\r\nbar")) == false {
		t.Fatal(errTestFailed)
	}

	// An empty string.
	if test, err = respDecoder.decode([]byte("$0\r\n\r\n")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(test.Bytes, []byte("")) == false {
		t.Fatal(errTestFailed)
	}

	// Nil.
	if test, err = respDecoder.decode([]byte("$-1\r\n")); err != nil {
		t.Fatal(err)
	}

	if test.IsNil != true {
		t.Fatal(errTestFailed)
	}

	// UTF-8 string.
	if test, err = respDecoder.decode([]byte("$3\r\n✓\r\n")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(test.Bytes, []byte("✓")) == false {
		t.Fatal(errTestFailed)
	}

	// Invalid.
	if test, err = respDecoder.decode([]byte("$12\r\nSmall\r\n")); err == nil {
		t.Fatal(errErrorExpected)
	}

	if test != nil {
		t.Fatal(errTestFailed)
	}

}

func TestArrayDecode(t *testing.T) {
	var test *Message
	var err error

	// Array with zero elements.
	if test, err = respDecoder.decode([]byte("*0\r\n")); err != nil {
		t.Fatal(err)
	}

	if len(test.Array) > 0 {
		t.Fatal(errTestFailed)
	}

	// Nil.
	if test, err = respDecoder.decode([]byte("*-1\r\n")); err != nil {
		t.Fatal(err)
	}

	if test.IsNil == false {
		t.Fatal(errTestFailed)
	}
}

func TestArrayDecodeTwoElements(t *testing.T) {
	var test *Message
	var err error

	// Array with two elements.
	if test, err = respDecoder.decode([]byte("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")); err != nil {
		t.Fatal(err)
	}

	if len(test.Array) != 2 {
		t.Fatal(errTestFailed)
	}

	if string(test.Array[0].Bytes) != "foo" {
		t.Fatal(errTestFailed)
	}

	if string(test.Array[1].Bytes) != "bar" {
		t.Fatal(errTestFailed)
	}
}

func TestArrayDecodeThreeIntegers(t *testing.T) {
	var test *Message
	var err error

	// Array of three integers.
	if test, err = respDecoder.decode([]byte("*3\r\n:1\r\n:2\r\n:3\r\n")); err != nil {
		t.Fatal(err)
	}

	res := test.Array

	if len(res) != 3 {
		t.Fatal(errTestFailed)
	}

	if res[0].Integer != 1 {
		t.Fatal(errTestFailed)
	}

	if res[1].Integer != 2 {
		t.Fatal(errTestFailed)
	}

	if res[2].Integer != 3 {
		t.Fatal(errTestFailed)
	}
}

func TestArrayMixed(t *testing.T) {
	var test *Message
	var err error

	// Array of four integers and one string.
	if test, err = respDecoder.decode([]byte("*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n")); err != nil {
		t.Fatal(err)
	}

	res := test.Array

	if len(res) != 5 {
		t.Fatal(errTestFailed)
	}

	if res[0].Integer != 1 {
		t.Fatal(errTestFailed)
	}

	if res[1].Integer != 2 {
		t.Fatal(errTestFailed)
	}

	if res[2].Integer != 3 {
		t.Fatal(errTestFailed)
	}

	if res[3].Integer != 4 {
		t.Fatal(errTestFailed)
	}

	if string(res[4].Bytes) != "foobar" {
		t.Fatal(errTestFailed)
	}
}

func TestArrayNested(t *testing.T) {
	var test *Message
	var err error

	// Array of two arrays.
	if test, err = respDecoder.decode([]byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n")); err != nil {
		t.Fatal(err)
	}

	res := test.Array

	if len(res) != 2 {
		t.Fatal(errTestFailed)
	}

	arr1 := res[0].Array
	arr2 := res[1].Array

	if arr1[0].Integer != 1 {
		t.Fatal(errTestFailed)
	}

	if arr1[1].Integer != 2 {
		t.Fatal(errTestFailed)
	}

	if arr1[2].Integer != 3 {
		t.Fatal(errTestFailed)
	}

	if arr2[0].Status != "Foo" {
		t.Fatal(errTestFailed)
	}
	if arr2[1].Error.Error() != "Bar" {
		t.Fatal(errTestFailed)
	}

}

func TestArrayWithNil(t *testing.T) {
	var test *Message
	var err error

	// Array of two arrays.
	if test, err = respDecoder.decode([]byte("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")); err != nil {
		t.Fatal(err)
	}

	res := test.Array

	if len(res) != 3 {
		t.Fatal(errTestFailed)
	}

	if string(res[0].Bytes) != "foo" {
		t.Fatal(errTestFailed)
	}
	if res[1].IsNil != true {
		t.Fatal(errTestFailed)
	}
	if string(res[2].Bytes) != "bar" {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeString(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode("Foo"); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("+Foo\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeError(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode(errors.New("Fatal error")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("-Fatal error\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeInteger(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode(123); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte(":123\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeBulk(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode([]byte("♥")); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("$3\r\n♥\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeArray(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode([]interface{}{"Foo", "Bar"}); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("*2\r\n+Foo\r\n+Bar\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeMixedArray(t *testing.T) {
	var buf []byte
	var err error

	mixed := []interface{}{
		[]interface{}{
			1, 2, 3,
		},
		[]interface{}{
			[]byte("Foo"),
			errors.New("Bar"),
			"Baz",
		},
	}

	if buf, err = respEncoder.encode(mixed); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*3\r\n$3\r\nFoo\r\n-Bar\r\n+Baz\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeZeroArray(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode([]interface{}{}); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("*0\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestEncodeNil(t *testing.T) {
	var buf []byte
	var err error

	if buf, err = respEncoder.encode(nil); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("$-1\r\n")) == false {
		t.Fatal(errTestFailed)
	}
}

func TestMarshalString(t *testing.T) {
	var buf []byte
	var dest string
	var err error

	if buf, err = Marshal("Test subject."); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("$13\r\nTest subject.\r\n")) == false {
		t.Fatal(errTestFailed)
	}

	if err = Unmarshal(buf, dest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, &dest); err != nil {
		t.Fatal(err)
	}

	if dest != "Test subject." {
		t.Fatal(err)
	}

	if buf, err = Marshal("★"); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("$3\r\n★\r\n")) == false {
		t.Fatal(errTestFailed)
	}

	if err = Unmarshal(buf, dest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, &dest); err != nil {
		t.Fatal(err)
	}

	if dest != "★" {
		t.Fatal(err)
	}
}

func TestMarshalInteger(t *testing.T) {
	var buf []byte
	var dest int
	var err error
	var wrongDest byte

	if buf, err = Marshal(123); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte(":123\r\n")) == false {
		t.Fatal(errTestFailed)
	}

	if err = Unmarshal(buf, nil); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, wrongDest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, &wrongDest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, dest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, &dest); err != nil {
		t.Fatal(err)
	}

	if dest != 123 {
		t.Fatal(err)
	}
}

func TestMarshalArray(t *testing.T) {
	var buf []byte
	var dest []int
	var err error
	var wrongDest bool

	if buf, err = Marshal([]interface{}{1, 2, 3, 4, 5, 6}); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte("*6\r\n:1\r\n:2\r\n:3\r\n:4\r\n:5\r\n:6\r\n")) == false {
		t.Fatal(errTestFailed)
	}

	if err = Unmarshal(buf, nil); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, wrongDest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, &wrongDest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, dest); err == nil {
		t.Fatal(errErrorExpected)
	}

	if err = Unmarshal(buf, &dest); err != nil {
		t.Fatal(err)
	}

}

func TestMarshalUnmarshal(t *testing.T) {
	var buf []byte
	var err error

	var destInt int
	var destString string

	// Marshaling an integer.
	if buf, err = Marshal(123456); err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(buf, []byte(":123456\r\n")) == false {
		t.Fatal(errTestFailed)
	}

	// Attempt to decode this integer into an integer.
	if err = Unmarshal(buf, &destInt); err != nil {
		t.Fatal(err)
	}

	if destInt != 123456 {
		t.Fatal()
	}

	// Attempt to decode this integer into an string.
	if err = Unmarshal(buf, &destString); err != nil {
		t.Fatal(err)
	}

	if destString != "123456" {
		t.Fatal()
	}
}
