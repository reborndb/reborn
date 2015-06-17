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

// RESP encoder. See: http://redis.io/topics/protocol
package resp

import (
	"bytes"
	"strconv"
)

type encoder struct {
}

var (
	encoderNil = []byte{'$', '-', '1', '\r', '\n'}
)

func (self encoder) encode(in interface{}) ([]byte, error) {
	switch v := in.(type) {
	case nil:
		return encoderNil, nil
	case string:
		out := bytes.Join([][]byte{{StringHeader}, []byte(v), endOfLine}, nil)
		return out, nil
	case error:
		out := bytes.Join([][]byte{{ErrorHeader}, []byte(v.Error()), endOfLine}, nil)
		return out, nil
	case int:
		out := bytes.Join([][]byte{{IntegerHeader}, []byte(strconv.Itoa(v)), endOfLine}, nil)
		return out, nil
	case []byte:
		out := bytes.Join([][]byte{{BulkHeader}, []byte(strconv.Itoa(len(v))), endOfLine, v, endOfLine}, nil)
		return out, nil
	case []interface{}:
		var buf bytes.Buffer
		buf.Write(bytes.Join([][]byte{{ArrayHeader}, []byte(strconv.Itoa(len(v))), endOfLine}, nil))
		for i := range v {
			chunk, err := self.encode(v[i])
			if err != nil {
				return nil, err
			}
			buf.Write(chunk)
		}
		return buf.Bytes(), nil
	}
	return nil, ErrInvalidInput
}
