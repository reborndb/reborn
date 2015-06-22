// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package resp

import (
	"fmt"

	"github.com/reborndb/go/errors"
)

var (
	ErrBadRespType     = errors.Static("bad resp type")
	ErrBadRespEnd      = errors.Static("bad resp end")
	ErrBadRespInt      = errors.Static("bad resp int")
	ErrBadRespBytesLen = errors.Static("bad resp bytes len")
	ErrBadRespArrayLen = errors.Static("bad resp array len")
)

type RespType byte

const (
	TypeString    RespType = '+'
	TypeError     RespType = '-'
	TypeInt       RespType = ':'
	TypeBulkBytes RespType = '$'
	TypeArray     RespType = '*'
	TypePing      RespType = '\n'
)

func (t RespType) String() string {
	switch t {
	case TypeString:
		return "<string>"
	case TypeError:
		return "<error>"
	case TypeInt:
		return "<int>"
	case TypeBulkBytes:
		return "<bulkbytes>"
	case TypeArray:
		return "<array>"
	case TypePing:
		return "<ping>"
	default:
		return "<unknown>"
	}
}

type Resp interface {
	Type() RespType
}

type String struct {
	Value string
}

func NewString(s string) *String {
	return &String{s}
}

func (r *String) Type() RespType {
	return TypeString
}

type Error struct {
	Value string
}

func NewError(err error) *Error {
	return &Error{err.Error()}
}

func NewErrorWithString(s string) *Error {
	return &Error{s}
}

func (r *Error) Type() RespType {
	return TypeError
}

type Int struct {
	Value int64
}

func NewInt(n int64) *Int {
	return &Int{n}
}

func (r *Int) Type() RespType {
	return TypeInt
}

type BulkBytes struct {
	Value []byte
}

func NewBulkBytes(p []byte) *BulkBytes {
	return &BulkBytes{p}
}

func NewBulkBytesWithString(s string) *BulkBytes {
	return &BulkBytes{[]byte(s)}
}

func (r *BulkBytes) Type() RespType {
	return TypeBulkBytes
}

type Array struct {
	Value []Resp
}

func NewArray() *Array {
	return &Array{}
}

func (r *Array) Type() RespType {
	return TypeArray
}

func (r *Array) Append(a Resp) {
	r.Value = append(r.Value, a)
}

func (r *Array) AppendString(s string) {
	r.Append(NewString(s))
}

func (r *Array) AppendBulkBytes(b []byte) {
	r.Append(NewBulkBytes(b))
}

func (r *Array) AppendInt(n int64) {
	r.Append(NewInt(n))
}

func (r *Array) AppendError(err error) {
	r.Append(NewError(err))
}

type Ping byte

func NewPing() Ping {
	return Ping('\n')
}

func (r Ping) Type() RespType {
	return TypePing
}

// RESP Request is a array of bulk strings.
func NewRequest(cmd string, args ...interface{}) Resp {
	ay := NewArray()
	ay.AppendBulkBytes([]byte(cmd))

	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			ay.AppendBulkBytes([]byte(v))
		case []byte:
			ay.AppendBulkBytes(v)
		case nil:
			// we use an empty string for nil arg
			ay.AppendBulkBytes([]byte(""))
		default:
			ay.AppendBulkBytes([]byte(fmt.Sprintf("%v", v)))
		}
	}

	return ay
}
