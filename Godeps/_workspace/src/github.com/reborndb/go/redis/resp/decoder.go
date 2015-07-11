// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package resp

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/go/io/ioutils"
)

type decoder struct {
	r *bufio.Reader
}

func Decode(r *bufio.Reader) (Resp, error) {
	d := &decoder{r}
	return d.decodeResp()
}

// Decode RESP Request, return must be array type or ping type
func DecodeRequest(r *bufio.Reader) (Resp, error) {
	d := &decoder{r}
	return d.decodeRequest()
}

func MustDecode(r *bufio.Reader) Resp {
	resp, err := Decode(r)
	if err != nil {
		log.Fatalf("decode redis resp failed - %s", err)
	}
	return resp
}

func DecodeFromBytes(p []byte) (Resp, error) {
	r := bufio.NewReader(bytes.NewReader(p))
	return Decode(r)
}

func DecodeRequestFromBytes(p []byte) (Resp, error) {
	r := bufio.NewReader(bytes.NewReader(p))
	return DecodeRequest(r)
}

func MustDecodeFromBytes(p []byte) Resp {
	resp, err := DecodeFromBytes(p)
	if err != nil {
		log.Fatalf("decode redis resp from bytes failed - %s", err)
	}
	return resp
}

func isLetter(c byte) bool {
	if c >= 'a' && c <= 'z' {
		return true
	}

	if c >= 'A' && c <= 'Z' {
		return true
	}

	return false
}

func (d *decoder) decodeRequest() (Resp, error) {
	t, err := d.decodeType()
	if err != nil {
		return nil, err
	}
	switch t {
	default:
		if !isLetter(byte(t)) {
			return nil, errors.Trace(ErrBadRespType)
		}
		// may be it's telnet text format
		d.r.UnreadByte()

		t, err := d.decodeText()
		if err != nil {
			return nil, err
		}

		items := strings.Fields(t)
		resp := &Array{}
		for _, item := range items {
			resp.AppendBulkBytes([]byte(item))
		}
		return resp, nil

	case TypeString, TypeError, TypeInt, TypeBulkBytes:
		return nil, errors.Trace(ErrBadRespType)
	case TypePing:
		return NewPing(), nil
	case TypeArray:
		resp := &Array{}
		resp.Value, err = d.decodeArray()
		return resp, err
	}
}

func (d *decoder) decodeResp() (Resp, error) {
	t, err := d.decodeType()
	if err != nil {
		return nil, err
	}
	switch t {
	default:
		return nil, errors.Trace(ErrBadRespType)
	case TypeString:
		resp := &String{}
		resp.Value, err = d.decodeText()
		return resp, err
	case TypeError:
		resp := &Error{}
		resp.Value, err = d.decodeText()
		return resp, err
	case TypeInt:
		resp := &Int{}
		resp.Value, err = d.decodeInt()
		return resp, err
	case TypeBulkBytes:
		resp := &BulkBytes{}
		resp.Value, err = d.decodeBulkBytes()
		return resp, err
	case TypeArray:
		resp := &Array{}
		resp.Value, err = d.decodeArray()
		return resp, err
	case TypePing:
		return NewPing(), nil
	}
}

func (d *decoder) decodeType() (RespType, error) {
	if b, err := d.r.ReadByte(); err != nil {
		return 0, errors.Trace(err)
	} else {
		return RespType(b), nil
	}
}

func (d *decoder) decodeText() (string, error) {
	b, err := d.r.ReadBytes('\n')
	if err != nil {
		return "", errors.Trace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return "", errors.Trace(ErrBadRespEnd)
	} else {
		return string(b[:n]), nil
	}
}

func (d *decoder) decodeInt() (int64, error) {
	b, err := d.decodeText()
	if err != nil {
		return 0, err
	}
	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, errors.Trace(err)
	} else {
		return n, nil
	}
}

func (d *decoder) decodeBulkBytes() ([]byte, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, errors.Trace(ErrBadRespBytesLen)
	} else if n == -1 {
		return nil, nil
	}
	b := make([]byte, n+2)
	if _, err := ioutils.ReadFull(d.r, b); err != nil {
		return nil, errors.Trace(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errors.Trace(ErrBadRespEnd)
	}
	return b[:n], nil
}

func (d *decoder) decodeArray() ([]Resp, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, errors.Trace(ErrBadRespArrayLen)
	} else if n == -1 {
		return nil, nil
	}
	a := make([]Resp, n)
	for i := 0; i < len(a); i++ {
		if a[i], err = d.decodeResp(); err != nil {
			return nil, err
		}
	}
	return a, nil
}
