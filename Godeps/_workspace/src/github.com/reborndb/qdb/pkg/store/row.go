// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/reborndb/qdb/pkg/engine"
)

var (
	ErrMetaKey = errors.New("invalid meta key")
	ErrDataKey = errors.New("invalid data key")

	ErrNotMatched = errors.New("unmatched raw bytes")

	ErrObjectCode  = errors.New("invalid object code")
	ErrObjectValue = errors.New("invalid object value")

	ErrNotString = errors.New("not string")
	ErrNotHash   = errors.New("not hash")
	ErrNotList   = errors.New("not list")
	ErrNotZSet   = errors.New("not zset")
	ErrNotSet    = errors.New("not set")
)

func EncodeMetaKey(db uint32, key []byte) []byte {
	if len(key) == 0 {
		log.Errorf("encode nil meta key")
	}
	tag, slot := HashKeyToSlot(key)
	if len(tag) == len(key) {
		key = nil
	}
	w := NewBufWriter(nil)
	encodeRawBytes(w, MetaCode, &db, &slot, &tag, &key)
	return w.Bytes()
}

func DecodeMetaKey(p []byte) (db uint32, key []byte, err error) {
	var tag []byte
	var slot uint32
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, MetaCode, &db, &slot, &tag, &key)
	err = decodeRawBytes(r, err)
	if err != nil {
		return
	}
	if len(key) == 0 {
		key = tag
	}
	if len(key) == 0 {
		log.Errorf("decode nil meta key")
	}
	return
}

func EncodeMetaKeyPrefixSlot(db uint32, slot uint32) []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, MetaCode, &db, &slot)
	return w.Bytes()
}

func EncodeMetaKeyPrefixTag(db uint32, tag []byte) []byte {
	slot := HashTagToSlot(tag)
	w := NewBufWriter(nil)
	encodeRawBytes(w, MetaCode, &db, &slot, &tag)
	return w.Bytes()
}

func EncodeDataKeyPrefix(db uint32, key []byte) []byte {
	if len(key) == 0 {
		log.Errorf("encode nil data key")
	}
	w := NewBufWriter(nil)
	encodeRawBytes(w, DataCode, &db, &key)
	return w.Bytes()
}

type storeRow interface {
	Code() ObjectCode

	MetaKey() []byte
	MetaValue() []byte
	ParseMetaValue(p []byte) error

	DataKey() []byte
	DataValue() []byte
	ParseDataValue(p []byte) error

	LoadDataValue(r storeReader) (bool, error)
	TestDataValue(r storeReader) (bool, error)

	GetExpireAt() int64
	SetExpireAt(expireat int64)
	IsExpired() bool

	lazyInit(db uint32, key []byte, h *storeRowHelper)
	storeObject(s *Store, bt *engine.Batch, expireat int64, obj interface{}) error
	deleteObject(s *Store, bt *engine.Batch) error
	loadObjectValue(r storeReader) (interface{}, error)
}

type storeRowHelper struct {
	code          ObjectCode
	metaKey       []byte
	dataKeyPrefix []byte

	ExpireAt int64

	dataKeyRefs   []interface{}
	metaValueRefs []interface{}
	dataValueRefs []interface{}
}

func loadStoreRow(r storeReader, db uint32, key []byte) (storeRow, error) {
	metaKey := EncodeMetaKey(db, key)
	p, err := r.getRowValue(metaKey)
	if err != nil || p == nil {
		return nil, err
	}
	if len(p) == 0 {
		return nil, errors.Trace(ErrObjectCode)
	}
	var o storeRow
	var code = ObjectCode(p[0])
	switch code {
	default:
		return nil, errors.Trace(ErrObjectCode)
	case StringCode:
		o = new(stringRow)
	case HashCode:
		o = new(hashRow)
	case ListCode:
		o = new(listRow)
	case ZSetCode:
		o = new(zsetRow)
	case SetCode:
		o = new(setRow)
	}
	o.lazyInit(db, key, &storeRowHelper{
		code:          code,
		metaKey:       metaKey,
		dataKeyPrefix: EncodeDataKeyPrefix(db, key),
	})
	return o, o.ParseMetaValue(p)
}

func newStoreRowHelper(db uint32, key []byte, code ObjectCode) *storeRowHelper {
	return &storeRowHelper{
		code:          code,
		metaKey:       EncodeMetaKey(db, key),
		dataKeyPrefix: EncodeDataKeyPrefix(db, key),
	}
}

func (o *storeRowHelper) Code() ObjectCode {
	return o.code
}

func (o *storeRowHelper) MetaKey() []byte {
	return o.metaKey
}

func (o *storeRowHelper) MetaValue() []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, o.code, &o.ExpireAt)
	encodeRawBytes(w, o.metaValueRefs...)
	return w.Bytes()
}

func (o *storeRowHelper) ParseMetaValue(p []byte) (err error) {
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, o.code, &o.ExpireAt)
	err = decodeRawBytes(r, err, o.metaValueRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *storeRowHelper) DataKey() []byte {
	if len(o.dataKeyRefs) != 0 {
		w := NewBufWriter(o.DataKeyPrefix())
		encodeRawBytes(w, o.dataKeyRefs...)
		return w.Bytes()
	} else {
		return o.DataKeyPrefix()
	}
}

func (o *storeRowHelper) DataKeyPrefix() []byte {
	return o.dataKeyPrefix
}

func (o *storeRowHelper) ParseDataKeySuffix(p []byte) (err error) {
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, o.dataKeyRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *storeRowHelper) DataValue() []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, o.code)
	encodeRawBytes(w, o.dataValueRefs...)
	return w.Bytes()
}

func (o *storeRowHelper) ParseDataValue(p []byte) (err error) {
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, o.code)
	err = decodeRawBytes(r, err, o.dataValueRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *storeRowHelper) LoadDataValue(r storeReader) (bool, error) {
	p, err := r.getRowValue(o.DataKey())
	if err != nil || p == nil {
		return false, err
	}
	return true, o.ParseDataValue(p)
}

func (o *storeRowHelper) TestDataValue(r storeReader) (bool, error) {
	p, err := r.getRowValue(o.DataKey())
	if err != nil || p == nil {
		return false, err
	}
	return true, nil
}

func (o *storeRowHelper) GetExpireAt() int64 {
	return o.ExpireAt
}

func (o *storeRowHelper) SetExpireAt(expireat int64) {
	o.ExpireAt = expireat
}

func (o *storeRowHelper) IsExpired() bool {
	return IsExpired(o.ExpireAt)
}

func IsExpired(expireat int64) bool {
	return expireat != 0 && expireat <= nowms()
}

const (
	MetaCode = byte('#')
	DataCode = byte('&')

	// for zset
	indexCode = byte('+')
)

type ObjectCode byte

const (
	StringCode ObjectCode = 'K'
	HashCode   ObjectCode = 'H'
	ListCode   ObjectCode = 'L'
	ZSetCode   ObjectCode = 'Z'
	SetCode    ObjectCode = 'S'
)

func (c ObjectCode) String() string {
	switch c {
	case StringCode:
		return "string"
	case HashCode:
		return "hash"
	case ListCode:
		return "list"
	case ZSetCode:
		return "zset"
	case SetCode:
		return "set"
	case 0:
		return "none"
	default:
		return fmt.Sprintf("unknown %02x", byte(c))
	}
}

func encodeRawBytes(w *BufWriter, refs ...interface{}) {
	for _, i := range refs {
		var err error
		switch x := i.(type) {
		case byte:
			err = w.WriteByte(x)
		case ObjectCode:
			err = w.WriteByte(byte(x))
		case *uint32:
			err = w.WriteUvarint(uint64(*x))
		case *uint64:
			err = w.WriteUvarint(*x)
		case *int64:
			err = w.WriteVarint(*x)
		case *float64:
			err = w.WriteFloat64(*x)
		case *[]byte:
			err = w.WriteVarbytes(*x)
		case *scoreInt:
			err = w.WriteUint64(uint64(*x))
		default:
			log.Fatalf("unsupported type in row value: %+v", x)
		}
		if err != nil {
			log.Fatalf("encode raw bytes failed - %s", err)
		}
	}
}

func decodeRawBytes(r *BufReader, err error, refs ...interface{}) error {
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		if r.Len() != 0 {
			return errors.Trace(ErrNotMatched)
		}
		return nil
	}
	for _, i := range refs {
		switch x := i.(type) {
		case byte:
			if v, err := r.ReadByte(); err != nil {
				return err
			} else if v != x {
				return errors.Errorf("read byte %d, expect %d", v, x)
			}
		case ObjectCode:
			if v, err := r.ReadByte(); err != nil {
				return err
			} else if v != byte(x) {
				return errors.Errorf("read code [%s], expect [%s]", ObjectCode(v), x)
			}
		case *[]byte:
			p, err := r.ReadVarbytes()
			if err != nil {
				return err
			}
			*x = p
		case *uint32:
			v, err := r.ReadUvarint()
			if err != nil {
				return err
			}
			*x = uint32(v)
		case *uint64:
			v, err := r.ReadUvarint()
			if err != nil {
				return err
			}
			*x = v
		case *int64:
			v, err := r.ReadVarint()
			if err != nil {
				return err
			}
			*x = v
		case *float64:
			v, err := r.ReadFloat64()
			if err != nil {
				return err
			}
			*x = v
		case *scoreInt:
			v, err := r.ReadUint64()
			if err != nil {
				return err
			}
			*x = scoreInt(v)
		case *byte:
			v, err := r.ReadByte()
			if err != nil {
				return err
			}
			*x = v
		default:
			log.Fatalf("unsupported type in row value: %+v", x)
		}
	}
	return nil
}
