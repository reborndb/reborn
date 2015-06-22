// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/juju/errors"
	"github.com/reborndb/go/redis/rdb"
	"github.com/reborndb/qdb/pkg/engine"
)

var errTravelBreak = errors.New("break current travel")

type zsetRow struct {
	*storeRowHelper

	Size   int64
	Member []byte
	Score  float64

	indexKeyPrefix []byte
	indexKeyRefs   []interface{}
	indexValueRefs []interface{}
}

func encodeIndexKeyPrefix(db uint32, key []byte) []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, indexCode, &db, &key)
	return w.Bytes()
}

func newZSetRow(db uint32, key []byte) *zsetRow {
	o := &zsetRow{}
	o.lazyInit(db, key, newStoreRowHelper(db, key, ZSetCode))
	return o
}

func (o *zsetRow) lazyInit(db uint32, key []byte, h *storeRowHelper) {
	o.storeRowHelper = h
	o.metaValueRefs = []interface{}{&o.Size}

	o.dataKeyRefs = []interface{}{&o.Member}
	o.dataValueRefs = []interface{}{&o.Score}

	o.indexKeyPrefix = encodeIndexKeyPrefix(db, key)
	o.indexKeyRefs = []interface{}{&o.Score, &o.Member}
	o.indexValueRefs = nil
}

func (o *zsetRow) IndexKeyPrefix() []byte {
	return o.indexKeyPrefix
}

func (o *zsetRow) IndexKey() []byte {
	w := NewBufWriter(o.IndexKeyPrefix())

	encodeRawBytes(w, o.indexKeyRefs...)
	return w.Bytes()
}

type scoreInt uint64

func (o *zsetRow) indexNextScoreKey() []byte {
	w := NewBufWriter(o.IndexKeyPrefix())

	nextScore := scoreInt(float64ToUint64(o.Score) + 1)

	encodeRawBytes(w, &nextScore, &o.Member)

	return w.Bytes()
}

func (o *zsetRow) IndexValue() []byte {
	w := NewBufWriter(nil)
	encodeRawBytes(w, o.code)
	encodeRawBytes(w, o.indexValueRefs...)
	return w.Bytes()
}

func (o *zsetRow) ParseIndexKeySuffix(p []byte) (err error) {
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, o.indexKeyRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *zsetRow) ParseIndexValue(p []byte) (err error) {
	r := NewBufReader(p)
	err = decodeRawBytes(r, err, o.code)
	err = decodeRawBytes(r, err, o.indexValueRefs...)
	err = decodeRawBytes(r, err)
	return
}

func (o *zsetRow) LoadIndexValue(r storeReader) (bool, error) {
	p, err := r.getRowValue(o.IndexKey())
	if err != nil || p == nil {
		return false, err
	}
	return true, o.ParseIndexValue(p)
}

func (o *zsetRow) TestIndexValue(r storeReader) (bool, error) {
	p, err := r.getRowValue(o.IndexKey())
	if err != nil || p == nil {
		return false, err
	}
	return true, nil
}

func (o *zsetRow) deleteObject(s *Store, bt *engine.Batch) error {
	it := s.getIterator()
	defer s.putIterator(it)
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		bt.Del(key)
	}

	for pfx := it.SeekTo(o.IndexKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		bt.Del(key)
	}

	bt.Del(o.MetaKey())
	return it.Error()
}

func (o *zsetRow) storeObject(s *Store, bt *engine.Batch, expireat int64, obj interface{}) error {
	zset, ok := obj.(rdb.ZSet)
	if !ok || len(zset) == 0 {
		return errors.Trace(ErrObjectValue)
	}
	for i, e := range zset {
		if e == nil {
			return errArguments("zset[%d] is nil", i)
		}
		if len(e.Member) == 0 {
			return errArguments("zset[%d], len(member) = %d", i, len(e.Member))
		}
	}

	ms := &markSet{}
	for _, e := range zset {
		o.Member, o.Score = e.Member, e.Score
		if math.IsNaN(o.Score) {
			return errors.Errorf("invalid nan score")
		}

		ms.Set(o.Member)
		bt.Set(o.DataKey(), o.DataValue())
		bt.Set(o.IndexKey(), o.IndexValue())
	}
	o.Size, o.ExpireAt = ms.Len(), expireat
	bt.Set(o.MetaKey(), o.MetaValue())
	return nil
}

func (o *zsetRow) loadObjectValue(r storeReader) (interface{}, error) {
	zset := make([]*rdb.ZSetElement, 0, o.Size)
	it := r.getIterator()
	defer r.putIterator(it)
	for pfx := it.SeekTo(o.DataKeyPrefix()); it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		sfx := key[len(pfx):]
		if err := o.ParseDataKeySuffix(sfx); err != nil {
			return nil, err
		}
		if err := o.ParseDataValue(it.Value()); err != nil {
			return nil, err
		}
		zset = append(zset, &rdb.ZSetElement{Member: o.Member, Score: float64(o.Score)})
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if o.Size == 0 || int64(len(zset)) != o.Size {
		return nil, errors.Errorf("len(zset) = %d, zset.size = %d", len(zset), o.Size)
	}
	return rdb.ZSet(zset), nil
}

func (s *Store) loadZSetRow(db uint32, key []byte, deleteIfExpired bool) (*zsetRow, error) {
	o, err := s.loadStoreRow(db, key, deleteIfExpired)
	if err != nil {
		return nil, err
	} else if o != nil {
		x, ok := o.(*zsetRow)
		if ok {
			return x, nil
		}
		return nil, errors.Trace(ErrNotZSet)
	}
	return nil, nil
}

// ZGETALL key
func (s *Store) ZGetAll(db uint32, args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return nil, err
	}

	x, err := o.loadObjectValue(s)
	if err != nil || x == nil {
		return nil, err
	}

	eles := x.(rdb.ZSet)
	rets := make([][]byte, len(eles)*2)
	for i, e := range eles {
		rets[i*2], rets[i*2+1] = e.Member, FormatFloat(e.Score)
	}
	return rets, nil
}

// ZCARD key
func (s *Store) ZCard(db uint32, args [][]byte) (int64, error) {
	if len(args) != 1 {
		return 0, errArguments("len(args) = %d, expect = 1", len(args))
	}

	key := args[0]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}
	return o.Size, nil
}

// ZADD key score member [score member ...]
func (s *Store) ZAdd(db uint32, args [][]byte) (int64, error) {
	if len(args) == 1 || len(args)%2 != 1 {
		return 0, errArguments("len(args) = %d, expect != 1 && mod 2 = 1", len(args))
	}

	key := args[0]

	var eles = make([]struct {
		Member []byte
		Score  float64
	}, len(args)/2)

	var err error
	for i := 0; i < len(eles); i++ {
		e := &eles[i]
		e.Score, err = ParseFloat(args[i*2+1])
		if err != nil {
			return 0, errArguments("parse args failed - %s", err)
		}

		e.Member = args[i*2+2]
		if len(e.Member) == 0 {
			return 0, errArguments("parse args[%d] failed, empty empty", i*2+2)
		}
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	if o == nil {
		o = newZSetRow(db, key)
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, e := range eles {
		o.Member = e.Member
		exists, err := o.LoadDataValue(s)
		if err != nil {
			return 0, err
		}
		if !exists {
			ms.Set(o.Member)
		} else {
			// if old exists, remove index key first
			bt.Del(o.IndexKey())
		}

		o.Score = e.Score

		bt.Set(o.DataKey(), o.DataValue())
		bt.Set(o.IndexKey(), o.IndexValue())
	}

	n := ms.Len()
	if n != 0 {
		o.Size += n
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	fw := &Forward{DB: db, Op: "ZAdd", Args: args}
	return n, s.commit(bt, fw)
}

// ZREM key member [member ...]
func (s *Store) ZRem(db uint32, args [][]byte) (int64, error) {
	if len(args) < 2 {
		return 0, errArguments("len(args) = %d, expect >= 2", len(args))
	}

	key := args[0]
	members := args[1:]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, err
	}

	ms := &markSet{}
	bt := engine.NewBatch()
	for _, o.Member = range members {
		if !ms.Has(o.Member) {
			exists, err := o.LoadDataValue(s)
			if err != nil {
				return 0, err
			}
			if exists {
				bt.Del(o.DataKey())
				bt.Del(o.IndexKey())
				ms.Set(o.Member)
			}
		}
	}

	n := ms.Len()
	if n != 0 {
		if o.Size -= n; o.Size > 0 {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
	}
	fw := &Forward{DB: db, Op: "ZRem", Args: args}
	return n, s.commit(bt, fw)
}

// ZSCORE key member
func (s *Store) ZScore(db uint32, args [][]byte) (float64, bool, error) {
	if len(args) != 2 {
		return 0, false, errArguments("len(args) = %d, expect = 2", len(args))
	}

	key := args[0]
	member := args[1]

	if err := s.acquire(); err != nil {
		return 0, false, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil || o == nil {
		return 0, false, err
	}

	o.Member = member
	exists, err := o.LoadDataValue(s)
	if err != nil || !exists {
		return 0, false, err
	} else {
		return o.Score, true, nil
	}
}

// ZINCRBY key delta member
func (s *Store) ZIncrBy(db uint32, args [][]byte) (float64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	delta, err := ParseFloat(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	member := args[2]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	bt := engine.NewBatch()

	var exists bool = false
	if o != nil {
		o.Member = member
		exists, err = o.LoadDataValue(s)
		if err != nil {
			return 0, err
		} else if exists {
			bt.Del(o.IndexKey())
		}
	} else {
		o = newZSetRow(db, key)
		o.Member = member
	}

	if exists {
		delta += o.Score
	} else {
		o.Size++
		bt.Set(o.MetaKey(), o.MetaValue())
	}
	o.Score = delta
	if math.IsNaN(delta) {
		return 0, errors.Errorf("invalid nan score")
	}

	bt.Set(o.DataKey(), o.DataValue())
	bt.Set(o.IndexKey(), o.IndexValue())

	fw := &Forward{DB: db, Op: "ZIncrBy", Args: args}
	return delta, s.commit(bt, fw)
}

// holds a inclusive/exclusive range spec by score comparison
type rangeSpec struct {
	Min float64
	Max float64

	// are min or max score exclusive
	MinEx bool
	MaxEx bool
}

func (r *rangeSpec) GteMin(v float64) bool {
	if r.MinEx {
		return v > r.Min
	} else {
		return v >= r.Min
	}
}

func (r *rangeSpec) LteMax(v float64) bool {
	if r.MaxEx {
		return v < r.Max
	} else {
		return v <= r.Max
	}
}

func (r *rangeSpec) InRange(v float64) bool {
	if r.Min > r.Max || (r.Min == r.Max && (r.MinEx || r.MaxEx)) {
		return false
	}

	if !r.GteMin(v) {
		return false
	}

	if !r.LteMax(v) {
		return false
	}
	return true
}

func parseRangeScore(buf []byte) (float64, bool, error) {
	if len(buf) == 0 {
		return 0, false, errors.Errorf("empty range score argument")
	}

	ex := false
	if buf[0] == '(' {
		buf = buf[1:]
		ex = true
	}

	f, err := strconv.ParseFloat(string(buf), 64)
	if err != nil {
		return 0, ex, errors.Trace(err)
	} else if math.IsNaN(f) {
		return 0, ex, errors.Errorf("invalid nan score")
	}

	return f, ex, nil
}

func parseRangeSpec(min []byte, max []byte) (*rangeSpec, error) {
	var r rangeSpec
	var err error

	if r.Min, r.MinEx, err = parseRangeScore(min); err != nil {
		return nil, err
	}

	if r.Max, r.MaxEx, err = parseRangeScore(max); err != nil {
		return nil, err
	}

	return &r, nil
}

// travel zset in range, call f in every iteration.
func (o *zsetRow) travelInRange(s *Store, r *rangeSpec, f func(o *zsetRow) error) error {
	it := s.getIterator()
	defer s.putIterator(it)

	o.Score = r.Min
	o.Member = []byte{}

	it.SeekTo(o.IndexKey())
	prefixKey := o.IndexKeyPrefix()
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return errors.Trace(err)
		}

		if r.InRange(o.Score) {
			if err := f(o); err == errTravelBreak {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
		} else if !r.LteMax(o.Score) {
			return nil
		}
	}

	if err := it.Error(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (o *zsetRow) seekToLastInRange(it *storeIterator, r *rangeSpec) {
	o.Score = r.Max
	o.Member = []byte{}
	it.SeekTo(o.indexNextScoreKey())
	if !it.Valid() {
		// try seek to last
		it.SeekToLast()
	} else {
		// there exists a data but is not mine
		it.Prev()
	}
}

// reverse travel zset in range, call f in every iteration.
func (o *zsetRow) reverseTravelInRange(s *Store, r *rangeSpec, f func(o *zsetRow) error) error {
	it := s.getIterator()
	defer s.putIterator(it)

	prefixKey := o.IndexKeyPrefix()

	o.seekToLastInRange(it, r)

	for ; it.Valid(); it.Prev() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return errors.Trace(err)
		}

		if r.InRange(o.Score) {
			if err := f(o); err == errTravelBreak {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
		} else if !r.GteMin(o.Score) {
			return nil
		}
	}

	if err := it.Error(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ZCOUNT key min max
func (s *Store) ZCount(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	min := args[1]
	max := args[2]

	r, err := parseRangeSpec(min, max)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var count int64 = 0
	f := func(o *zsetRow) error {
		count++
		return nil
	}

	if err = o.travelInRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	return count, nil
}

var (
	minString []byte = make([]byte, 9)
	maxString []byte = make([]byte, 9)
)

// we can only use internal pointer to check whether slice is min/max string or not.
func isSameSlice(a []byte, b []byte) bool {
	pa := (*reflect.SliceHeader)(unsafe.Pointer(&a))
	pb := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return pa.Data == pb.Data
}

func isMinString(a []byte) bool {
	return isSameSlice(a, minString)
}

func isMaxString(a []byte) bool {
	return isSameSlice(a, maxString)
}

func isInfSting(a []byte) bool {
	return isMinString(a) || isMaxString(a)
}

type lexRangeSpec struct {
	Min   []byte
	Max   []byte
	MinEx bool
	MaxEx bool
}

func (r *lexRangeSpec) GteMin(v []byte) bool {
	if isMinString(r.Min) {
		return true
	} else if isMaxString(r.Min) {
		return false
	}

	if r.MinEx {
		return bytes.Compare(v, r.Min) > 0
	} else {
		return bytes.Compare(v, r.Min) >= 0
	}
}

func (r *lexRangeSpec) LteMax(v []byte) bool {
	if isMaxString(r.Max) {
		return true
	} else if isMinString(r.Max) {
		return false
	}

	if r.MaxEx {
		return bytes.Compare(r.Max, v) > 0
	} else {
		return bytes.Compare(r.Max, v) >= 0
	}
}

func (r *lexRangeSpec) InRange(v []byte) bool {
	if !isInfSting(r.Min) && !isInfSting(r.Max) {
		if bytes.Compare(r.Min, r.Max) == 0 && (r.MinEx || r.MaxEx) {
			return false
		} else if bytes.Compare(r.Min, r.Max) > 0 {
			return false
		}
	}

	if !r.GteMin(v) {
		return false
	}

	if !r.LteMax(v) {
		return false
	}

	return true
}

func parseLexRangeItem(buf []byte) ([]byte, bool, error) {
	if len(buf) == 0 {
		return nil, false, errors.Errorf("empty lex range item")
	}

	ex := false
	var dest []byte

	switch buf[0] {
	case '+':
		if len(buf) > 1 {
			return nil, false, errors.Errorf("invalid lex range item, only +  allowed, but %s", buf)
		}
		dest = maxString
	case '-':
		if len(buf) > 1 {
			return nil, false, errors.Errorf("invalid lex range item, only - allowed, but %s", buf)
		}
		dest = minString
	case '(', '[':
		dest = buf[1:]
		if len(dest) == 0 {
			return nil, false, errors.Errorf("invalid empty lex range item %s", buf)
		}
		ex = buf[0] == '('
	default:
		return nil, false, errors.Errorf("invalid lex range item at first byte, %s", buf)
	}

	return dest, ex, nil
}

func parseLexRangeSpec(min []byte, max []byte) (*lexRangeSpec, error) {
	var r lexRangeSpec
	var err error

	r.Min, r.MinEx, err = parseLexRangeItem(min)
	if err != nil {
		return nil, err
	}

	r.Max, r.MaxEx, err = parseLexRangeItem(max)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// travel zset in lex range, call f in every iteration.
func (o *zsetRow) travelInLexRange(s *Store, r *lexRangeSpec, f func(o *zsetRow) error) error {
	it := s.getIterator()
	defer s.putIterator(it)

	o.Score = math.Inf(-1)
	o.Member = r.Min

	it.SeekTo(o.IndexKey())
	prefixKey := o.IndexKeyPrefix()
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return errors.Trace(err)
		}

		if r.InRange(o.Member) {
			if err := f(o); err == errTravelBreak {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
		} else if !r.LteMax(o.Member) {
			return nil
		}
	}

	if err := it.Error(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (o *zsetRow) seekToLastInLexRange(it *storeIterator, r *lexRangeSpec) {
	o.Score = math.Inf(1)
	o.Member = r.Max

	it.SeekTo(o.indexNextScoreKey())
	if !it.Valid() {
		// we will try to use SeekToLast
		it.SeekToLast()
	} else {
		// there is a data but not mine, step prev
		it.Prev()
	}
}

// reverse travel zset in lex range, call f in every iteration.
func (o *zsetRow) reverseTravelInLexRange(s *Store, r *lexRangeSpec, f func(o *zsetRow) error) error {
	it := s.getIterator()
	defer s.putIterator(it)

	prefixKey := o.IndexKeyPrefix()

	o.seekToLastInLexRange(it, r)

	for ; it.Valid(); it.Prev() {
		key := it.Key()
		if !bytes.HasPrefix(key, prefixKey) {
			return nil
		}

		key = key[len(prefixKey):]

		if err := o.ParseIndexKeySuffix(key); err != nil {
			return errors.Trace(err)
		}

		if r.InRange(o.Member) {
			if err := f(o); err == errTravelBreak {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
		} else if !r.GteMin(o.Member) {
			return nil
		}
	}

	if err := it.Error(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ZLEXCOUNT key min max
func (s *Store) ZLexCount(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect = 3", len(args))
	}

	key := args[0]
	min := args[1]
	max := args[2]

	r, err := parseLexRangeSpec(min, max)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	}

	var count int64 = 0
	f := func(o *zsetRow) error {
		count++
		return nil
	}

	if err = o.travelInLexRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	return count, nil
}

func (s *Store) genericZRange(db uint32, args [][]byte, reverse bool) ([][]byte, error) {
	if len(args) != 3 && len(args) != 4 {
		return nil, errArguments("len(args) = %d, expect = 3/4", len(args))
	}

	key := args[0]
	start, err := ParseInt(args[1])
	if err != nil {
		return nil, errArguments("parse args failed - %s", err)
	}
	stop, err := ParseInt(args[2])
	if err != nil {
		return nil, errArguments("parse args failed - %s", err)
	}

	withScore := 1
	if len(args) == 4 {
		if strings.ToUpper(FormatString(args[3])) != "WITHSCORES" {
			return nil, errArguments("parse args[3] failed, must WITHSCORES")
		}
		withScore = 2
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return nil, err
	}

	var rangeLen int64
	start, stop, rangeLen = sanitizeIndexes(start, stop, o.Size)
	if rangeLen == 0 {
		// empty
		return [][]byte{}, nil
	}

	r := &rangeSpec{Min: math.Inf(-1), Max: math.Inf(1), MinEx: true, MaxEx: true}

	res := make([][]byte, 0, rangeLen*int64(withScore))
	offset := int64(0)
	f := func(o *zsetRow) error {
		if offset >= start {
			res = append(res, o.Member)
			if withScore == 2 {
				res = append(res, FormatFloat(o.Score))
			}

			rangeLen--
			if rangeLen <= 0 {
				return errTravelBreak
			}

		}
		offset++
		return nil
	}

	if !reverse {
		err = o.travelInRange(s, r, f)
	} else {
		err = o.reverseTravelInRange(s, r, f)
	}

	return res, errors.Trace(err)
}

// ZRANGE key start stop [WITHSCORES]
func (s *Store) ZRange(db uint32, args [][]byte) ([][]byte, error) {
	return s.genericZRange(db, args, false)
}

// ZREVRANGE key start stop [WITHSCORES]
func (s *Store) ZRevRange(db uint32, args [][]byte) ([][]byte, error) {
	return s.genericZRange(db, args, true)
}

func sanitizeIndexes(start int64, stop int64, size int64) (int64, int64, int64) {
	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}

	if start < 0 {
		start = 0
	}

	if start > stop || start >= size {
		// empty
		return start, stop, 0
	}

	if stop >= size {
		stop = size - 1
	}

	return start, stop, (stop - start) + 1
}

func (s *Store) genericZRangeBylex(db uint32, args [][]byte, reverse bool) ([][]byte, error) {
	if len(args) != 3 && len(args) != 6 {
		return nil, errArguments("len(args) = %d, expect = 3 or 6", len(args))
	}

	key := args[0]
	min := args[1]
	max := args[2]

	if reverse {
		min, max = max, min
	}

	var offset int64 = 0
	var count int64 = -1
	var err error
	if len(args) == 6 {
		if strings.ToUpper(FormatString(args[3])) != "LIMIT" {
			return nil, errArguments("parse args[3] failed, no limit")
		}
		if offset, err = ParseInt(args[4]); err != nil {
			return nil, errArguments("parse args[4] failed, %v", err)
		}
		if count, err = ParseInt(args[5]); err != nil {
			return nil, errArguments("parse args[5] failed, %v", err)
		}
	}

	r, err := parseLexRangeSpec(min, max)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return nil, err
	}

	res := make([][]byte, 0, 4)
	n := int64(0)
	f := func(o *zsetRow) error {
		if n >= offset {
			if count == 0 {
				return errTravelBreak
			}

			res = append(res, o.Member)

			count--
		}

		n++
		return nil
	}

	if !reverse {
		err = o.travelInLexRange(s, r, f)
	} else {
		err = o.reverseTravelInLexRange(s, r, f)
	}

	return res, errors.Trace(err)
}

// ZRANGEBYLEX key min max [LIMIT offset count]
func (s *Store) ZRangeByLex(db uint32, args [][]byte) ([][]byte, error) {
	return s.genericZRangeBylex(db, args, false)
}

// ZRevRANGEBYLEX key min max [LIMIT offset count]
func (s *Store) ZRevRangeByLex(db uint32, args [][]byte) ([][]byte, error) {
	return s.genericZRangeBylex(db, args, true)
}

func (s *Store) genericZRangeByScore(db uint32, args [][]byte, reverse bool) ([][]byte, error) {
	if len(args) < 3 {
		return nil, errArguments("len(args) = %d, expect >= 3", len(args))
	}

	key := args[0]
	min := args[1]
	max := args[2]

	if reverse {
		min, max = max, min
	}

	r, err := parseRangeSpec(min, max)
	if err != nil {
		return nil, errors.Trace(err)
	}

	withScore := 1
	var offset int64 = 0
	var count int64 = -1
	for i := 3; i < len(args); {
		switch strings.ToUpper(FormatString(args[i])) {
		case "WITHSCORES":
			withScore = 2
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, errArguments("parse args[%d] failed, invalid limit format", i)
			}

			if offset, err = ParseInt(args[i+1]); err != nil {
				return nil, errArguments("parse args[%d] failed, %v", i+1, err)
			}
			if count, err = ParseInt(args[i+2]); err != nil {
				return nil, errArguments("parse args[%d] failed, %v", i+2, err)
			}
			i += 3
		default:
			return nil, errArguments("parse args[%d] failed, %s", i, args[i])
		}
	}

	res := make([][]byte, 0, 4)
	n := int64(0)
	f := func(o *zsetRow) error {
		if n >= offset {
			if count == 0 {
				return errTravelBreak
			}

			res = append(res, o.Member)
			if withScore == 2 {
				res = append(res, FormatFloat(o.Score))
			}

			count--
		}

		n++
		return nil
	}

	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return nil, err
	}

	if !reverse {
		err = o.travelInRange(s, r, f)
	} else {
		err = o.reverseTravelInRange(s, r, f)
	}

	return res, errors.Trace(err)
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func (s *Store) ZRangeByScore(db uint32, args [][]byte) ([][]byte, error) {
	return s.genericZRangeByScore(db, args, false)
}

// ZREVRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func (s *Store) ZRevRangeByScore(db uint32, args [][]byte) ([][]byte, error) {
	return s.genericZRangeByScore(db, args, true)
}

func (s *Store) genericZRank(db uint32, args [][]byte, reverse bool) (int64, error) {
	if len(args) != 2 {
		return 0, errArguments("len(args) = %d, expect 2", len(args))
	}

	key := args[0]
	member := args[1]

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	} else if o == nil {
		return -1, nil
	}

	o.Member = member
	exists, err := o.LoadDataValue(s)
	if err != nil {
		return 0, errors.Trace(err)
	} else if !exists {
		return -1, nil
	}

	r := &rangeSpec{Min: math.Inf(-1), Max: o.Score, MinEx: true, MaxEx: false}
	n := int64(1)
	checkScore := o.Score
	f := func(o *zsetRow) error {
		if checkScore > o.Score {
			n++
		}
		return nil
	}

	if err := o.travelInRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	if !reverse {
		return n - 1, nil
	} else {
		return o.Size - n, nil
	}
}

// ZRANK key member
func (s *Store) ZRank(db uint32, args [][]byte) (int64, error) {
	return s.genericZRank(db, args, false)
}

// ZREVRANK key member
func (s *Store) ZRevRank(db uint32, args [][]byte) (int64, error) {
	return s.genericZRank(db, args, true)
}

// ZREMRANGEBYLEX key min max
func (s *Store) ZRemRangeByLex(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect 3", len(args))
	}

	key := args[0]
	min := args[1]
	max := args[2]

	r, err := parseLexRangeSpec(min, max)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	} else if o == nil {
		return 0, nil
	}

	bt := engine.NewBatch()
	n := int64(0)

	f := func(o *zsetRow) error {
		bt.Del(o.DataKey())
		bt.Del(o.IndexKey())
		n++
		return nil
	}

	if err := o.travelInLexRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	if n > 0 {
		if o.Size -= n; o.Size > 0 {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
	}

	fw := &Forward{DB: db, Op: "ZRemRangeByLex", Args: args}
	return n, s.commit(bt, fw)
}

// ZREMRANGEBYRANK key start stop
func (s *Store) ZRemRangeByRank(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect 3", len(args))
	}

	key := args[0]
	start, err := ParseInt(args[1])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}
	stop, err := ParseInt(args[2])
	if err != nil {
		return 0, errArguments("parse args failed - %s", err)
	}

	r := &rangeSpec{Min: math.Inf(-1), Max: math.Inf(1), MinEx: true, MaxEx: true}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	} else if o == nil {
		return 0, nil
	}

	var rangeLen int64
	start, stop, rangeLen = sanitizeIndexes(start, stop, o.Size)

	if rangeLen == 0 {
		return 0, nil
	}

	bt := engine.NewBatch()
	n := int64(0)

	offset := int64(0)

	f := func(o *zsetRow) error {
		if offset >= start {
			bt.Del(o.DataKey())
			bt.Del(o.IndexKey())
			n++
			rangeLen--
			if rangeLen <= 0 {
				return errTravelBreak
			}

		}
		offset++
		return nil
	}

	if err := o.travelInRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	if n > 0 {
		if o.Size -= n; o.Size > 0 {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
	}

	fw := &Forward{DB: db, Op: "ZRemRangeByRank", Args: args}
	return n, s.commit(bt, fw)
}

// ZREMRANGEBYSCORE key min max
func (s *Store) ZRemRangeByScore(db uint32, args [][]byte) (int64, error) {
	if len(args) != 3 {
		return 0, errArguments("len(args) = %d, expect 3", len(args))
	}

	key := args[0]
	min := args[1]
	max := args[2]

	r, err := parseRangeSpec(min, max)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if err := s.acquire(); err != nil {
		return 0, err
	}
	defer s.release()

	o, err := s.loadZSetRow(db, key, true)
	if err != nil {
		return 0, err
	} else if o == nil {
		return 0, nil
	}

	bt := engine.NewBatch()
	n := int64(0)

	f := func(o *zsetRow) error {
		bt.Del(o.DataKey())
		bt.Del(o.IndexKey())
		n++
		return nil
	}

	if err := o.travelInRange(s, r, f); err != nil {
		return 0, errors.Trace(err)
	}

	if n > 0 {
		if o.Size -= n; o.Size > 0 {
			bt.Set(o.MetaKey(), o.MetaValue())
		} else {
			bt.Del(o.MetaKey())
		}
	}

	fw := &Forward{DB: db, Op: "ZRemRangeByScore", Args: args}
	return n, s.commit(bt, fw)
}
