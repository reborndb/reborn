// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/juju/errors"
)

func Num64(i interface{}) interface{} {
	switch x := i.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return int64(x)
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	case uint64:
		return uint64(x)
	case float32:
		return float64(x)
	case float64:
		return float64(x)
	default:
		return x
	}
}

func ParseFloat(i interface{}) (float64, error) {
	var s string
	switch x := Num64(i).(type) {
	case int64:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case float64:
		switch {
		case math.IsNaN(x):
			return 0, errors.New("float is NaN")
		}
		return float64(x), nil
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprint(x)
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, errors.Trace(err)
}

func ParseUint(i interface{}) (uint64, error) {
	var s string
	switch x := Num64(i).(type) {
	case int64:
		if x < 0 {
			return 0, errors.New("integer overflow")
		}
		return uint64(x), nil
	case uint64:
		return uint64(x), nil
	case float64:
		switch {
		case math.IsNaN(x):
			return 0, errors.New("float is NaN")
		case math.IsInf(x, 0):
			return 0, errors.New("float is Inf")
		case math.Abs(x-float64(uint64(x))) > 1e-9:
			return 0, errors.New("float to uint64")
		}
		return uint64(x), nil
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprint(x)
	}
	u, err := strconv.ParseUint(s, 10, 64)
	return u, errors.Trace(err)
}

func ParseInt(i interface{}) (int64, error) {
	var s string
	switch x := Num64(i).(type) {
	case int64:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, errors.New("integer overflow")
		}
		return int64(x), nil
	case float64:
		switch {
		case math.IsNaN(x):
			return 0, errors.New("float is NaN")
		case math.IsInf(x, 0):
			return 0, errors.New("float is Inf")
		case math.Abs(x-float64(int64(x))) > 1e-9:
			return 0, errors.New("float to int64")
		}
		return int64(x), nil
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprint(x)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, errors.Trace(err)
}

func FormatString(i interface{}) string {
	switch y := i.(type) {
	case []byte:
		return string(y)
	case string:
		return y
	default:
		return fmt.Sprintf("%v", y)
	}
}

func FormatFloat(v float64) []byte {
	return []byte(FormatFloatString(v))
}

func FormatFloatString(v float64) string {
	s := strconv.FormatFloat(v, 'f', 17, 64)
	// redis use inf and -inf for float bulk string returns
	switch s {
	case "+Inf":
		return "inf"
	case "-Inf":
		return "-inf"
	default:
		return s
	}
}

func FormatUint(u uint64) []byte {
	return []byte(strconv.FormatUint(u, 10))
}

func FormatInt(v int64) []byte {
	return []byte(strconv.FormatInt(v, 10))
}

func parseArgument(arg interface{}, ref interface{}) error {
	switch x := ref.(type) {
	default:
		return errors.Errorf("unsupported type, %v", reflect.TypeOf(x))
	case *int64:
		v, err := ParseInt(arg)
		if err != nil {
			return errors.Errorf("expect %v, %s", reflect.TypeOf(*x), err.Error())
		}
		*x = v
	case *uint32:
		v, err := ParseUint(arg)
		if err != nil {
			return errors.Errorf("expect %v, %s", reflect.TypeOf(*x), err.Error())
		} else if v > math.MaxUint32 {
			return errors.Errorf("expect %v, but got %d", reflect.TypeOf(*x), v)
		}
		*x = uint32(v)
	case *uint64:
		v, err := ParseUint(arg)
		if err != nil {
			return errors.Errorf("expect %v, %s", reflect.TypeOf(*x), err.Error())
		}
		*x = v
	case *float64:
		v, err := ParseFloat(arg)
		if err != nil {
			return errors.Errorf("expect %v, %s", reflect.TypeOf(*x), err.Error())
		}
		*x = v
	case *[]byte:
		switch y := arg.(type) {
		case []byte:
			*x = y
		case string:
			*x = []byte(y)
		default:
			return errors.Errorf("expect %v, but got %v", reflect.TypeOf(*x), reflect.TypeOf(y))
		}
	case *string:
		switch y := arg.(type) {
		case []byte:
			*x = string(y)
		case string:
			*x = y
		default:
			return errors.Errorf("expect %v, but got %v", reflect.TypeOf(*x), reflect.TypeOf(y))
		}
	}
	return nil
}

func FormatByte(arg interface{}) []byte {
	switch x := arg.(type) {
	default:
		return []byte{}
	case int64:
		return FormatInt(int64(x))
	case int32:
		return FormatInt(int64(x))
	case int:
		return FormatInt(int64(x))
	case uint64:
		return FormatUint(uint64(x))
	case uint32:
		return FormatUint(uint64(x))
	case uint:
		return FormatUint(uint64(x))
	case float64:
		return FormatFloat(float64(x))
	case float32:
		return FormatFloat(float64(x))
	case []byte:
		return x
	case string:
		return []byte(x)
	}
}

func FormatBytes(args ...interface{}) [][]byte {
	values := make([][]byte, len(args))
	for i, arg := range args {
		values[i] = FormatByte(arg)
	}

	return values
}
