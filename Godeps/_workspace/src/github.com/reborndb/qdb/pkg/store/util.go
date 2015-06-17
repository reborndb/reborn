// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package store

import "math"

// We can not use lexicographically bytes comparison for negative and positive float directly.
// so here we will do a trick below.
func float64ToUint64(f float64) uint64 {
	u := math.Float64bits(f)
	if f >= 0 {
		u |= 0x8000000000000000
	} else {
		u = ^u
	}
	return u
}

func uint64ToFloat64(u uint64) float64 {
	if u&0x8000000000000000 > 0 {
		u &= ^uint64(0x8000000000000000)
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}
