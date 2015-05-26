// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package assert

import (
	"fmt"
	"testing"

	"github.com/reborndb/reborn/extern/redis-port/pkg/libs/errors"
	"github.com/reborndb/reborn/extern/redis-port/pkg/libs/trace"
)

func Fatalf(t *testing.T, format string, args ...interface{}) {
	t.Fatalf("%s\n%s", fmt.Sprintf(format, args...), trace.TraceN(1, 32))
}

func Must(t *testing.T, b bool) {
	if b {
		return
	}
	t.Fatalf("assertion failed\n%s", trace.TraceN(1, 32))
}

func ErrorIsNil(t *testing.T, err error) {
	if err == nil {
		return
	}
	stack := errors.ErrorStack(err)
	if stack == nil {
		stack = trace.TraceN(1, 32)
	}
	t.Fatalf("%s\n%s", err, stack)
}
