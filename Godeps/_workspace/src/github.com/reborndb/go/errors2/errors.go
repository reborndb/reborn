// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package errors2

import (
	"github.com/juju/errors"
)

func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}

	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	return e1.Error() == e2.Error()
}

func ErrorNotEqual(err1, err2 error) bool {
	return !ErrorEqual(err1, err2)
}
