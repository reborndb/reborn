// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package resp

import (
	"strings"

	"github.com/juju/errors"
)

func ParseArgs(resp Resp) (cmd string, args [][]byte, err error) {
	var array []Resp
	if o, ok := resp.(*Array); !ok {
		return "", nil, errors.Errorf("expect array, but got type = '%s'", resp.Type())
	} else if o == nil || len(o.Value) == 0 {
		return "", nil, errors.New("request is an empty array")
	} else {
		array = o.Value
	}
	slices := make([][]byte, 0, len(array))
	for i, resp := range array {
		if o, ok := resp.(*BulkBytes); !ok {
			return "", nil, errors.Errorf("args[%d], expect bulkbytes, but got '%s'", i, resp.Type())
		} else if i == 0 && len(o.Value) == 0 {
			return "", nil, errors.New("command is empty")
		} else {
			slices = append(slices, o.Value)
		}
	}
	return strings.ToLower(string(slices[0])), slices[1:], nil
}
