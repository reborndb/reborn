// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package binlog

type Forward struct {
	DB   uint32
	Op   string
	Args []interface{}
}
