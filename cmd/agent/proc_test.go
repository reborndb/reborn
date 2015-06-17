// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"os"
	"path"

	. "gopkg.in/check.v1"
)

func (s *testAgentSuite) TestDir(c *C) {
	dir := "./var/proc_test"
	file := path.Join(dir, "1")

	var err error
	err = os.MkdirAll(dir, 0755)
	c.Assert(err, IsNil)

	c.Assert(isFileExist(file), Equals, false)

	f, err := os.Create(file)
	c.Assert(f, NotNil)
	c.Assert(err, IsNil)

	c.Assert(isFileExist(file), Equals, true)
	c.Assert(isDirExist(file), Equals, false)

	c.Assert(isFileExist(dir), Equals, false)
	c.Assert(isDirExist(dir), Equals, true)

	newDir := dir + "_new"
	err = os.RemoveAll(newDir)
	c.Assert(err, IsNil)

	err = os.Rename(dir, newDir)
	c.Assert(err, IsNil)

	c.Assert(isFileExist(dir), Equals, false)
	c.Assert(isDirExist(dir), Equals, false)

	err = os.MkdirAll(dir, 0755)
	c.Assert(err, IsNil)

	c.Assert(isDirExist(dir), Equals, true)
	err = os.Rename(dir, newDir)
	c.Assert(err, NotNil)

	c.Assert(isDirExist(dir), Equals, true)

	err = os.RemoveAll(dir)
	c.Assert(err, IsNil)

	c.Assert(isDirExist(dir), Equals, false)

	err = os.RemoveAll(dir)
	c.Assert(err, IsNil)

	c.Assert(isDirExist(dir), Equals, false)

	err = os.RemoveAll(newDir)
	c.Assert(err, IsNil)

	err = os.RemoveAll(file)
	c.Assert(err, IsNil)
	c.Assert(isFileExist(file), Equals, false)

	err = os.Rename(newDir, dir)
	c.Assert(err, NotNil)
	c.Assert(os.IsNotExist(err), Equals, true)

	err = os.Rename(dir, newDir)
	c.Assert(err, NotNil)
	c.Assert(os.IsNotExist(err), Equals, true)
}
