// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package gocheck2

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type CheckersSuite struct{}

var _ = Suite(&CheckersSuite{})

func (s *CheckersSuite) SetUpTest(c *C) {
}

func testHasKey(c *C, expectedResult bool, expectedErr string, params ...interface{}) {
	actualResult, actualErr := HasKey.Check(params, nil)
	if actualResult != expectedResult || actualErr != expectedErr {
		c.Fatalf(
			"Check returned (%#v, %#v) rather than (%#v, %#v)",
			actualResult, actualErr, expectedResult, expectedErr)
	}
}

func (s *CheckersSuite) TestHasKey(c *C) {
	testHasKey(c, true, "", map[string]int{"foo": 1}, "foo")
	testHasKey(c, false, "", map[string]int{"foo": 1}, "bar")
	testHasKey(c, true, "", map[int][]byte{10: nil}, 10)

	testHasKey(c, false, "First argument to HasKey must be a map", nil, "bar")
	testHasKey(
		c, false, "Second argument must be assignable to the map key type",
		map[string]int{"foo": 1}, 10)
}

func (s *CheckersSuite) TestCompare(c *C) {
	c.Assert(10, Less, 11)
	c.Assert(10, LessEqual, 10)
	c.Assert(10, Greater, 9)
	c.Assert(10, GreaterEqual, 10)
	c.Assert(10, Not(LessEqual), 9)
	c.Assert(10, Not(Less), 9)
	c.Assert("ABC", Less, "ABCD")
	c.Assert([]byte("ABC"), Less, []byte("ABCD"))
	c.Assert(3.14, Less, 3.145)
}
