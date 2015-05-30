// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"os"
	"os/exec"
	"syscall"
)

func main() {
	// create a new session to prevent receiving signals from parent
	syscall.Setsid()

	cmd := os.Args[1]
	args := os.Args[2:]
	c := exec.Command(cmd, args...)
	c.Start()
	os.Exit(0)
}
