// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package engine

import (
	"fmt"
	"strings"
	"sync"
)

type Driver interface {
	Open(path string, conf interface{}, repair bool) (Database, error)
}

var drivers map[string]Driver
var driverLock sync.Mutex

func getDriver(name string) (Driver, error) {
	driverLock.Lock()
	defer driverLock.Unlock()

	name = strings.ToLower(name)

	d, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("%s is not registered", name)
	}

	return d, nil
}

// Opens a database specified by its database driver name.
func Open(name string, path string, conf interface{}, repair bool) (Database, error) {
	d, err := getDriver(name)
	if err != nil {
		return nil, err
	}

	return d.Open(path, conf, repair)
}

// Returns current registered drivers
func Drivers() []string {
	driverLock.Lock()
	defer driverLock.Unlock()

	ds := make([]string, 0, len(drivers))
	for name, _ := range drivers {
		ds = append(ds, name)
	}

	return ds
}

// Registers a database driver by the specified driver name.
func Register(name string, driver Driver) {
	driverLock.Lock()
	defer driverLock.Unlock()

	name = strings.ToLower(name)

	if driver == nil {
		panic(fmt.Sprintf("Empty driver for %s", name))
	}

	if _, ok := drivers[name]; ok {
		panic(fmt.Sprintf("%s has been already registered", name))
	}

	drivers[name] = driver
}

func init() {
	drivers = make(map[string]Driver)
}
