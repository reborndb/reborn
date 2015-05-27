// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package env

import (
	"os"

	"github.com/BurntSushi/toml"
	errors "github.com/juju/errors"
)

type Config struct {
	Product         string `toml:"product"`
	Coordinator     string `toml:"coordinator"`
	CoordinatorAddr string `toml:"coordinator_addr"`
	DashboardAddr   string `toml:"dashboard_addr"`
}

func NewConfigWithFile(name string) (*Config, error) {
	var c Config
	if _, err := toml.DecodeFile(name, &c); err != nil {
		return nil, errors.Trace(err)
	}

	setDefaultStringIfEmpty(&c.Product, "test")
	setDefaultStringIfEmpty(&c.Coordinator, "zookeeper")
	setDefaultStringIfEmpty(&c.CoordinatorAddr, "localhost:2181")

	hostname, _ := os.Hostname()
	setDefaultStringIfEmpty(&c.DashboardAddr, hostname+":18087")

	return &c, nil
}

func setDefaultStringIfEmpty(dest *string, defaultValue string) {
	if len(*dest) == 0 {
		*dest = defaultValue
	}
}
