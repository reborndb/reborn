// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package service

import (
	"bytes"

	"github.com/BurntSushi/toml"
	"github.com/reborndb/go/bytesize"
)

type Config struct {
	Listen      string `toml:"listen_address"`
	PidFile     string `toml:"pid_file"`
	DumpPath    string `toml:"dump_filepath"`
	ConnTimeout int    `toml:"conn_timeout"`

	SyncFilePath string `toml:"sync_file_path"`
	SyncFileSize int    `toml:"sync_file_size"`
	SyncBuffSize int    `toml:"sync_memory_buffer"`

	ReplPingSlavePeriod int `toml:"repl_ping_slave_period"`
	// If empty, we will use memory for replication backlog
	ReplBacklogFilePath string `toml:"repl_backlog_file_path"`
	ReplBacklogSize     int    `toml:"repl_backlog_size"`
	// If no slaves after time, backlog will be released.
	// 0 means to no release at all.
	ReplBacklogTTL int `toml:"repl_backlog_ttl"`

	Auth       string `toml:"auth"`
	MasterAuth string `toml:"master_auth"`
}

func NewDefaultConfig() *Config {
	return &Config{
		Listen:      "0.0.0.0:6380",
		PidFile:     "./var/qdb.pid",
		DumpPath:    "dump.rdb",
		ConnTimeout: 900,

		SyncFilePath: "./var/sync.pipe",
		SyncFileSize: bytesize.GB * 32,
		SyncBuffSize: bytesize.MB * 32,

		ReplPingSlavePeriod: 10,
		ReplBacklogSize:     bytesize.GB * 10,
	}
}

func (c *Config) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}
