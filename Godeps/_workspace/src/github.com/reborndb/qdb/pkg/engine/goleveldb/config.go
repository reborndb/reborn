// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package goleveldb

import "github.com/reborndb/go/bytesize"

type Config struct {
	BlockSize       int `toml:"block_size"`
	CacheSize       int `toml:"cache_size"`
	WriteBufferSize int `toml:"write_buffer_size"`
	BloomFilterSize int `toml:"bloom_filter_size"`
	MaxOpenFiles    int `toml:"max_open_files"`
}

func NewDefaultConfig() *Config {
	return &Config{
		BlockSize:       bytesize.KB * 4,
		CacheSize:       bytesize.MB * 4,
		WriteBufferSize: bytesize.MB * 4,
		BloomFilterSize: 10,
		MaxOpenFiles:    500,
	}
}
