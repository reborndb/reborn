// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"fmt"
	"os"
	"path"

	"github.com/kardianos/osext"
	log "github.com/ngaut/logging"

	"github.com/ngaut/zkhelper"

	"github.com/c4pt0r/cfg"
)

func InitConfig() (*cfg.Cfg, error) {
	configFile := os.Getenv("REBORN_CONF")
	if len(configFile) == 0 {
		configFile = "config.ini"
	}
	ret := cfg.NewCfg(configFile)
	if err := ret.Load(); err != nil {
		return nil, err
	}
	return ret, nil
}

func InitConfigFromFile(filename string) (*cfg.Cfg, error) {
	ret := cfg.NewCfg(filename)
	if err := ret.Load(); err != nil {
		return nil, err
	}
	return ret, nil
}

func GetCoordLock(coordConn zkhelper.Conn, productName string) zkhelper.ZLocker {
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/LOCK", productName)
	ret := zkhelper.CreateMutex(coordConn, coordPath)
	return ret
}

func GetExecutorPath() string {
	// we cannot rely on os.Args[0], it may be faked sometimes
	execPath, err := osext.ExecutableFolder()
	if err != nil {
		log.Fatal(err)
	}

	return execPath
}

type Strings []string

func (s1 Strings) Eq(s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func CreatePidFile(name string) error {
	if len(name) == 0 {
		return nil
	}

	os.MkdirAll(path.Dir(name), 0755)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.WriteString(fmt.Sprintf("%d", os.Getpid())); err != nil {
		return err
	}
	return nil
}
