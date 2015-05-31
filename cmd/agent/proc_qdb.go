// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"path"

	log "github.com/ngaut/logging"
)

type qdbArgs struct {
	Addr   string `json:"addr"`
	DBType string `json:"dbtype"`
	CPUNum string `json:"cpu_num"`
}

// qdb-server
func startQDB(args *qdbArgs) (*process, error) {
	p := newDefaultProcess("qdb-server", qdbType)

	if len(args.Addr) == 0 {
		return nil, fmt.Errorf("qdb must have an address, not empty")
	}

	if len(args.CPUNum) == 0 {
		args.CPUNum = "2"
	}

	p.Ctx["addr"] = args.Addr

	if len(qdbConfigFile) > 0 {
		p.addCmdArgs(fmt.Sprintf("--config=%s", qdbConfigFile))
	}

	p.addCmdArgs("-L", path.Join(p.baseLogDir(), "qdb.log"))
	p.addCmdArgs(fmt.Sprintf("--ncpu=%s", args.CPUNum))
	p.addCmdArgs(fmt.Sprintf("--dbtype=%s", args.DBType))
	p.addCmdArgs(fmt.Sprintf("--dbpath=%s", path.Join(p.baseDataDir(), "db")))
	p.addCmdArgs(fmt.Sprintf("--addr=%s", args.Addr))
	p.addCmdArgs(fmt.Sprintf("--pidfile=%s", p.pidPath()))
	p.addCmdArgs(fmt.Sprintf("--dump_path=%s", path.Join(p.baseDataDir(), "dump.rdb")))
	p.addCmdArgs(fmt.Sprintf("--sync_file_path=%s", path.Join(p.baseDataDir(), "sync.pipe")))
	p.addCmdArgs(fmt.Sprintf("--repl_backlog_file_path=%s", path.Join(p.baseDataDir(), "repl_backlog")))

	// below we use fixed config, later maybe passed from args
	p.addCmdArgs(fmt.Sprintf("--conn_timeout=900"))
	p.addCmdArgs(fmt.Sprintf("--sync_file_size=34359738368"))
	p.addCmdArgs(fmt.Sprintf("--sync_buff_size=8388608"))
	p.addCmdArgs(fmt.Sprintf("--repl_backlog_size=10737418240"))

	bindRedisProcHandler(p)

	if err := p.start(); err != nil {
		log.Errorf("start redis err %v", err)
		return nil, err
	}

	addCheckProc(p)

	return p, nil
}
