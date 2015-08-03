// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"os/exec"
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

type proxyArgs struct {
	Addr     string `json:"addr"`
	HTTPAddr string `json:"http_addr"`
	CPUNum   string `json:"cpu_num"`
}

// reborn-proxy -c configfile -L logfile --cpu=2 --addr=addr --id=id --http-addr=addr --dump-path=path --pidfile=file
func startProxy(args *proxyArgs) (*process, error) {
	p := newDefaultProcess("reborn-proxy", proxyType)

	if len(args.Addr) == 0 {
		return nil, errors.Errorf("proxy must have an address, not empty")
	}

	if len(args.HTTPAddr) == 0 {
		return nil, errors.Errorf("proxy must have a http address, not empty")
	}

	p.addCmdArgs("-c", configFile)
	p.addCmdArgs("-L", path.Join(p.procLogDir(), "proxy.log"))
	if len(args.CPUNum) == 0 {
		args.CPUNum = "2"
	}
	p.addCmdArgs(fmt.Sprintf("--cpu=%s", args.CPUNum))
	p.addCmdArgs(fmt.Sprintf("--addr=%s", args.Addr))
	p.addCmdArgs(fmt.Sprintf("--http-addr=%s", args.HTTPAddr))
	p.addCmdArgs(fmt.Sprintf("--dump-path=%s", p.procLogDir()))
	p.addCmdArgs(fmt.Sprintf("--pidfile=%s", p.pidPath()))
	p.addCmdArgs(fmt.Sprintf("--id=%s", p.ID))

	p.Ctx = args2Map(args)

	bindProxyProcHandler(p)

	if err := p.start(); err != nil {
		log.Errorf("start proxy err %v", err)
		return nil, errors.Trace(err)
	}

	addCheckProc(p)

	return p, nil
}

func bindProxyProcHandler(p *process) error {
	postStart := func(p *process) error {
		//we will use reborn-config to set proxy online
		//reborn-config -c config.ini proxy online proxy_1
		cmd := exec.Command("reborn-config", "-c", configFile, "-L", path.Join(p.procLogDir(), "dashboard.log"), "proxy", "online", p.ID)
		return cmd.Run()
	}

	p.postStartFunc = postStart
	return nil
}
