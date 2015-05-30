// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/reborndb/go/log"
)

type dashboardArgs struct {
	Addr string `json:"addr"`
}

// reborn-config -c config.ini -L dashboard.log dashboard --addr=:18087 --http-log=requests.log --pidfile=file
func startDashboard(args *dashboardArgs) (*process, error) {
	p := newDefaultProcess("reborn-config", dashboardType)

	if len(args.Addr) == 0 {
		args.Addr = ":18087"
	}

	p.Ctx = args2Map(args)

	p.addCmdArgs("-c", configFile)
	p.addCmdArgs("-L", path.Join(p.baseLogDir(), "dashboard.log"))
	p.addCmdArgs(fmt.Sprintf("--pidfile=%s", p.pidPath()))

	p.addCmdArgs("dashboard")
	// below is for dashboard options

	p.addCmdArgs(fmt.Sprintf("--http-log=%s", path.Join(p.baseLogDir(), "dashboard.log")))
	p.addCmdArgs(fmt.Sprintf("--addr=%s", args.Addr))

	bindDashboardProcHandler(p)

	if err := p.start(); err != nil {
		log.Errorf("start dashboard err %v", err)
		return nil, err
	}

	addCheckProc(p)

	return p, nil
}

func bindDashboardProcHandler(p *process) error {
	postStart := func(p *process) error {
		resp, err := http.Get(fmt.Sprintf("http://%s/ping", p.Ctx["addr"]))
		if err != nil {
			return err
		}

		defer resp.Body.Close()
		_, err = ioutil.ReadAll(resp.Body)

		return err
	}

	p.postStartFunc = postStart

	return nil
}
