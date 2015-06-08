// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"path"
	"runtime"
	"strconv"

	"github.com/reborndb/reborn/pkg/proxy/router"
	"github.com/reborndb/reborn/pkg/utils"

	"github.com/docopt/docopt-go"
	log "github.com/ngaut/logging"
)

var (
	cpus       = 2
	addr       = ":9000"
	httpAddr   = ":9001"
	proxyID    = ""
	configFile = "config.ini"
	pidfile    = ""
)

var usage = `usage: reborn-proxy [options]

options:
   -c <config_file>               set config file
   -L <log_file>                  set output log file, default is stdout
   --log-level=<loglevel>         set log level: info, warn, error, debug [default: info]
   --cpu=<cpu_num>                num of cpu cores that proxy can use
   --addr=<proxy_listen_addr>     proxy listen address, example: 0.0.0.0:9000
   --id=<proxy_id>                proxy id, global unique, can not be empty 
   --http-addr=<debug_http_addr>  debug vars http server
   --dump-path=<path>             dump path to log crash error
   --pidfile=<path>               proxy pid file
   --proxy-auth=PASSWORD          proxy password
   --server-auth=PASSWORD         backend server password
`

var banner string = `
    ____       __                     ____  ____ 
   / __ \___  / /_  ____  _________  / __ \/ __ )
  / /_/ / _ \/ __ \/ __ \/ ___/ __ \/ / / / __  |
 / _, _/  __/ /_/ / /_/ / /  / / / / /_/ / /_/ / 
/_/ |_|\___/_.___/\____/_/  /_/ /_/_____/_____/  
                                                 
`

func handleSetLogLevel(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	level := r.Form.Get("level")
	log.SetLevelByString(level)
	log.Info("set log level to", level)
}

func setStringFromOpt(dest *string, args map[string]interface{}, key string) {
	if s, ok := args[key].(string); ok && len(s) != 0 {
		*dest = s
	}
}

func main() {
	fmt.Print(banner)
	log.SetLevelByString("info")

	args, err := docopt.Parse(usage, nil, true, "reborn proxy v0.1", true)
	if err != nil {
		log.Error(err)
	}

	// set config file
	if args["-c"] != nil {
		configFile = args["-c"].(string)
	}

	// set output log file
	if args["-L"] != nil {
		log.SetOutputByName(args["-L"].(string))
	}

	// set log level
	if args["--log-level"] != nil {
		log.SetLevelByString(args["--log-level"].(string))
	}

	// set cpu
	if args["--cpu"] != nil {
		cpus, err = strconv.Atoi(args["--cpu"].(string))
		if err != nil {
			log.Fatal(err)
		}
	}

	// set addr
	if args["--addr"] != nil {
		addr = args["--addr"].(string)
	}

	// set http addr
	if args["--http-addr"] != nil {
		httpAddr = args["--http-addr"].(string)
	}

	if args["--id"] != nil {
		proxyID = args["--id"].(string)
	} else {
		log.Fatalf("invalid empty proxy id")
	}

	dumppath := utils.GetExecutorPath()
	if args["--dump-path"] != nil {
		dumppath = args["--dump-path"].(string)
	}

	if args["--pidfile"] != nil {
		pidfile = args["--pidfile"].(string)
	}

	log.Info("dump file path:", dumppath)
	log.CrashLog(path.Join(dumppath, "reborn-proxy.dump"))

	router.CheckUlimit(1024)
	runtime.GOMAXPROCS(cpus)

	http.HandleFunc("/setloglevel", handleSetLogLevel)
	go http.ListenAndServe(httpAddr, nil)
	log.Info("running on ", addr)
	conf, err := router.LoadConf(configFile)
	if err != nil {
		log.Fatal(err)
	}

	conf.Addr = addr
	conf.HTTPAddr = httpAddr
	conf.ProxyID = proxyID
	conf.PidFile = pidfile

	setStringFromOpt(&conf.ProxyPassword, args, "--proxy-auth")
	setStringFromOpt(&conf.ServerPassword, args, "--server-auth")

	if err := utils.CreatePidFile(conf.PidFile); err != nil {
		log.Fatal(err)
	}

	s := router.NewServer(conf)
	s.Run()
	log.Warning("exit")
}
