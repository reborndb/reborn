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

	"github.com/docopt/docopt-go"
	"github.com/ngaut/log"
	"github.com/reborndb/reborn/pkg/proxy/router"
	"github.com/reborndb/reborn/pkg/utils"
)

var (
	cpus       = 2
	addr       = ":9000"
	httpAddr   = ":9001"
	proxyID    = ""
	configFile = "config.ini"
	pidfile    = ""
	netTimeout = 5
	proto      = "tcp"
	proxyAuth  = ""
)

var usage = `usage: reborn-proxy [options]

options:
   -c <config_file>               set config file
   -L <log_file>                  set output log file, default is stdout
   --addr=<proxy_listen_addr>     proxy listen address, example: 0.0.0.0:9000
   --cpu=<cpu_num>                num of cpu cores that proxy can use
   --dump-path=<path>             dump path to log crash error
   --http-addr=<debug_http_addr>  debug vars http server
   --id=<proxy_id>                proxy id, global unique, can not be empty
   --log-level=<loglevel>         set log level: info, warn, error, debug [default: info]
   --net-timeout=<timeout>        connection timeout
   --pidfile=<path>               proxy pid file
   --proto=<listen_proto>         proxy listen address proto, like tcp
   --proxy-auth=PASSWORD          proxy auth
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

func setIntArgFromOpt(dest *int, args map[string]interface{}, key string) {
	if s, ok := args[key].(string); ok && len(s) != 0 {
		n, err := strconv.Atoi(s)
		if err != nil {
			log.Fatalf("parse int arg err %v", err)
			return
		}

		*dest = n
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
	setStringFromOpt(&configFile, args, "-c")

	// set output log file
	if v := args["-L"]; v != nil {
		log.SetHighlighting(false)
		log.SetOutputByName(v.(string))
	}

	// set log level
	if v := args["--log-level"]; v != nil {
		log.SetLevelByString(v.(string))
	}

	// set cpu
	setIntArgFromOpt(&cpus, args, "--cpu")

	// set addr
	setStringFromOpt(&addr, args, "--addr")

	// set proto
	setStringFromOpt(&proto, args, "--proto")

	// set http addr
	setStringFromOpt(&httpAddr, args, "--http-addr")

	// set proxy id
	setStringFromOpt(&proxyID, args, "--id")
	if len(proxyID) == 0 {
		log.Fatalf("invalid empty proxy id")
	}

	// set log dump path
	dumppath := utils.GetExecutorPath()
	setStringFromOpt(&dumppath, args, "--dump-path")

	log.Info("dump file path:", dumppath)
	log.CrashLog(path.Join(dumppath, "reborn-proxy.dump"))

	// set pidfile
	setStringFromOpt(&pidfile, args, "--pidfile")

	// set proxy auth
	setStringFromOpt(&proxyAuth, args, "--proxy-auth")

	// set net time
	setIntArgFromOpt(&netTimeout, args, "--net-timeout")

	router.CheckUlimit(1024)
	runtime.GOMAXPROCS(cpus)

	http.HandleFunc("/setloglevel", handleSetLogLevel)
	go http.ListenAndServe(httpAddr, nil)

	conf, err := router.LoadConf(configFile)
	if err != nil {
		log.Fatal(err)
	}

	conf.Addr = addr
	conf.HTTPAddr = httpAddr
	conf.ProxyID = proxyID
	conf.PidFile = pidfile
	conf.NetTimeout = netTimeout
	conf.Proto = proto
	conf.ProxyAuth = proxyAuth

	if err := utils.CreatePidFile(conf.PidFile); err != nil {
		log.Fatal(err)
	}

	log.Info("running on ", addr)

	s := router.NewServer(conf)
	s.Run()
	log.Warning("exit")
}
