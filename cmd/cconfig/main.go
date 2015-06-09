// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/reborndb/reborn/pkg/env"
	"github.com/reborndb/reborn/pkg/utils"

	"github.com/c4pt0r/cfg"
	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
)

// global objects
var (
	globalEnv  env.Env
	globalConn zkhelper.Conn
	livingNode string
	pidFile    string
)

type Command struct {
	Run   func(cmd *Command, args []string)
	Usage string
	Short string
	Long  string
	Flag  flag.FlagSet
	Ctx   interface{}
}

var usage = `usage: reborn-config [options]

options:
   -c								set config file
   -L								set output log file, default is stdout
   --log-level=<loglevel>			set log level: info, warn, error, debug [default: info]
   --http-addr=<debug_http_addr>	debug http address
   --pidfile=<file>					program pidfile

commands:
	server
	slot
	dashboard
	action
	proxy
`

func Fatal(msg interface{}) {
	if globalConn != nil {
		globalConn.Close()
	}

	if len(pidFile) > 0 {
		os.Remove(pidFile)
	}

	// cleanup
	switch msg.(type) {
	case string:
		log.Fatal(msg)
	case error:
		log.Fatal(errors.ErrorStack(msg.(error)))
	}
}

func runCommand(cmd string, args []string) (err error) {
	argv := make([]string, 1)
	argv[0] = cmd
	argv = append(argv, args...)
	switch cmd {
	case "action":
		return errors.Trace(cmdAction(argv))
	case "dashboard":
		return errors.Trace(cmdDashboard(argv))
	case "server":
		return errors.Trace(cmdServer(argv))
	case "proxy":
		return errors.Trace(cmdProxy(argv))
	case "slot":
		return errors.Trace(cmdSlot(argv))
	}
	return errors.Errorf("%s is not a valid command. See 'reborn-config -h'", cmd)
}

func main() {
	log.SetLevelByString("info")

	args, err := docopt.Parse(usage, nil, true, "reborn config v0.1", true)
	if err != nil {
		log.Error(err)
	}

	if v := args["--pidfile"]; v != nil {
		pidFile = v.(string)
	}

	// set config file
	var configFile string
	var config *cfg.Cfg
	if args["-c"] != nil {
		configFile = args["-c"].(string)
		config, err = utils.InitConfigFromFile(configFile)
		if err != nil {
			Fatal(err)
		}
	} else {
		config, err = utils.InitConfig()
		if err != nil {
			Fatal(err)
		}
	}

	// load global vars
	globalEnv = env.LoadRebornEnv(config)
	globalConn = CreateCoordConn()

	// set output log file
	if args["-L"] != nil {
		log.SetOutputByName(args["-L"].(string))
	}

	// set log level
	if args["--log-level"] != nil {
		log.SetLevelByString(args["--log-level"].(string))
	}

	cmd := args["<command>"].(string)
	cmdArgs := args["<args>"].([]string)

	debugHTTP := ":10086"
	if v := args["--http-addr"]; v != nil {
		debugHTTP = v.(string)
	}
	//debug var address
	go http.ListenAndServe(debugHTTP, nil)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill)
	go func() {
		<-c

		Fatal("ctrl-c or SIGTERM found, exit")
	}()

	utils.CreatePidFile(pidFile)

	err = runCommand(cmd, cmdArgs)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
