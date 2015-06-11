// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/utils"

	"github.com/codegangsta/martini-contrib/binding"
	"github.com/codegangsta/martini-contrib/render"
	"github.com/docopt/docopt-go"
	"github.com/go-martini/martini"
	"github.com/juju/errors"
	"github.com/martini-contrib/cors"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
)

func cmdDashboard(argv []string) (err error) {
	usage := `usage: reborn-config dashboard [--addr=<address>] [--http-log=<log_file>]

options:
	--addr	listen ip:port, e.g. localhost:12345, :8086, [default: :8086]
	--http-log	http request log [default: request.log ]
`

	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Debug(args)

	logFileName := "request.log"
	if args["--http-log"] != nil {
		logFileName = args["--http-log"].(string)
	}

	addr := ":8086"
	if args["--addr"] != nil {
		addr = args["--addr"].(string)
	}

	runDashboard(addr, logFileName)
	return nil
}

var proxiesSpeed int64

func CreateCoordConn() zkhelper.Conn {
	conn, err := globalEnv.NewCoordConn()
	if err != nil {
		Fatal("Failed to create coordinator connection: " + err.Error())
	}
	return conn
}

func jsonRet(output map[string]interface{}) (int, string) {
	b, err := json.Marshal(output)
	if err != nil {
		log.Warning(err)
	}
	return 200, string(b)
}

func jsonRetFail(errCode int, msg string) (int, string) {
	return jsonRet(map[string]interface{}{
		"ret": errCode,
		"msg": msg,
	})
}

func jsonRetSucc() (int, string) {
	return jsonRet(map[string]interface{}{
		"ret": 0,
		"msg": "OK",
	})
}

func getAllProxyOps() int64 {
	conn := CreateCoordConn()
	defer conn.Close()
	proxies, err := models.ProxyList(conn, globalEnv.ProductName(), nil)
	if err != nil {
		log.Warning(err)
		return -1
	}

	var total int64
	for _, p := range proxies {
		i, err := p.Ops()
		if err != nil {
			log.Warning(err)
		}
		total += i
	}
	return total
}

// for debug
func getAllProxyDebugVars() map[string]map[string]interface{} {
	conn := CreateCoordConn()
	defer conn.Close()
	proxies, err := models.ProxyList(conn, globalEnv.ProductName(), nil)
	if err != nil {
		log.Warning(err)
		return nil
	}

	ret := make(map[string]map[string]interface{})
	for _, p := range proxies {
		m, err := p.DebugVars()
		if err != nil {
			log.Warning(err)
		}
		ret[p.ID] = m
	}
	return ret
}

func getProxySpeedChan() <-chan int64 {
	c := make(chan int64)
	go func() {
		var lastCnt int64
		for {
			cnt := getAllProxyOps()
			if lastCnt > 0 {
				c <- cnt - lastCnt
			}
			lastCnt = cnt
			time.Sleep(1 * time.Second)
		}
	}()
	return c
}

func pageSlots(r render.Render) {
	r.HTML(200, "slots", nil)
}

func createDashboardNode(conn zkhelper.Conn) error {
	// make sure root dir is exists
	rootDir := fmt.Sprintf("/zk/reborn/db_%s", globalEnv.ProductName())
	zkhelper.CreateRecursive(conn, rootDir, "", 0, zkhelper.DefaultDirACLs())

	coordPath := fmt.Sprintf("%s/dashboard", rootDir)
	// make sure we're the only one dashboard
	timeoutCh := time.After(60 * time.Second)

	for {
		if exists, _, ch, _ := conn.ExistsW(coordPath); exists {
			data, _, _ := conn.Get(coordPath)

			if checkDashboardAlive(data) {
				return errors.Errorf("dashboard already exists: %s", string(data))
			} else {
				log.Warningf("dashboard %s exists in zk, wait it removed", data)

				select {
				case <-ch:
				case <-timeoutCh:
					return errors.Errorf("wait existed dashboard %s removed timeout", string(data))
				}
			}
		} else {
			break
		}
	}

	content := fmt.Sprintf(`{"addr": "%v", "pid": %v}`, globalEnv.DashboardAddr(), os.Getpid())
	pathCreated, err := conn.Create(coordPath, []byte(content),
		zk.FlagEphemeral, zkhelper.DefaultFileACLs())

	log.Infof("dashboard node %s created, data %s, err %v", pathCreated, string(content), err)

	return errors.Trace(err)
}

func checkDashboardAlive(data []byte) bool {
	var v struct {
		Addr string `json:"addr"`
		Pid  int    `json:"pid"`
	}
	err := json.Unmarshal(data, &v)
	if err != nil {
		log.Errorf("invalid dashboard data %s, json unmarshal err: %v, force remove", data, err)
		return false
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/ping", v.Addr))
	if err != nil {
		log.Errorf("ping dashboard %s failed, err %v", v.Addr, err)
		return false
	}

	// we ping dashboard ok, it may be alive
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	return true
}

func releaseDashboardNode(conn zkhelper.Conn) {
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/dashboard", globalEnv.ProductName())

	if exists, _, _ := conn.Exists(coordPath); exists {
		log.Info("removing dashboard node")
		conn.Delete(coordPath, 0)
	}
}

func runDashboard(addr string, httpLogFile string) {
	log.Info("dashboard listening on addr: ", addr)
	m := martini.Classic()
	f, err := os.OpenFile(httpLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		Fatal(err)
	}
	defer f.Close()

	m.Map(stdlog.New(f, "[martini]", stdlog.LstdFlags))

	binRoot := utils.GetExecutorPath()

	m.Use(martini.Static(filepath.Join(binRoot, "assets/statics")))
	m.Use(render.Renderer(render.Options{
		Directory:  filepath.Join(binRoot, "assets/template"),
		Extensions: []string{".tmpl", ".html"},
		Charset:    "UTF-8",
		IndentJSON: true,
	}))

	m.Use(cors.Allow(&cors.Options{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"POST", "GET", "DELETE", "PUT"},
		AllowHeaders:     []string{"Origin", "x-requested-with", "Content-Type", "Content-Range", "Content-Disposition", "Content-Description"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
	}))

	m.Get("/api/server_groups", apiGetServerGroupList)
	m.Get("/api/overview", apiOverview)

	m.Get("/api/redis/:addr/stat", apiRedisStat)
	m.Get("/api/redis/:addr/:id/slotinfo", apiGetRedisSlotInfo)
	m.Get("/api/redis/group/:group_id/:slot_id/slotinfo", apiGetRedisSlotInfoFromGroupId)

	m.Put("/api/server_groups", binding.Json(models.ServerGroup{}), apiAddServerGroup)
	m.Put("/api/server_group/(?P<id>[0-9]+)/addServer", binding.Json(models.Server{}), apiAddServerToGroup)
	m.Delete("/api/server_group/(?P<id>[0-9]+)", apiRemoveServerGroup)

	m.Put("/api/server_group/(?P<id>[0-9]+)/removeServer", binding.Json(models.Server{}), apiRemoveServerFromGroup)
	m.Get("/api/server_group/(?P<id>[0-9]+)", apiGetServerGroup)
	m.Post("/api/server_group/(?P<id>[0-9]+)/promote", binding.Json(models.Server{}), apiPromoteServer)

	m.Get("/api/migrate/status", apiMigrateStatus)
	m.Get("/api/migrate/tasks", apiGetMigrateTasks)
	m.Delete("/api/migrate/pending_task/:id/remove", apiRemovePendingMigrateTask)
	m.Delete("/api/migrate/task/:id/stop", apiStopMigratingTask)
	m.Post("/api/migrate", binding.Json(MigrateTaskInfo{}), apiDoMigrate)

	m.Post("/api/rebalance", apiRebalance)
	m.Get("/api/rebalance/status", apiRebalanceStatus)

	m.Get("/api/slot/list", apiGetSlots)
	m.Get("/api/slot/:id", apiGetSingleSlot)
	m.Post("/api/slots/init", apiInitSlots)
	m.Get("/api/slots", apiGetSlots)
	m.Post("/api/slot", binding.Json(RangeSetTask{}), apiSlotRangeSet)
	m.Get("/api/proxy/list", apiGetProxyList)
	m.Get("/api/proxy/debug/vars", apiGetProxyDebugVars)
	m.Post("/api/proxy", binding.Json(models.ProxyInfo{}), apiSetProxyStatus)

	m.Get("/api/action/gc", apiActionGC)
	m.Get("/api/force_remove_locks", apiForceRemoveLocks)
	m.Get("/api/remove_fence", apiRemoveFence)

	m.Get("/slots", pageSlots)
	m.Get("/ping", func() int { return 200 })
	m.Get("/", func(r render.Render) {
		r.Redirect("/admin")
	})

	// create temp node in coordinator
	if err := createDashboardNode(globalConn); err != nil {
		Fatal(err)
	}
	defer releaseDashboardNode(globalConn)

	// create long live migrate manager
	globalMigrateManager = NewMigrateManager(globalConn, globalEnv.ProductName(), preMigrateCheck)
	// defer globalMigrateManager.removeNode()

	go func() {
		c := getProxySpeedChan()
		for {
			atomic.StoreInt64(&proxiesSpeed, <-c)
		}
	}()

	m.RunOnAddr(addr)
}
