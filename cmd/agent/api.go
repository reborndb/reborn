// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/reborndb/reborn/pkg/utils"
)

func runHTTPServer() {
	m := mux.NewRouter()

	m.HandleFunc("/api/start", apiStartProc).Methods("POST", "PUT")
	m.HandleFunc("/api/start_redis", apiStartRedisProc).Methods("POST", "PUT")
	m.HandleFunc("/api/start_qdb", apiStartQDBProc).Methods("POST", "PUT")
	m.HandleFunc("/api/start_proxy", apiStartProxyProc).Methods("POST", "PUT")
	m.HandleFunc("/api/start_dashboard", apiStartDashboardProc).Methods("POST", "PUT")
	m.HandleFunc("/api/stop", apiStopProc).Methods("DELETE", "POST", "PUT")
	m.HandleFunc("/api/procs", apiListProcs)
	m.HandleFunc("/api/check_store", apiCheckStore)

	http.Handle("/", m)
	http.ListenAndServe(addr, nil)
}

func respError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	if len(msg) > 0 {
		w.Write([]byte(msg))
	} else {
		w.Write([]byte(http.StatusText(code)))
	}
}

func respJson(w http.ResponseWriter, v interface{}) {
	w.WriteHeader(http.StatusOK)

	data, _ := json.Marshal(v)
	w.Write(data)
}

func apiStartProc(w http.ResponseWriter, r *http.Request) {
	tp := strings.ToLower(r.FormValue("type"))
	switch tp {
	case qdbType:
		apiStartQDBProc(w, r)
	case dashboardType:
		apiStartDashboardProc(w, r)
	case proxyType:
		apiStartProxyProc(w, r)
	case redisType:
		apiStartRedisProc(w, r)
	default:
		respError(w, http.StatusBadRequest, fmt.Sprintf("invalid proc type %s", tp))
		return
	}
}

func apiStopProc(w http.ResponseWriter, r *http.Request) {
	id := strings.ToLower(r.FormValue("id"))
	err := stopCheckProc(id)
	if err != nil {
		respError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

// /start_redis?addr=addr
func apiStartRedisProc(w http.ResponseWriter, r *http.Request) {
	addr := r.FormValue("addr")
	if len(addr) == 0 {
		respError(w, http.StatusBadRequest, "must have an address for redis, not empty")
		return
	}

	args := new(redisArgs)
	args.Addr = addr

	p, err := startRedis(args)
	if err != nil {
		respError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respJson(w, p)
}

// /start_qdb?addr=addr&dbtype=rocksdb&cpu_num=2
func apiStartQDBProc(w http.ResponseWriter, r *http.Request) {
	addr := r.FormValue("addr")
	dbType := r.FormValue("dbtype")
	cpuNum := r.FormValue("cpu_num")

	if len(addr) == 0 {
		respError(w, http.StatusBadRequest, "must have an address for qdb, not empty")
		return
	}

	if len(dbType) == 0 {
		dbType = "rocksdb"
	}

	args := new(qdbArgs)
	args.Addr = addr
	args.DBType = dbType
	args.CPUNum = cpuNum

	p, err := startQDB(args)
	if err != nil {
		respError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respJson(w, p)
}

// /start_proxy?addr=addr&http_addr=addr&cpu_num=2
func apiStartProxyProc(w http.ResponseWriter, r *http.Request) {
	addr := r.FormValue("addr")
	httpAddr := r.FormValue("http_addr")
	cpuNum := r.FormValue("cpu_num")

	if len(addr) == 0 {
		respError(w, http.StatusBadRequest, "must have an address for proxy, not empty")
		return
	}

	if len(httpAddr) == 0 {
		respError(w, http.StatusBadRequest, "must have a http address for proxy, not empty")
		return
	}

	args := new(proxyArgs)
	args.Addr = addr
	args.HTTPAddr = httpAddr
	args.CPUNum = cpuNum

	p, err := startProxy(args)
	if err != nil {
		respError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respJson(w, p)
}

// /start_dashboard?addr=addr
func apiStartDashboardProc(w http.ResponseWriter, r *http.Request) {
	addr := r.FormValue("addr")

	args := new(dashboardArgs)
	args.Addr = addr

	p, err := startDashboard(args)
	if err != nil {
		respError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respJson(w, p)
}

type procStatus struct {
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	Type    string `json:"type"`
	Running bool   `json:"running"`
}

func apiListProcs(w http.ResponseWriter, r *http.Request) {
	m.Lock()

	stats := make([]procStatus, 0, len(procs))
	for _, p := range procs {
		b, _ := p.checkAlive()
		s := procStatus{
			ID:      p.ID,
			Pid:     p.Pid,
			Type:    p.Type,
			Running: b,
		}

		stats = append(stats, s)
	}

	m.Unlock()

	respJson(w, stats)
}

// /check_store?addr=addr
func apiCheckStore(w http.ResponseWriter, r *http.Request) {
	addr := r.FormValue("addr")

	err := utils.Ping(addr, globalEnv.StoreAuth())
	if err != nil {
		respError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}
