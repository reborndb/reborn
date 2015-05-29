// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

func runHTTPServer() {
	m := mux.NewRouter()

	m.HandleFunc("/api/start", apiStartProc).Methods("POST", "PUT")
	m.HandleFunc("/api/start_redis", apiStartRedisProc).Methods("POST", "PUT")
	m.HandleFunc("/api/stop", apiStopProc).Methods("DELETE", "POST", "PUT")
	m.HandleFunc("/api/procs", apiListProcs)

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
	case proxyType, dashboardType, qdbType:
		w.WriteHeader(http.StatusNotImplemented)
		return
	case redisType:
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

// /start_redis?port=6379
func apiStartRedisProc(w http.ResponseWriter, r *http.Request) {
	port := r.FormValue("port")
	if len(port) == 0 {
		respError(w, http.StatusBadRequest, fmt.Sprintf("must have a port for redis, not empty"))
		return
	}
	if n, err := strconv.ParseInt(port, 10, 16); err != nil || n <= 0 {
		respError(w, http.StatusBadRequest, fmt.Sprintf("invalid port redis port %s", port))
		return
	}

	args := new(redisArgs)
	args.Port = port

	p, err := startRedis(args)
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
