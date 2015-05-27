package env

import (
	"os"
	"strings"

	"github.com/c4pt0r/cfg"
	errors "github.com/juju/errors"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
)

type Env interface {
	ProductName() string
	DashboardAddr() string
	NewCoordConn() (zkhelper.Conn, error)
}

type RebornEnv struct {
	dashboardAddr   string
	productName     string
	coordinator     string
	coordinatorAddr string
}

func LoadRebornEnv(cfg *cfg.Cfg) Env {
	if cfg == nil {
		log.Fatal("config error")
	}

	productName, err := cfg.ReadString("product", "test")
	if err != nil {
		log.Fatal(err)
	}

	coordinatorAddr, err := cfg.ReadString("coordinator_addr", "localhost:2181")
	if err != nil {
		log.Fatal(err)
	}

	hostname, _ := os.Hostname()
	dashboardAddr, err := cfg.ReadString("dashboard_addr", hostname+":18087")
	if err != nil {
		log.Fatal(err)
	}

	coordinator, err := cfg.ReadString("coordinator", "zookeeper")
	if err != nil {
		log.Fatal(err)
	}

	return &RebornEnv{
		dashboardAddr:   dashboardAddr,
		productName:     productName,
		coordinator:     coordinator,
		coordinatorAddr: coordinatorAddr,
	}
}

func (e *RebornEnv) ProductName() string {
	return e.productName
}

func (e *RebornEnv) DashboardAddr() string {
	return e.dashboardAddr
}

func (e *RebornEnv) NewCoordConn() (zkhelper.Conn, error) {
	switch e.coordinator {
	case "zookeeper":
		return zkhelper.ConnectToZk(e.coordinatorAddr)
	case "etcd":
		addr := strings.TrimSpace(e.coordinatorAddr)
		if !strings.HasPrefix(addr, "http://") {
			addr = "http://" + addr
		}
		return zkhelper.NewEtcdConn(addr)
	}

	return nil, errors.Errorf("need coordinator in config file, %+v", e)
}
