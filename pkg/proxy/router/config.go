// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strings"

	"github.com/reborndb/reborn/pkg/proxy/router/topology"
	"github.com/reborndb/reborn/pkg/utils"

	log "github.com/ngaut/logging"
)

type Conf struct {
	ProductName     string
	NetTimeout      int    //seconds
	Proto           string //tcp or tcp4
	CoordinatorAddr string
	Coordinator     string

	Addr     string
	HTTPAddr string
	ProxyID  string
	PidFile  string

	// unexport
	f topology.CoordFactory
}

func LoadConf(configFile string) (*Conf, error) {
	srvConf := &Conf{}
	conf, err := utils.InitConfigFromFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	srvConf.ProductName, _ = conf.ReadString("product", "test")
	if len(srvConf.ProductName) == 0 {
		log.Fatalf("invalid config: product entry is missing in %s", configFile)
	}
	srvConf.CoordinatorAddr, _ = conf.ReadString("coordinator_addr", "")
	if len(srvConf.CoordinatorAddr) == 0 {
		log.Fatalf("invalid config: need coordinator addr entry is missing in %s", configFile)
	}
	srvConf.CoordinatorAddr = strings.TrimSpace(srvConf.CoordinatorAddr)

	srvConf.NetTimeout, _ = conf.ReadInt("net_timeout", 5)
	srvConf.Proto, _ = conf.ReadString("proto", "tcp")
	srvConf.Coordinator, _ = conf.ReadString("coordinator", "zookeeper")

	srvConf.Addr, _ = conf.ReadString("addr", "")
	srvConf.HTTPAddr, _ = conf.ReadString("http_addr", "")
	srvConf.ProxyID, _ = conf.ReadString("proxy_id", "")
	srvConf.PidFile, _ = conf.ReadString("pidfile", "")

	return srvConf, nil
}
