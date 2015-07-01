// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"

	"github.com/docopt/docopt-go"
	"github.com/ngaut/log"
	"github.com/reborndb/reborn/pkg/models"
)

func cmdProxy(argv []string) (err error) {
	usage := `usage:
	reborn-config proxy list
	reborn-config proxy offline <proxy_name>
	reborn-config proxy online <proxy_name>
`
	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Debug(args)

	if args["list"].(bool) {
		return runProxyList()
	}

	proxyName := args["<proxy_name>"].(string)
	if args["online"].(bool) {
		return runSetProxyStatus(proxyName, models.PROXY_STATE_ONLINE)
	}
	if args["offline"].(bool) {
		return runSetProxyStatus(proxyName, models.PROXY_STATE_MARK_OFFLINE)
	}
	return nil
}

func runProxyList() error {
	var v interface{}
	err := callApi(METHOD_GET, "/api/proxy/list", nil, &v)
	if err != nil {
		return err
	}
	fmt.Println(jsonify(v))
	return nil
}

func runSetProxyStatus(proxyName, status string) error {
	info := models.ProxyInfo{
		ID:    proxyName,
		State: status,
	}
	var v interface{}
	err := callApi(METHOD_POST, "/api/proxy", info, &v)
	if err != nil {
		return err
	}
	fmt.Println(jsonify(v))
	return nil
}
