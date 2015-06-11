// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"github.com/juju/errors"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/zkhelper"
)

type agentInfo struct {
	ID   string
	Addr string `json:"addr"`
	PID  int    `json:"pid"`
}

func agentPath() string {
	return fmt.Sprintf("/zk/reborn/db_%s/agent", globalEnv.ProductName())
}

func addAgent(a *agentInfo) error {
	basePath := agentPath()

	zkhelper.CreateRecursive(globalConn, basePath, "", 0, zkhelper.DefaultDirACLs())

	contents, err := json.Marshal(a)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = globalConn.Create(path.Join(basePath, a.ID), contents, zk.FlagEphemeral, zkhelper.DefaultFileACLs())
	return errors.Trace(err)
}

func getAgents() ([]*agentInfo, error) {
	basePath := agentPath()

	children, _, err := globalConn.Children(basePath)
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return nil, errors.Trace(err)
	}

	sort.Strings(children)

	var agents []*agentInfo
	for _, child := range children {
		if agent, err := getAgent(child); err != nil {
			return nil, errors.Trace(err)
		} else {
			agents = append(agents, agent)
		}
	}
	return agents, nil
}

func getAgent(id string) (*agentInfo, error) {
	basePath := agentPath()

	data, _, err := globalConn.Get(path.Join(basePath, id))
	if err != nil {
		return nil, errors.Trace(err)
	}

	a := new(agentInfo)
	if err := json.Unmarshal(data, a); err != nil {
		return nil, errors.Trace(err)
	}

	a.ID = id
	return a, nil
}
