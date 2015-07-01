// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package group

import (
	"github.com/ngaut/log"
	"github.com/reborndb/reborn/pkg/models"
)

type Group struct {
	master       string
	redisServers map[string]*models.Server
}

func (g *Group) Master() string {
	return g.master
}

func NewGroup(groupInfo models.ServerGroup) *Group {
	g := &Group{
		redisServers: make(map[string]*models.Server),
	}

	for _, server := range groupInfo.Servers {
		if server.Type == models.SERVER_TYPE_MASTER {
			if len(g.master) > 0 {
				log.Fatalf("two masters are not allowed: %+v", groupInfo)
			}
			g.master = server.Addr
		}
		g.redisServers[server.Addr] = server
	}

	if len(g.master) == 0 {
		log.Fatalf("master not found: %+v", groupInfo)
	}

	return g
}
