// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"strconv"
	"time"

	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/utils"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
	"github.com/nu7hatch/gouuid"
)

type NodeInfo struct {
	GroupId   int
	CurSlots  []int
	MaxMemory int64
}

func getLivingNodeInfos(coordConn zkhelper.Conn) ([]*NodeInfo, error) {
	groups, err := models.ServerGroups(coordConn, globalEnv.ProductName())
	if err != nil {
		return nil, errors.Trace(err)
	}
	slots, err := models.Slots(coordConn, globalEnv.ProductName())
	slotMap := make(map[int][]int)
	for _, slot := range slots {
		if slot.State.Status == models.SLOT_STATUS_ONLINE {
			slotMap[slot.GroupId] = append(slotMap[slot.GroupId], slot.Id)
		}
	}
	var ret []*NodeInfo
	for _, g := range groups {
		master, err := g.Master(coordConn)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if master == nil {
			return nil, errors.Errorf("group %d has no master", g.Id)
		}
		out, err := utils.GetRedisConfig(master.Addr, "MAXMEMORY", globalEnv.StoreAuth())
		if err != nil {
			return nil, errors.Trace(err)
		}
		maxMem, err := strconv.ParseInt(out, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if maxMem <= 0 {
			return nil, errors.Errorf("redis %s should set maxmemory", master.Addr)
		}
		node := &NodeInfo{
			GroupId:   g.Id,
			CurSlots:  slotMap[g.Id],
			MaxMemory: maxMem,
		}
		ret = append(ret, node)
	}
	cnt := 0
	for _, info := range ret {
		cnt += len(info.CurSlots)
	}
	if cnt != models.DEFAULT_SLOT_NUM {
		return nil, errors.New("not all slots are online")
	}
	return ret, nil
}

func getQuotaMap(coordConn zkhelper.Conn) (map[int]int, error) {
	nodes, err := getLivingNodeInfos(coordConn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := make(map[int]int)
	var totalMem int64
	totalQuota := 0
	for _, node := range nodes {
		totalMem += node.MaxMemory
	}

	for _, node := range nodes {
		quota := int(models.DEFAULT_SLOT_NUM * node.MaxMemory * 1.0 / totalMem)
		ret[node.GroupId] = quota
		totalQuota += quota
	}

	// round up
	if totalQuota < models.DEFAULT_SLOT_NUM {
		for k, _ := range ret {
			ret[k] += models.DEFAULT_SLOT_NUM - totalQuota
			break
		}
	}

	return ret, nil
}

// experimental simple auto rebalance :)
func Rebalance(coordConn zkhelper.Conn, delay int) error {
	targetQuota, err := getQuotaMap(coordConn)
	if err != nil {
		return errors.Trace(err)
	}
	livingNodes, err := getLivingNodeInfos(coordConn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("start rebalance")
	for _, node := range livingNodes {
		for len(node.CurSlots) > targetQuota[node.GroupId] {
			for _, dest := range livingNodes {
				if dest.GroupId != node.GroupId && len(dest.CurSlots) < targetQuota[dest.GroupId] {
					slot := node.CurSlots[len(node.CurSlots)-1]
					// create a migration task
					t := NewMigrateTask(MigrateTaskInfo{
						Delay:      delay,
						FromSlot:   slot,
						ToSlot:     slot,
						NewGroupId: dest.GroupId,
						Status:     MIGRATE_TASK_MIGRATING,
						CreateAt:   strconv.FormatInt(time.Now().Unix(), 10),
					})
					u, err := uuid.NewV4()
					if err != nil {
						return errors.Trace(err)
					}
					t.Id = u.String()

					if ok, err := preMigrateCheck(t); ok {
						// do migrate
						err := t.run()
						if err != nil {
							log.Warning(err)
							return errors.Trace(err)
						}
					} else {
						log.Warning(err)
						return errors.Trace(err)
					}
					node.CurSlots = node.CurSlots[0 : len(node.CurSlots)-1]
					dest.CurSlots = append(dest.CurSlots, slot)
				}
			}
		}
	}
	log.Info("rebalance finish")
	return nil
}
