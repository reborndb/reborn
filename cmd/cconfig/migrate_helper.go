// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/juju/errors"
	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/utils"
)

const (
	MIGRATE_TIMEOUT = 30000
)

var ErrGroupMasterNotFound = errors.New("group master not found")
var ErrInvalidAddr = errors.New("invalid addr")
var ErrStopMigrateByUser = errors.New("migration stopped by user")
var ErrServerIsNotMaster = errors.New("server is not master")

// return: success_count, remain_count, error
// slotsmgrt host port timeout slotnum count
func sendRedisMigrateCmd(c redis.Conn, slotId int, toAddr string) (int, int, error) {
	addrParts := strings.Split(toAddr, ":")
	if len(addrParts) != 2 {
		return -1, -1, ErrInvalidAddr
	}

	reply, err := redis.Values(c.Do("SLOTSMGRTTAGSLOT", addrParts[0], addrParts[1], MIGRATE_TIMEOUT, slotId))
	if err != nil {
		return -1, -1, errors.Trace(err)
	}

	var succ, remain int
	if _, err := redis.Scan(reply, &succ, &remain); err != nil {
		return -1, -1, errors.Trace(err)
	}
	return succ, remain, nil
}

func checkMaster(addr string) error {
	if master, err := utils.GetRole(addr, ""); err != nil {
		return errors.Trace(err)
	} else if master != "master" {
		return ErrServerIsNotMaster
	} else {
		return nil
	}
}

// Migrator Implement
type RebornSlotMigrator struct{}

func (m *RebornSlotMigrator) Migrate(slot *models.Slot, fromGroup, toGroup int, task *MigrateTask, onProgress func(SlotMigrateProgress)) (err error) {
	groupFrom, err := models.GetGroup(task.coordConn, task.productName, fromGroup)
	if err != nil {
		return errors.Trace(err)
	}
	groupTo, err := models.GetGroup(task.coordConn, task.productName, toGroup)
	if err != nil {
		return errors.Trace(err)
	}

	fromMaster, err := groupFrom.Master(task.coordConn)
	if err != nil {
		return errors.Trace(err)
	}

	toMaster, err := groupTo.Master(task.coordConn)
	if err != nil {
		return errors.Trace(err)
	}

	if fromMaster == nil || toMaster == nil {
		return ErrGroupMasterNotFound
	}

	if err = checkMaster(fromMaster.Addr); err != nil {
		return errors.Trace(err)
	}

	if err = checkMaster(toMaster.Addr); err != nil {
		return errors.Trace(err)
	}

	c, err := redis.Dial("tcp", fromMaster.Addr)
	if err != nil {
		return errors.Trace(err)
	}

	defer c.Close()

	_, remain, err := sendRedisMigrateCmd(c, slot.Id, toMaster.Addr)
	if err != nil {
		return errors.Trace(err)
	}

	for remain > 0 {
		if task.Delay > 0 {
			time.Sleep(time.Duration(task.Delay) * time.Millisecond)
		}
		if task.stopChan != nil {
			select {
			case <-task.stopChan:
				return ErrStopMigrateByUser
			default:
			}
		}
		_, remain, err = sendRedisMigrateCmd(c, slot.Id, toMaster.Addr)
		if remain >= 0 {
			onProgress(SlotMigrateProgress{
				SlotId:    slot.Id,
				FromGroup: fromGroup,
				ToGroup:   toGroup,
				Remain:    remain,
			})
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
