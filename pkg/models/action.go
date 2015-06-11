// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
)

type ActionType string

const (
	ACTION_TYPE_SERVER_GROUP_CHANGED ActionType = "group_changed"
	ACTION_TYPE_SERVER_GROUP_REMOVE  ActionType = "group_remove"
	ACTION_TYPE_SLOT_CHANGED         ActionType = "slot_changed"
	ACTION_TYPE_MULTI_SLOT_CHANGED   ActionType = "multi_slot_changed"
	ACTION_TYPE_SLOT_MIGRATE         ActionType = "slot_migrate"
	ACTION_TYPE_SLOT_PREMIGRATE      ActionType = "slot_premigrate"

	ActionTimeoutMs     = 30 * 1000
	CheckTimeIntervalMs = 500
	MaxKeepActionsNum   = 500
)

const (
	GC_TYPE_N = iota + 1
	GC_TYPE_SEC
)

type Action struct {
	Type      ActionType  `json:"type"`
	Desc      string      `json:"desc"`
	Target    interface{} `json:"target"`
	Ts        string      `json:"ts"` // timestamp
	Receivers []string    `json:"receivers"`
}

func GetWatchActionPath(productName string) string {
	return fmt.Sprintf("/zk/reborn/db_%s/actions", productName)
}

func GetActionResponsePath(productName string) string {
	return path.Join(path.Dir(GetWatchActionPath(productName)), "ActionResponse")
}

func GetActionWithSeq(coordConn zkhelper.Conn, productName string, seq int64, provider string) (*Action, error) {
	var act Action
	data, _, err := coordConn.Get(path.Join(GetWatchActionPath(productName), coordConn.Seq2Str(seq)))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := json.Unmarshal(data, &act); err != nil {
		return nil, errors.Trace(err)
	}

	return &act, nil
}

func GetActionObject(coordConn zkhelper.Conn, productName string, seq int64, act interface{}, provider string) error {
	data, _, err := coordConn.Get(path.Join(GetWatchActionPath(productName), coordConn.Seq2Str(seq)))
	if err != nil {
		return errors.Trace(err)
	}

	if err := json.Unmarshal(data, act); err != nil {
		return errors.Trace(err)
	}

	return nil
}

var ErrReceiverTimeout = errors.New("receiver timeout")

func WaitForReceiverWithTimeout(coordConn zkhelper.Conn, productName string, actionCoordPath string, proxies []ProxyInfo, timeoutInMs int) error {
	if len(proxies) == 0 {
		return nil
	}

	times := 0
	proxyIds := make(map[string]struct{})
	var offlineProxyIds []string
	for _, p := range proxies {
		proxyIds[p.ID] = struct{}{}
	}

	checkTimes := timeoutInMs / CheckTimeIntervalMs
	for times < checkTimes {
		if times >= 6 && (times*CheckTimeIntervalMs)%1000 == 0 {
			log.Warning("abnormal waiting time for receivers", actionCoordPath, offlineProxyIds)
		}
		// get confirm ids
		nodes, _, err := coordConn.Children(actionCoordPath)
		if err != nil {
			return errors.Trace(err)
		}
		confirmIds := make(map[string]struct{})
		for _, node := range nodes {
			id := path.Base(node)
			confirmIds[id] = struct{}{}
		}
		if len(confirmIds) != 0 {
			match := true
			// check if all proxy have responsed
			var notMatchList []string
			for id, _ := range proxyIds {
				// if proxy id not in confirm ids, means someone didn't response
				if _, ok := confirmIds[id]; !ok {
					match = false
					notMatchList = append(notMatchList, id)
				}
			}
			if match {
				return nil
			}
			offlineProxyIds = notMatchList
		}
		times += 1
		time.Sleep(CheckTimeIntervalMs * time.Millisecond)
	}
	if len(offlineProxyIds) > 0 {
		log.Error("proxies didn't responed: ", offlineProxyIds)
	}

	// set offline proxies
	for _, id := range offlineProxyIds {
		log.Errorf("mark proxy %s to PROXY_STATE_MARK_OFFLINE", id)
		if err := SetProxyStatus(coordConn, productName, id, PROXY_STATE_MARK_OFFLINE); err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(ErrReceiverTimeout)
}

func GetActionSeqList(coordConn zkhelper.Conn, productName string) ([]int, error) {
	nodes, _, err := coordConn.Children(GetWatchActionPath(productName))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ExtraSeqList(nodes)
}

func ExtraSeqList(nodes []string) ([]int, error) {
	var seqs []int
	for _, nodeName := range nodes {
		//ugly code for support old version
		ss := strings.Split(nodeName, "_")
		seq, err := strconv.Atoi(ss[len(ss)-1])

		if err != nil {
			return nil, errors.Trace(err)
		}
		seqs = append(seqs, seq)
	}

	sort.Ints(seqs)

	return seqs, nil
}

func ActionGC(coordConn zkhelper.Conn, productName string, gcType int, keep int) error {
	prefix := GetWatchActionPath(productName)
	respPrefix := GetActionResponsePath(productName)

	exists, err := zkhelper.NodeExists(coordConn, prefix)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		// if action path not exists just return nil
		return nil
	}

	actions, _, err := coordConn.Children(prefix)
	if err != nil {
		return errors.Trace(err)
	}

	var act Action

	if gcType == GC_TYPE_N {
		sort.Strings(actions)
		if len(actions)-MaxKeepActionsNum <= keep {
			return nil
		}
		for _, action := range actions[:len(actions)-keep-MaxKeepActionsNum] {
			if err := zkhelper.DeleteRecursive(coordConn, path.Join(prefix, action), -1); err != nil {
				return errors.Trace(err)
			}
			err := zkhelper.DeleteRecursive(coordConn, path.Join(respPrefix, action), -1)
			if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
				return errors.Trace(err)
			}
		}
	} else if gcType == GC_TYPE_SEC {
		secs := keep
		currentTs := time.Now().Unix()

		for _, action := range actions {
			b, _, err := coordConn.Get(path.Join(prefix, action))
			if err != nil {
				return errors.Trace(err)
			}
			if err := json.Unmarshal(b, &act); err != nil {
				return errors.Trace(err)
			}
			log.Info(action, act.Ts)
			ts, _ := strconv.ParseInt(act.Ts, 10, 64)

			if currentTs-ts > int64(secs) {
				if err := zkhelper.DeleteRecursive(coordConn, path.Join(prefix, action), -1); err != nil {
					return errors.Trace(err)
				}
				err := zkhelper.DeleteRecursive(coordConn, path.Join(respPrefix, action), -1)
				if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
					return errors.Trace(err)
				}
			}
		}
	}

	return nil
}

func CreateActionRootPath(coordConn zkhelper.Conn, path string) error {
	// if action dir not exists, create it first
	exists, err := zkhelper.NodeExists(coordConn, path)
	if err != nil {
		return errors.Trace(err)
	}

	if !exists {
		_, err := zkhelper.CreateOrUpdate(coordConn, path, "", 0, zkhelper.DefaultDirACLs(), true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func NewAction(coordConn zkhelper.Conn, productName string, actionType ActionType, target interface{}, desc string, needConfirm bool) error {
	return NewActionWithTimeout(coordConn, productName, actionType, target, desc, needConfirm, ActionTimeoutMs)
}

func NewActionWithTimeout(coordConn zkhelper.Conn, productName string, actionType ActionType, target interface{}, desc string, needConfirm bool, timeoutInMs int) error {
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	action := &Action{
		Type:   actionType,
		Desc:   desc,
		Target: target,
		Ts:     ts,
	}

	// set action receivers
	proxies, err := ProxyList(coordConn, productName, func(p *ProxyInfo) bool {
		return p.State == PROXY_STATE_ONLINE
	})
	if err != nil {
		return errors.Trace(err)
	}
	if needConfirm {
		// do fencing here, make sure 'offline' proxies are really offline
		// now we only check whether the proxy lists are match
		fenceProxies, err := GetFenceProxyMap(coordConn, productName)
		if err != nil {
			return errors.Trace(err)
		}
		for _, proxy := range proxies {
			delete(fenceProxies, proxy.Addr)
		}
		if len(fenceProxies) > 0 {
			errMsg := bytes.NewBufferString("Some proxies may not stop cleanly:")
			for k, _ := range fenceProxies {
				errMsg.WriteString(" ")
				errMsg.WriteString(k)
			}
			return errors.New(errMsg.String())
		}
	}
	for _, p := range proxies {
		buf, err := json.Marshal(p)
		if err != nil {
			return errors.Trace(err)
		}
		action.Receivers = append(action.Receivers, string(buf))
	}

	b, _ := json.Marshal(action)

	prefix := GetWatchActionPath(productName)
	//action root path
	err = CreateActionRootPath(coordConn, prefix)
	if err != nil {
		return errors.Trace(err)
	}

	//response path
	respPath := path.Join(path.Dir(prefix), "ActionResponse")
	err = CreateActionRootPath(coordConn, respPath)
	if err != nil {
		return errors.Trace(err)
	}

	//create response node, etcd do not support create in order directory
	//get path first
	actionRespPath, err := coordConn.Create(respPath+"/", b, int32(zk.FlagSequence), zkhelper.DefaultFileACLs())
	if err != nil {
		log.Error(err, respPath)
		return errors.Trace(err)
	}

	//remove file then create directory
	coordConn.Delete(actionRespPath, -1)
	actionRespPath, err = coordConn.Create(actionRespPath, b, 0, zkhelper.DefaultDirACLs())
	if err != nil {
		log.Error(err, respPath)
		return errors.Trace(err)
	}

	suffix := path.Base(actionRespPath)

	// create action node
	actionPath := path.Join(prefix, suffix)
	_, err = coordConn.Create(actionPath, b, 0, zkhelper.DefaultFileACLs())
	if err != nil {
		log.Error(err, actionPath)
		return errors.Trace(err)
	}

	if needConfirm {
		if err := WaitForReceiverWithTimeout(coordConn, productName, actionRespPath, proxies, timeoutInMs); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func ForceRemoveLock(coordConn zkhelper.Conn, productName string) error {
	lockPath := fmt.Sprintf("/zk/reborn/db_%s/LOCK", productName)
	children, _, err := coordConn.Children(lockPath)
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return errors.Trace(err)
	}

	for _, c := range children {
		fullPath := path.Join(lockPath, c)
		log.Info("deleting..", fullPath)
		err := coordConn.Delete(fullPath, 0)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func ForceRemoveDeadFence(coordConn zkhelper.Conn, productName string) error {
	proxies, err := ProxyList(coordConn, productName, func(p *ProxyInfo) bool {
		return p.State == PROXY_STATE_ONLINE
	})
	if err != nil {
		return errors.Trace(err)
	}
	fenceProxies, err := GetFenceProxyMap(coordConn, productName)
	if err != nil {
		return errors.Trace(err)
	}
	// remove online proxies's fence
	for _, proxy := range proxies {
		delete(fenceProxies, proxy.Addr)
	}

	// delete dead fence in zookeeper
	path := GetProxyFencePath(productName)
	for remainFence, _ := range fenceProxies {
		fencePath := filepath.Join(path, remainFence)
		log.Info("removing fence: ", fencePath)
		if err := zkhelper.DeleteRecursive(coordConn, fencePath, -1); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
