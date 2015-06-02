// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topology

import (
	"encoding/json"
	"path"

	"github.com/reborndb/reborn/pkg/models"

	"github.com/juju/errors"
	topo "github.com/ngaut/go-zookeeper/zk"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
)

type TopoUpdate interface {
	OnGroupChange(groupId int)
	OnSlotChange(slotId int)
}

type CoordFactory func(coordAddr string) (zkhelper.Conn, error)

type Topology struct {
	ProductName string
	coordAddr   string
	coordConn   zkhelper.Conn
	fact        CoordFactory
	coordinator string
}

func (top *Topology) GetGroup(groupId int) (*models.ServerGroup, error) {
	return models.GetGroup(top.coordConn, top.ProductName, groupId)
}

func (top *Topology) Exist(path string) (bool, error) {
	return zkhelper.NodeExists(top.coordConn, path)
}

func (top *Topology) GetSlotByIndex(i int) (*models.Slot, *models.ServerGroup, error) {
	slot, err := models.GetSlot(top.coordConn, top.ProductName, i)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	log.Debugf("get slot %d : %+v", i, slot)
	if slot.State.Status != models.SLOT_STATUS_ONLINE && slot.State.Status != models.SLOT_STATUS_MIGRATE {
		log.Errorf("slot not online, %+v", slot)
	}

	groupServer, err := models.GetGroup(top.coordConn, top.ProductName, slot.GroupId)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return slot, groupServer, nil
}

func NewTopo(ProductName string, coordAddr string, f CoordFactory, coordinator string) *Topology {
	t := &Topology{coordAddr: coordAddr, ProductName: ProductName, fact: f, coordinator: coordinator}
	if t.fact == nil {
		switch t.coordinator {
		case "etcd":
			t.fact = zkhelper.NewEtcdConn
		case "zookeeper":
			t.fact = zkhelper.ConnectToZk
		default:
			log.Fatal("coordinator not found in config")
		}
	}
	t.InitCoordConn()
	return t
}

func (top *Topology) InitCoordConn() {
	var err error
	top.coordConn, err = top.fact(top.coordAddr)
	if err != nil {
		log.Fatal(err)
	}
}

func (top *Topology) GetActionWithSeq(seq int64) (*models.Action, error) {
	return models.GetActionWithSeq(top.coordConn, top.ProductName, seq, top.coordinator)
}

func (top *Topology) GetActionWithSeqObject(seq int64, act *models.Action) error {
	return models.GetActionObject(top.coordConn, top.ProductName, seq, act, top.coordinator)
}

func (top *Topology) GetActionSeqList(productName string) ([]int, error) {
	return models.GetActionSeqList(top.coordConn, productName)
}

func (top *Topology) IsChildrenChangedEvent(e interface{}) bool {
	return e.(topo.Event).Type == topo.EventNodeChildrenChanged
}

func (top *Topology) CreateProxyInfo(pi *models.ProxyInfo) (string, error) {
	return models.CreateProxyInfo(top.coordConn, top.ProductName, pi)
}

func (top *Topology) CreateProxyFenceNode(pi *models.ProxyInfo) (string, error) {
	return models.CreateProxyFenceNode(top.coordConn, top.ProductName, pi)
}

func (top *Topology) GetProxyInfo(proxyName string) (*models.ProxyInfo, error) {
	return models.GetProxyInfo(top.coordConn, top.ProductName, proxyName)
}

func (top *Topology) GetActionResponsePath(seq int) string {
	return path.Join(models.GetActionResponsePath(top.ProductName), top.coordConn.Seq2Str(int64(seq)))
}

func (top *Topology) SetProxyStatus(proxyName string, status string) error {
	return models.SetProxyStatus(top.coordConn, top.ProductName, proxyName, status)
}

func (top *Topology) Close(proxyName string) {
	// delete fence znode
	pi, err := models.GetProxyInfo(top.coordConn, top.ProductName, proxyName)
	if err != nil {
		log.Error("killing fence error, proxy %s is not exists", proxyName)
	} else {
		zkhelper.DeleteRecursive(top.coordConn, path.Join(models.GetProxyFencePath(top.ProductName), pi.Addr), -1)
	}
	// delete ephemeral znode
	zkhelper.DeleteRecursive(top.coordConn, path.Join(models.GetProxyPath(top.ProductName), proxyName), -1)
	top.coordConn.Close()
}

func (top *Topology) DoResponse(seq int, pi *models.ProxyInfo) error {
	//create response node
	actionPath := top.GetActionResponsePath(seq)
	//log.Debug("actionPath:", actionPath)
	data, err := json.Marshal(pi)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = top.coordConn.Create(path.Join(actionPath, pi.Id), data,
		0, zkhelper.DefaultFileACLs())

	return err
}

func (top *Topology) IsSessionExpiredEvent(event interface{}) bool {
	e, ok := event.(topo.Event)
	if !ok {
		return false
	}

	if e.State == topo.StateExpired && e.Type == topo.EventNotWatching {
		return true
	}

	return false
}

func (top *Topology) doWatch(evtch <-chan topo.Event, evtbus chan interface{}) {
	e := <-evtch
	log.Warningf("topo event %+v", e)

	switch e.Type {
	//case topo.EventNodeCreated:
	//case topo.EventNodeDataChanged:
	case topo.EventNodeChildrenChanged: //only care children changed
		//todo:get changed node and decode event
	default:
		log.Warningf("%+v", e)
	}

	evtbus <- e
}

func (top *Topology) WatchChildren(path string, evtbus chan interface{}) ([]string, error) {
	content, _, evtch, err := top.coordConn.ChildrenW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go top.doWatch(evtch, evtbus)
	return content, nil
}

func (top *Topology) WatchNode(path string, evtbus chan interface{}) ([]byte, error) {
	content, _, evtch, err := top.coordConn.GetW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go top.doWatch(evtch, evtbus)
	return content, nil
}
