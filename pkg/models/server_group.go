// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/reborndb/reborn/pkg/utils"

	"github.com/juju/errors"
	"github.com/ngaut/go-zookeeper/zk"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
)

const (
	SERVER_TYPE_MASTER  string = "master"
	SERVER_TYPE_SLAVE   string = "slave"
	SERVER_TYPE_OFFLINE string = "offline"
)

// redis server instance
type Server struct {
	Type    string `json:"type"`
	GroupId int    `json:"group_id"`
	Addr    string `json:"addr"`
}

// redis server group
type ServerGroup struct {
	Id          int       `json:"id"`
	ProductName string    `json:"product_name"`
	Servers     []*Server `json:"servers"`
}

func (s *Server) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

func (sg *ServerGroup) String() string {
	b, _ := json.MarshalIndent(sg, "", "  ")
	return string(b) + "\n"
}

func GetServer(coordConn zkhelper.Conn, coordPath string) (*Server, error) {
	data, _, err := coordConn.Get(coordPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	srv := Server{}
	if err := json.Unmarshal(data, &srv); err != nil {
		return nil, errors.Trace(err)
	}
	return &srv, nil
}

func NewServer(serverType string, addr string) *Server {
	return &Server{
		Type:    serverType,
		GroupId: INVALID_ID,
		Addr:    addr,
	}
}

func NewServerGroup(productName string, id int) *ServerGroup {
	return &ServerGroup{
		Id:          id,
		ProductName: productName,
	}
}

func GroupExists(coordConn zkhelper.Conn, productName string, groupId int) (bool, error) {
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d", productName, groupId)
	exists, _, err := coordConn.Exists(coordPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return exists, nil
}

func GetGroup(coordConn zkhelper.Conn, productName string, groupId int) (*ServerGroup, error) {
	exists, err := GroupExists(coordConn, productName, groupId)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exists {
		return nil, errors.NotFoundf("group %d", groupId)
	}

	group := &ServerGroup{
		ProductName: productName,
		Id:          groupId,
	}

	group.Servers, err = group.GetServers(coordConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return group, nil
}

func ServerGroups(coordConn zkhelper.Conn, productName string) ([]*ServerGroup, error) {
	var ret []*ServerGroup
	root := fmt.Sprintf("/zk/reborn/db_%s/servers", productName)
	groups, _, err := coordConn.Children(root)
	// if ErrNoNode, we may return an empty slice like ProxyList
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return nil, errors.Trace(err)
	}

	// Buggy :X
	//zkhelper.ChildrenRecursive(*coordConn, root)

	for _, group := range groups {
		// parse group_1 => 1
		groupId, err := strconv.Atoi(strings.Split(group, "_")[1])
		if err != nil {
			return nil, errors.Trace(err)
		}
		g, err := GetGroup(coordConn, productName, groupId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, g)
	}
	return ret, nil
}

func (sg *ServerGroup) Master(coordConn zkhelper.Conn) (*Server, error) {
	servers, err := sg.GetServers(coordConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range servers {
		// TODO check if there are two masters
		if s.Type == SERVER_TYPE_MASTER {
			return s, nil
		}
	}
	return nil, nil
}

func (sg *ServerGroup) Remove(coordConn zkhelper.Conn) error {
	// check if this group is not used by any slot
	slots, err := Slots(coordConn, sg.ProductName)
	if err != nil {
		return errors.Trace(err)
	}

	for _, slot := range slots {
		if slot.GroupId == sg.Id {
			return errors.AlreadyExistsf("group %d is using by slot %d", slot.GroupId, slot.Id)
		}
	}

	// do delete
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d", sg.ProductName, sg.Id)
	err = zkhelper.DeleteRecursive(coordConn, coordPath, -1)

	// we know that there's no slots affected, so this action doesn't need proxy confirm
	err = NewAction(coordConn, sg.ProductName, ACTION_TYPE_SERVER_GROUP_REMOVE, sg, "", false)
	return errors.Trace(err)
}

func (sg *ServerGroup) RemoveServer(coordConn zkhelper.Conn, addr string) error {
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d/%s", sg.ProductName, sg.Id, addr)
	data, _, err := coordConn.Get(coordPath)
	if err != nil {
		return errors.Trace(err)
	}

	var s Server
	err = json.Unmarshal(data, &s)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(s)
	if s.Type == SERVER_TYPE_MASTER {
		return errors.New("cannot remove master, use promote first")
	}

	err = coordConn.Delete(coordPath, -1)
	if err != nil {
		return errors.Trace(err)
	}

	// update server list
	for i := 0; i < len(sg.Servers); i++ {
		if sg.Servers[i].Addr == s.Addr {
			sg.Servers = append(sg.Servers[:i], sg.Servers[i+1:]...)
			break
		}
	}

	// remove slave won't need proxy confirm
	err = NewAction(coordConn, sg.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, sg, "", false)
	return errors.Trace(err)
}

func (sg *ServerGroup) Promote(conn zkhelper.Conn, addr string, auth string) error {
	var s *Server
	exists := false
	for i := 0; i < len(sg.Servers); i++ {
		if sg.Servers[i].Addr == addr {
			s = sg.Servers[i]
			exists = true
			break
		}
	}

	if !exists {
		return errors.NotFoundf("no such addr %s", addr)
	}

	err := utils.SlaveNoOne(s.Addr, auth)
	if err != nil {
		return errors.Trace(err)
	}

	// set origin master offline
	master, err := sg.Master(conn)
	if err != nil {
		return errors.Trace(err)
	}

	// old master may be nil
	if master != nil {
		master.Type = SERVER_TYPE_OFFLINE
		err = sg.AddServer(conn, master, auth)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// promote new server to master
	s.Type = SERVER_TYPE_MASTER
	err = sg.AddServer(conn, s, auth)
	return errors.Trace(err)
}

func (sg *ServerGroup) Create(coordConn zkhelper.Conn) error {
	if sg.Id < 0 {
		return errors.NotSupportedf("invalid server group id %d", sg.Id)
	}
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d", sg.ProductName, sg.Id)
	_, err := zkhelper.CreateOrUpdate(coordConn, coordPath, "", 0, zkhelper.DefaultDirACLs(), true)
	if err != nil {
		return errors.Trace(err)
	}
	err = NewAction(coordConn, sg.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, sg, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	// set no server slots' group id to this server group, no need to return error
	slots, err := NoGroupSlots(coordConn, sg.ProductName)
	if err == nil && len(slots) > 0 {
		SetSlots(coordConn, sg.ProductName, slots, sg.Id, SLOT_STATUS_ONLINE)
	}

	return nil
}

func (sg *ServerGroup) Exists(coordConn zkhelper.Conn) (bool, error) {
	coordPath := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d", sg.ProductName, sg.Id)
	b, err := zkhelper.NodeExists(coordConn, coordPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return b, nil
}

var ErrNodeExists = errors.New("node already exists")

func (sg *ServerGroup) AddServer(coordConn zkhelper.Conn, s *Server, auth string) error {
	s.GroupId = sg.Id

	servers, err := sg.GetServers(coordConn)
	if err != nil {
		return errors.Trace(err)
	}
	var masterAddr string
	for _, server := range servers {
		if server.Type == SERVER_TYPE_MASTER {
			masterAddr = server.Addr
		}
	}

	// make sure there is only one master
	if s.Type == SERVER_TYPE_MASTER && len(masterAddr) > 0 {
		return errors.Trace(ErrNodeExists)
	}

	// if this group has no server. auto promote this server to master
	if len(servers) == 0 {
		s.Type = SERVER_TYPE_MASTER
	}

	val, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}

	coordPath := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d/%s", sg.ProductName, sg.Id, s.Addr)
	_, err = zkhelper.CreateOrUpdate(coordConn, coordPath, string(val), 0, zkhelper.DefaultFileACLs(), true)

	// update servers
	servers, err = sg.GetServers(coordConn)
	if err != nil {
		return errors.Trace(err)
	}
	sg.Servers = servers

	if s.Type == SERVER_TYPE_MASTER {
		err = NewAction(coordConn, sg.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, sg, "", true)
		if err != nil {
			return errors.Trace(err)
		}
	} else if s.Type == SERVER_TYPE_SLAVE && len(masterAddr) > 0 {
		// send command slaveof to slave
		err := utils.SlaveOf(s.Addr, masterAddr, auth)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (sg *ServerGroup) GetServers(coordConn zkhelper.Conn) ([]*Server, error) {
	var ret []*Server
	root := fmt.Sprintf("/zk/reborn/db_%s/servers/group_%d", sg.ProductName, sg.Id)
	nodes, _, err := coordConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, node := range nodes {
		nodePath := root + "/" + node
		s, err := GetServer(coordConn, nodePath)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, s)
	}
	return ret, nil
}
