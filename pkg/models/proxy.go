// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
)

const (
	PROXY_STATE_ONLINE       = "online"
	PROXY_STATE_OFFLINE      = "offline"
	PROXY_STATE_MARK_OFFLINE = "mark_offline"
)

type ProxyInfo struct {
	ID           string `json:"id"`
	Addr         string `json:"addr"`
	LastEvent    string `json:"last_event"`
	LastEventTs  int64  `json:"last_event_ts"`
	State        string `json:"state"`
	Description  string `json:"description"`
	DebugVarAddr string `json:"debug_var_addr"`
	Pid          int    `json:"pid"`
	StartAt      string `json:"start_at"`
}

func (p *ProxyInfo) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[ProxyInfo](%+v)", *p)
}

func (p *ProxyInfo) Ops() (int64, error) {
	m, err := p.DebugVars()
	if err != nil {
		return -1, errors.Trace(err)
	}

	if v, ok := m["router"]; ok {
		if vv, ok := v.(map[string]interface{})["ops"]; ok {
			return int64(vv.(float64)), nil
		}
	}

	return 0, nil
}

func (p *ProxyInfo) DebugVars() (map[string]interface{}, error) {
	resp, err := http.Get("http://" + p.DebugVarAddr + "/debug/vars")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	m := map[string]interface{}{}
	if err = json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, errors.Trace(err)
	}

	return m, nil
}

func GetProxyPath(productName string) string {
	return fmt.Sprintf("/zk/reborn/db_%s/proxy", productName)
}

func CreateProxyInfo(coordConn zkhelper.Conn, productName string, pi *ProxyInfo) (string, error) {
	data, err := json.Marshal(pi)
	if err != nil {
		return "", errors.Trace(err)
	}

	dir := GetProxyPath(productName)
	zkhelper.CreateRecursive(coordConn, dir, "", 0, zkhelper.DefaultDirACLs())
	return coordConn.Create(path.Join(dir, pi.ID), data, zk.FlagEphemeral, zkhelper.DefaultFileACLs())
}

func GetProxyFencePath(productName string) string {
	return fmt.Sprintf("/zk/reborn/db_%s/fence", productName)
}

func CreateProxyFenceNode(coordConn zkhelper.Conn, productName string, pi *ProxyInfo) (string, error) {
	return zkhelper.CreateRecursive(coordConn, path.Join(GetProxyFencePath(productName), pi.Addr), "",
		0, zkhelper.DefaultFileACLs())
}

func ProxyList(coordConn zkhelper.Conn, productName string, filter func(*ProxyInfo) bool) ([]ProxyInfo, error) {
	ret := make([]ProxyInfo, 0)
	root := GetProxyPath(productName)
	proxies, _, err := coordConn.Children(root)
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return nil, errors.Trace(err)
	}

	for _, proxyName := range proxies {
		pi, err := GetProxyInfo(coordConn, productName, proxyName)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if filter == nil || filter(pi) == true {
			ret = append(ret, *pi)
		}
	}

	return ret, nil
}

func GetFenceProxyMap(coordConn zkhelper.Conn, productName string) (map[string]bool, error) {
	children, _, err := coordConn.Children(GetProxyFencePath(productName))
	if err != nil {
		if err.Error() == zk.ErrNoNode.Error() {
			return make(map[string]bool), nil
		} else {
			return nil, errors.Trace(err)
		}
	}

	m := make(map[string]bool, len(children))
	for _, fenceNode := range children {
		m[fenceNode] = true
	}

	return m, nil
}

var ErrUnknownProxyStatus = errors.New("unknown status, should be (online offline)")

func SetProxyStatus(coordConn zkhelper.Conn, productName string, proxyName string, status string) error {
	if status != PROXY_STATE_ONLINE && status != PROXY_STATE_MARK_OFFLINE && status != PROXY_STATE_OFFLINE {
		return errors.Errorf("%v, %s", ErrUnknownProxyStatus, status)
	}

	p, err := GetProxyInfo(coordConn, productName, proxyName)
	if err != nil {
		return errors.Trace(err)
	}

	// check slot status before setting proxy online
	if status == PROXY_STATE_ONLINE {
		slots, err := Slots(coordConn, productName)
		if err != nil {
			return errors.Trace(err)
		}

		for _, slot := range slots {
			if slot.State.Status != SLOT_STATUS_ONLINE {
				return errors.Errorf("slot %v is not online", slot)
			}
			if slot.GroupId == INVALID_ID {
				return errors.Errorf("slot %v has invalid group id", slot)
			}
		}
	}

	p.State = status
	b, _ := json.Marshal(p)

	_, err = coordConn.Set(path.Join(GetProxyPath(productName), proxyName), b, -1)
	if err != nil {
		return errors.Trace(err)
	}

	if status == PROXY_STATE_MARK_OFFLINE {
		// wait for the proxy down
		for {
			_, _, c, err := coordConn.GetW(path.Join(GetProxyPath(productName), proxyName))
			if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}

			<-c

			info, err := GetProxyInfo(coordConn, productName, proxyName)
			log.Info("mark_offline, check proxy status:", proxyName, info, err)
			if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
				log.Info("shutdown proxy successful")
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}

			if info.State == PROXY_STATE_OFFLINE {
				log.Info("proxy:", proxyName, "offline success!")
				return nil
			}
		}
	}

	return nil
}

func GetProxyInfo(coordConn zkhelper.Conn, productName string, proxyName string) (*ProxyInfo, error) {
	var pi ProxyInfo
	data, _, err := coordConn.Get(path.Join(GetProxyPath(productName), proxyName))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := json.Unmarshal(data, &pi); err != nil {
		return nil, errors.Trace(err)
	}

	return &pi, nil
}
