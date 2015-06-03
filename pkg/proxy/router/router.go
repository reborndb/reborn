// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"bufio"
	"bytes"
	"container/list"
	"io"
	"net"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/proxy/group"
	"github.com/reborndb/reborn/pkg/proxy/parser"
	"github.com/reborndb/reborn/pkg/proxy/redisconn"
	topo "github.com/reborndb/reborn/pkg/proxy/router/topology"

	"github.com/juju/errors"
	stats "github.com/ngaut/gostats"
	log "github.com/ngaut/logging"
)

const (
	DefaultReaderSize = 32 * 1024
	DefaultWiterSize  = 32 * 1024

	RedisConnReaderSize = 16 * 1024
	RedisConnWiterSize  = 16 * 1024

	PipelineResponseNum = 1000
	PipelineRequestNum  = 1000

	EventBusNum         = 1000
	MigrateKeyTimeoutMs = 30 * 1000
	PoolCapability      = 16
)

type Server struct {
	slots  [models.DEFAULT_SLOT_NUM]*Slot
	top    *topo.Topology
	evtbus chan interface{}
	reqCh  chan *PipelineRequest

	lastActionSeq int
	pi            models.ProxyInfo
	startAt       time.Time

	moper       *MultiOperator
	pools       *redisconn.Pools
	counter     *stats.Counters
	onSuicide   onSuicideFun
	bufferedReq *list.List
	conf        *Conf

	pipeConns map[string]*taskRunner //redis->taskrunner
}

func (s *Server) clearSlot(i int) {
	if !validSlot(i) {
		return
	}

	if s.slots[i] != nil {
		s.slots[i].dst = nil
		s.slots[i].migrateFrom = nil
		s.slots[i] = nil
	}
}

func (s *Server) stopTaskRunners() {
	wg := &sync.WaitGroup{}
	log.Warning("taskrunner count", len(s.pipeConns))
	wg.Add(len(s.pipeConns))
	for _, tr := range s.pipeConns {
		tr.in <- wg
	}
	wg.Wait()

	//remove all
	for k, _ := range s.pipeConns {
		delete(s.pipeConns, k)
	}
}

func (s *Server) dumpCounter() {
	for {
		time.Sleep(5 * time.Second)
		log.Info(s.counter.String())
	}
}

func (s *Server) fillSlot(i int, force bool) {
	if !validSlot(i) {
		return
	}

	if !force && s.slots[i] != nil { //check
		log.Fatalf("slot %d already filled, slot: %+v", i, s.slots[i])
	}

	s.clearSlot(i)

	slotInfo, groupInfo, err := s.top.GetSlotByIndex(i)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	slot := &Slot{
		slotInfo:  slotInfo,
		dst:       group.NewGroup(*groupInfo),
		groupInfo: groupInfo,
	}

	log.Infof("fill slot %d, force %v, %+v", i, force, slot.dst)

	if slot.slotInfo.State.Status == models.SLOT_STATUS_MIGRATE {
		//get migrate src group and fill it
		from, err := s.top.GetGroup(slot.slotInfo.State.MigrateStatus.From)
		if err != nil { //todo: retry ?
			log.Fatal(err)
		}
		slot.migrateFrom = group.NewGroup(*from)
	}

	s.slots[i] = slot
	s.counter.Add("FillSlot", 1)
}

func (s *Server) createTaskRunner(slot *Slot) error {
	dst := slot.dst.Master()
	if _, ok := s.pipeConns[dst]; !ok {
		tr, err := NewTaskRunner(dst, s.conf.NetTimeout)
		if err != nil {
			return errors.Errorf("create task runner failed, %v,  %+v, %+v", err, slot.dst, slot.slotInfo)
		} else {
			s.pipeConns[dst] = tr
		}
	}

	return nil
}

func (s *Server) createTaskRunners() {
	for _, slot := range s.slots {
		if err := s.createTaskRunner(slot); err != nil {
			log.Error(err)
			return
		}
	}
}

func (s *Server) handleMigrateState(slotIndex int, keys ...[]byte) error {
	shd := s.slots[slotIndex]
	if shd.slotInfo.State.Status != models.SLOT_STATUS_MIGRATE {
		return nil
	}

	if shd.migrateFrom == nil {
		log.Fatalf("migrateFrom not exist %+v", shd)
	}

	if shd.dst.Master() == shd.migrateFrom.Master() {
		log.Fatalf("the same migrate src and dst, %+v", shd)
	}

	redisConn, err := s.pools.GetConn(shd.migrateFrom.Master())
	if err != nil {
		return errors.Trace(err)
	}

	defer s.pools.PutConn(redisConn)

	err = writeMigrateKeyCmd(redisConn, shd.dst.Master(), MigrateKeyTimeoutMs, keys...)
	if err != nil {
		redisConn.Close()
		log.Errorf("migrate key %s error, from %s to %s, err:%v",
			string(keys[0]), shd.migrateFrom.Master(), shd.dst.Master(), err)
		return errors.Trace(err)
	}

	redisReader := redisConn.BufioReader()

	//handle migrate result
	for i := 0; i < len(keys); i++ {
		resp, err := parser.Parse(redisReader)
		if err != nil {
			log.Errorf("migrate key %s error, from %s to %s, err:%v",
				string(keys[i]), shd.migrateFrom.Master(), shd.dst.Master(), err)
			redisConn.Close()
			return errors.Trace(err)
		}

		result, err := resp.Bytes()

		log.Debug("migrate", string(keys[0]), "from", shd.migrateFrom.Master(), "to", shd.dst.Master(),
			string(result))

		if resp.Type == parser.ErrorResp {
			redisConn.Close()
			log.Error(string(keys[0]), string(resp.Raw), "migrateFrom", shd.migrateFrom.Master())
			return errors.New(string(resp.Raw))
		}
	}

	s.counter.Add("Migrate", int64(len(keys)))

	return nil
}

func (s *Server) sendBack(c *session, op []byte, keys [][]byte, resp *parser.Resp, result []byte) {
	c.pipelineSeq++
	pr := &PipelineRequest{
		op:    op,
		keys:  keys,
		seq:   c.pipelineSeq,
		backQ: c.backQ,
		req:   resp,
	}

	resp, err := parser.Parse(bufio.NewReader(bytes.NewReader(result)))
	//just send to backQ
	c.backQ <- &PipelineResponse{ctx: pr, err: err, resp: resp}
}

func (s *Server) redisTunnel(c *session) error {
	resp, op, keys, err := getRespOpKeys(c)
	if err != nil {
		return errors.Trace(err)
	}
	k := keys[0]

	opstr := strings.ToUpper(string(op))
	buf, next, err := filter(opstr, keys, c, s.conf.NetTimeout)
	if err != nil {
		if len(buf) > 0 { //quit command or error message
			s.sendBack(c, op, keys, resp, buf)
		}
		return errors.Trace(err)
	}

	start := time.Now()
	defer func() {
		recordResponseTime(s.counter, time.Since(start)/1000/1000)
	}()

	s.counter.Add(opstr, 1)
	s.counter.Add("ops", 1)
	if !next {
		s.sendBack(c, op, keys, resp, buf)
		return nil
	}

	if isMulOp(opstr) {
		if !isTheSameSlot(keys) { //can not send to redis directly
			var result []byte
			err := s.moper.handleMultiOp(opstr, keys, &result)
			if err != nil {
				return errors.Trace(err)
			}
			s.sendBack(c, op, keys, resp, result)
			return nil
		}
	}

	i := mapKey2Slot(k)

	//pipeline
	c.pipelineSeq++
	pr := &PipelineRequest{
		slotIdx: i,
		op:      op,
		keys:    keys,
		seq:     c.pipelineSeq,
		backQ:   c.backQ,
		req:     resp,
		wg:      &sync.WaitGroup{},
	}
	pr.wg.Add(1)

	s.reqCh <- pr
	pr.wg.Wait()

	return nil
}

func (s *Server) handleConn(c net.Conn) {
	log.Info("new connection", c.RemoteAddr())

	s.counter.Add("connections", 1)
	client := &session{
		Conn:        c,
		r:           bufio.NewReaderSize(c, DefaultReaderSize),
		w:           bufio.NewWriterSize(c, DefaultWiterSize),
		CreateAt:    time.Now(),
		backQ:       make(chan *PipelineResponse, PipelineResponseNum),
		closeSignal: &sync.WaitGroup{},
	}
	client.closeSignal.Add(1)

	go client.WritingLoop()

	var err error
	defer func() {
		client.closeSignal.Wait() //waiting for writer goroutine

		if err != nil { //todo: fix this ugly error check
			if GetOriginError(err.(*errors.Err)).Error() != io.EOF.Error() {
				log.Warningf("close connection %v, %v", client, errors.ErrorStack(err))
			} else {
				log.Infof("close connection by eof %v", client)
			}
		} else {
			log.Infof("close connection %v", client)
		}

		s.counter.Add("connections", -1)
	}()

	for {
		err = s.redisTunnel(client)
		if err != nil {
			close(client.backQ)
			return
		}
		client.Ops++
	}
}

func (s *Server) OnSlotRangeChange(param *models.SlotMultiSetParam) {
	log.Warningf("slotRangeChange %+v", param)
	if !validSlot(param.From) || !validSlot(param.To) {
		log.Errorf("invalid slot number, %+v", param)
		return
	}

	for i := param.From; i <= param.To; i++ {
		switch param.Status {
		case models.SLOT_STATUS_OFFLINE:
			s.clearSlot(i)
		case models.SLOT_STATUS_ONLINE:
			s.fillSlot(i, true)
		default:
			log.Errorf("can not handle status %v", param.Status)
		}
	}
}

func (s *Server) OnGroupChange(groupId int) {
	log.Warning("group changed", groupId)

	for i, slot := range s.slots {
		if slot.slotInfo.GroupId == groupId {
			s.fillSlot(i, true)
		}
	}
}

func (s *Server) registerSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-c
		log.Info("ctrl-c or SIGTERM found, mark offline server")
		done := make(chan error)
		s.evtbus <- &killEvent{done: done}
		<-done
	}()
}

func (s *Server) Run() {
	log.Infof("listening %s on %s", s.conf.Proto, s.conf.Addr)
	listener, err := net.Listen(s.conf.Proto, s.conf.Addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Warning(errors.ErrorStack(err))
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) responseAction(seq int64) {
	log.Info("send response", seq)
	err := s.top.DoResponse(int(seq), &s.pi)
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

func (s *Server) getProxyInfo() models.ProxyInfo {
	//todo:send request to evtbus, and get response
	var pi = s.pi
	return pi
}

func (s *Server) getActionObject(seq int, target interface{}) {
	act := &models.Action{Target: target}
	err := s.top.GetActionWithSeqObject(int64(seq), act)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("%+v", act)
}

func (s *Server) checkAndDoTopoChange(seq int) bool {
	act, err := s.top.GetActionWithSeq(int64(seq))
	if err != nil { //todo: error is not "not exist"
		log.Fatal(errors.ErrorStack(err), "action seq", seq)
	}

	if !needResponse(act.Receivers, s.pi) { //no need to response
		return false
	}

	log.Warningf("action %v receivers %v", seq, act.Receivers)

	s.stopTaskRunners()

	switch act.Type {
	case models.ACTION_TYPE_SLOT_MIGRATE, models.ACTION_TYPE_SLOT_CHANGED,
		models.ACTION_TYPE_SLOT_PREMIGRATE:
		slot := &models.Slot{}
		s.getActionObject(seq, slot)
		s.fillSlot(slot.Id, true)
	case models.ACTION_TYPE_SERVER_GROUP_CHANGED:
		serverGroup := &models.ServerGroup{}
		s.getActionObject(seq, serverGroup)
		s.OnGroupChange(serverGroup.Id)
	case models.ACTION_TYPE_SERVER_GROUP_REMOVE:
		//do not care
	case models.ACTION_TYPE_MULTI_SLOT_CHANGED:
		param := &models.SlotMultiSetParam{}
		s.getActionObject(seq, param)
		s.OnSlotRangeChange(param)
	default:
		log.Fatalf("unknown action %+v", act)
	}

	s.createTaskRunners()

	return true
}

func (s *Server) handleMarkOffline() {
	s.top.Close(s.pi.ID)
	if s.onSuicide == nil {
		s.onSuicide = func() error {
			log.Errorf("suicide %+v, %s", s.pi, s.counter)
			os.Remove(s.conf.PidFile)
			os.Exit(0)
			return nil
		}
	}

	s.onSuicide()
}

func (s *Server) handleProxyCommand() {
	pi, err := s.top.GetProxyInfo(s.pi.ID)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if pi.State == models.PROXY_STATE_MARK_OFFLINE {
		s.handleMarkOffline()
	}
}

func (s *Server) processAction(e interface{}) {
	if strings.Index(GetEventPath(e), models.GetProxyPath(s.top.ProductName)) == 0 {
		//proxy event, should be order for me to suicide
		s.handleProxyCommand()
		return
	}

	//re-watch
	nodes, err := s.top.WatchChildren(models.GetWatchActionPath(s.top.ProductName), s.evtbus)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	seqs, err := models.ExtraSeqList(nodes)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if len(seqs) == 0 || !s.top.IsChildrenChangedEvent(e) {
		return
	}

	//get last pos
	index := -1
	for i, seq := range seqs {
		if s.lastActionSeq < seq {
			index = i
			break
		}
	}

	if index < 0 {
		return
	}

	actions := seqs[index:]
	for _, seq := range actions {
		exist, err := s.top.Exist(path.Join(s.top.GetActionResponsePath(seq), s.pi.ID))
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		if exist {
			continue
		}

		if s.checkAndDoTopoChange(seq) {
			s.responseAction(int64(seq))
		}
	}

	s.lastActionSeq = seqs[len(seqs)-1]
}

func (s *Server) dispatch(r *PipelineRequest) (success bool) {
	slotStatus := s.slots[r.slotIdx].slotInfo.State.Status
	if slotStatus != models.SLOT_STATUS_ONLINE && slotStatus != models.SLOT_STATUS_MIGRATE {
		return false
	}

	if err := s.handleMigrateState(r.slotIdx, r.keys...); err != nil {
		r.backQ <- &PipelineResponse{ctx: r, resp: nil, err: err}
		return true
	}

	tr, ok := s.pipeConns[s.slots[r.slotIdx].dst.Master()]
	if !ok {
		//try recreate taskrunner
		if err := s.createTaskRunner(s.slots[r.slotIdx]); err != nil {
			r.backQ <- &PipelineResponse{ctx: r, resp: nil, err: err}
			return true
		}

		tr = s.pipeConns[s.slots[r.slotIdx].dst.Master()]
	}
	tr.in <- r

	return true
}

func (s *Server) handleTopoEvent() {
	for {
		select {
		case r := <-s.reqCh:
			if s.slots[r.slotIdx].slotInfo.State.Status == models.SLOT_STATUS_PRE_MIGRATE {
				s.bufferedReq.PushBack(r)
				continue
			}

			for e := s.bufferedReq.Front(); e != nil; {
				next := e.Next()
				if s.dispatch(e.Value.(*PipelineRequest)) {
					s.bufferedReq.Remove(e)
				}
				e = next
			}

			if !s.dispatch(r) {
				log.Fatalf("should never happend, %+v, %+v", r, s.slots[r.slotIdx].slotInfo)
			}
		case e := <-s.evtbus:
			switch e.(type) {
			case *killEvent:
				s.handleMarkOffline()
				e.(*killEvent).done <- nil
			default:
				if s.top.IsSessionExpiredEvent(e) {
					log.Fatalf("session expired: %+v", e)
				}

				evtPath := GetEventPath(e)
				log.Infof("got event %s, %v, lastActionSeq %d", s.pi.ID, e, s.lastActionSeq)
				if strings.Index(evtPath, models.GetActionResponsePath(s.conf.ProductName)) == 0 {
					seq, err := strconv.Atoi(path.Base(evtPath))
					if err != nil {
						log.Warning(err)
					} else {
						if seq < s.lastActionSeq {
							log.Infof("ignore, lastActionSeq %d, seq %d", s.lastActionSeq, seq)
							continue
						}
					}

				}

				s.processAction(e)
			}
		}
	}
}

func (s *Server) waitOnline() {
	for {
		pi, err := s.top.GetProxyInfo(s.pi.ID)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		if pi.State == models.PROXY_STATE_MARK_OFFLINE {
			s.handleMarkOffline()
		}

		if pi.State == models.PROXY_STATE_ONLINE {
			s.pi.State = pi.State
			println("good, we are on line", s.pi.ID)
			log.Info("we are online", s.pi.ID)
			_, err := s.top.WatchNode(path.Join(models.GetProxyPath(s.top.ProductName), s.pi.ID), s.evtbus)
			if err != nil {
				log.Fatal(errors.ErrorStack(err))
			}

			return
		}

		select {
		case e := <-s.evtbus:
			switch e.(type) {
			case *killEvent:
				s.handleMarkOffline()
				e.(*killEvent).done <- nil
			}
		default: //otherwise ignore it
		}

		println("wait to be online ", s.pi.ID)
		log.Warning(s.pi.ID, "wait to be online")

		time.Sleep(3 * time.Second)
	}
}

func (s *Server) FillSlots() {
	for i := 0; i < models.DEFAULT_SLOT_NUM; i++ {
		s.fillSlot(i, false)
	}
}

func (s *Server) RegisterAndWait(wait bool) {
	_, err := s.top.CreateProxyInfo(&s.pi)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	_, err = s.top.CreateProxyFenceNode(&s.pi)
	if err != nil {
		log.Warning(errors.ErrorStack(err))
	}

	if wait {
		s.waitOnline()
	}
}

func NewServer(conf *Conf) *Server {
	log.Infof("start with configuration: %+v", conf)

	f := func(addr string) (*redisconn.Conn, error) {
		return redisconn.NewConnectionWithSize(addr, conf.NetTimeout, RedisConnReaderSize, RedisConnWiterSize)
	}

	s := &Server{
		conf:          conf,
		evtbus:        make(chan interface{}, EventBusNum),
		top:           topo.NewTopo(conf.ProductName, conf.CoordinatorAddr, conf.f, conf.Coordinator),
		counter:       stats.NewCounters("router"),
		lastActionSeq: -1,
		startAt:       time.Now(),
		moper:         newMultiOperator(conf.Addr),
		reqCh:         make(chan *PipelineRequest, PipelineRequestNum),
		pools:         redisconn.NewPools(PoolCapability, f),
		pipeConns:     make(map[string]*taskRunner),
		bufferedReq:   list.New(),
	}

	s.pi.ID = conf.ProxyID
	s.pi.State = models.PROXY_STATE_OFFLINE

	addr := conf.Addr
	addrs := strings.Split(addr, ":")
	if len(addrs) != 2 {
		log.Fatalf("bad addr %s", addr)
	}

	hname, err := os.Hostname()
	if err != nil {
		log.Fatal("get host name failed", err)
	}

	s.pi.Addr = hname + ":" + addrs[1]

	debugVarAddr := conf.HTTPAddr
	debugVarAddrs := strings.Split(debugVarAddr, ":")
	if len(debugVarAddrs) != 2 {
		log.Fatalf("bad debugVarAddr %s", debugVarAddr)
	}
	s.pi.DebugVarAddr = hname + ":" + debugVarAddrs[1]

	s.pi.Pid = os.Getpid()
	s.pi.StartAt = time.Now().String()

	log.Infof("proxy_info:%+v", s.pi)

	stats.Publish("evtbus", stats.StringFunc(func() string {
		return strconv.Itoa(len(s.evtbus))
	}))
	stats.Publish("startAt", stats.StringFunc(func() string {
		return s.startAt.String()
	}))

	s.RegisterAndWait(true)
	s.registerSignal()

	_, err = s.top.WatchChildren(models.GetWatchActionPath(conf.ProductName), s.evtbus)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	s.FillSlots()

	//start event handler
	go s.handleTopoEvent()
	go s.dumpCounter()

	log.Info("proxy start ok")

	return s
}
