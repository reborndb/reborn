package main

import (
	"container/list"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/ngaut/logging"
	"github.com/ngaut/zkhelper"
	"github.com/reborndb/reborn/pkg/models"
)

const (
	MAX_LOCK_TIMEOUT = 10 * time.Second
)

const (
	MIGRATE_TASK_PENDING   string = "pending"
	MIGRATE_TASK_MIGRATING string = "migrating"
	MIGRATE_TASK_FINISHED  string = "finished"
	MIGRATE_TASK_ERR       string = "error"
)

type SlotMigrator interface {
	Migrate(slot *models.Slot, fromGroup, toGroup int, task *MigrateTask, onProgress func(SlotMigrateProgress)) error
}

// check if migrate task is valid
type MigrateTaskCheckFunc func(t *MigrateTask) (bool, error)

// migrate task will store on coordinator
type MigrateManager struct {
	// pre migrate check functions
	preCheck     MigrateTaskCheckFunc
	pendingTasks *list.List
	runningTask  *MigrateTask
	// coordConn
	coordConn   zkhelper.Conn
	productName string
	lck         sync.RWMutex
}

// func getManagerPath(productName string) string {
// 	return fmt.Sprintf("/zk/reborn/db_%s/migrate_manager", productName)
// }

// func (m *MigrateManager) createNode() error {
// 	zkhelper.CreateRecursive(m.coordConn, fmt.Sprintf("/zk/reborn/db_%s/migrate_tasks", m.productName), "", 0, zkhelper.DefaultDirACLs())
// 	_, err := m.coordConn.Create(getManagerPath(m.productName),
// 		[]byte(""), zk.FlagEphemeral, zkhelper.DefaultFileACLs())
// 	if err != nil {
// 		log.Error("dashboard already exists! err: ", err)
// 	}
// 	return nil
// }

// func (m *MigrateManager) removeNode() error {
// 	return zkhelper.DeleteRecursive(m.coordConn, getManagerPath(m.productName), 0)
// }

func NewMigrateManager(coordConn zkhelper.Conn, pn string, preTaskCheck MigrateTaskCheckFunc) *MigrateManager {
	m := &MigrateManager{
		pendingTasks: list.New(),
		preCheck:     preTaskCheck,
		coordConn:    coordConn,
		productName:  pn,
	}
	// err := m.createNode()
	// if err != nil {
	// 	Fatal("another reborn-config exists? shut it down and try again")
	// }
	go m.loop()
	return m
}

func (m *MigrateManager) PostTask(t *MigrateTask) {
	m.lck.Lock()
	m.pendingTasks.PushBack(t)
	m.lck.Unlock()
}

func (m *MigrateManager) loop() error {
	for {
		m.lck.RLock()
		ele := m.pendingTasks.Front()
		m.lck.RUnlock()
		if ele == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// get pending task, and run
		m.lck.Lock()
		m.pendingTasks.Remove(ele)
		m.lck.Unlock()

		t := ele.Value.(*MigrateTask)
		t.coordConn = m.coordConn
		t.productName = m.productName

		m.runningTask = t
		if m.preCheck != nil {
			log.Info("start migration pre-check")
			if ok, err := m.preCheck(t); !ok {
				if err != nil {
					log.Error(err)
				}
				log.Error("migration pre-check error", t)
				continue
			}
		}
		log.Info("migration pre-check done")
		// do migrate
		err := t.run()
		if err != nil {
			log.Error(err)
		}

		// reset runningtask
		m.lck.Lock()
		m.runningTask = nil
		m.lck.Unlock()
	}
}

func (m *MigrateManager) RemovePendingTask(taskId string) error {
	m.lck.Lock()
	defer m.lck.Unlock()

	for e := m.pendingTasks.Front(); e != nil; e = e.Next() {
		t := e.Value.(*MigrateTask)
		if t.Id == taskId && t.Status == MIGRATE_TASK_PENDING {
			m.pendingTasks.Remove(e)
			return nil
		}
	}
	return errors.NotFoundf("task: %s", taskId)
}

func (m *MigrateManager) StopRunningTask() error {
	m.lck.Lock()
	defer m.lck.Unlock()

	err := m.runningTask.stop()
	if err != nil {
		return errors.Trace(err)
	}
	m.runningTask = nil
	return nil
}

func (m *MigrateManager) Tasks() []*MigrateTask {
	m.lck.RLock()
	defer m.lck.RUnlock()

	var tasks = make([]*MigrateTask, 0)
	for e := m.pendingTasks.Front(); e != nil; e = e.Next() {
		tasks = append(tasks, e.Value.(*MigrateTask))
	}

	return tasks
}

func (m *MigrateManager) getTaskById(taskId string) *MigrateTask {
	// if running task is target
	if m.runningTask.Id == taskId {
		return m.runningTask
	}

	for e := m.pendingTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(*MigrateTask).Id == taskId {
			return e.Value.(*MigrateTask)
		}
	}

	return nil
}
