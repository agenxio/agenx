package job

import (
    "sync"

    "github.com/satori/go.uuid"
    "github.com/queueio/sentry/utils/log"
)

type Executor struct {
    sync.RWMutex

    tasks map[uuid.UUID]Task
    wg    sync.WaitGroup
    done  chan struct{}
}

func New() *Executor {
    return &Executor{
        tasks: map[uuid.UUID]Task{},
        done:  make(chan struct{}),
    }
}

func (e *Executor) remove(t Task) {
    e.Lock()
    defer e.Unlock()

    delete(e.tasks, t.ID())
}

func (e *Executor) add(t Task) {
    e.Lock()
    defer e.Unlock()

    e.tasks[t.ID()] = t
}

func (e *Executor) Stop() {
    e.Lock()
    defer func() {
        e.Unlock()
        e.WaitForCompletion()
    }()

    close(e.done)

    for _, task := range e.tasks {
        go func(t Task) {
            t.Stop()
        }(task)
    }
}

func (e *Executor) WaitForCompletion() {
    e.wg.Wait()
}

func (e *Executor) Start(t Task) {
    e.Lock()
    defer e.Unlock()

    if !e.active() {
        return
    }

    e.wg.Add(1)
    go func() {
        defer func() {
            e.remove(t)
            e.wg.Done()
        }()

        e.add(t)

        err := t.Run()
        if err != nil {
            log.Err("Error running task: %v", err)
        }
    }()
}

func (e *Executor) Len() uint64 {
    e.RLock()
    defer e.RUnlock()

    return uint64(len(e.tasks))
}

func (e *Executor) active() bool {
    select {
    case <-e.done:
        return false
    default:
        return true
    }
}