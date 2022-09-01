package taskmanager

import (
    "context"
    "sync"
    "time"
)

type (
    Manager struct {
        taskChan      chan task
        deadline      time.Duration
        wg            sync.WaitGroup
        closeChan     chan bool
        retryInterval time.Duration
    }

    Config struct {
        Threshold     int    `json:",default=512"`
        Threads       int    `json:",default=512"`
        Deadline      string `json:",default=1h"`
        RetryInterval string `json:",default=5s"`
    }

    task struct {
        process   TaskFunc
        retry     bool
        ctx       context.Context
        ctxCancel context.CancelFunc
        callback  TaskCallback
    }

    TaskFunc     func() bool
    FailCallback func()
    Option       func(config *Config)
)

const (
    DefaultSize          = 100
    DefaultThreads       = 3
    DefaultDeadline      = 2 * time.Hour
    DefaultRetryInterval = 5 * time.Second
)

func NewManager(c Option) *Manager {
    mgr := new(Manager)
    err := mgr.Init(configHelper(c))
    if err != nil {
        return nil
    }
    return mgr
}

func validate(option Option) error {
    if option.DataCenterIdBits < 0 || option.DataCenterIdBits > 31 {
        return ErrInitial
    }
    if option.WorkerIdBits < 0 || option.WorkerIdBits > 31 {
        return ErrInitial
    }
    if option.SequenceBits < 0 || option.SequenceBits > 31 {
        return ErrInitial
    }

    if option.DataCenterIdBits+option.WorkerIdBits+option.SequenceBits >= 64 {
        return ErrInitial
    }

    if option.DataCenterIdBits > 0 {
        if dataCenterId > bitsToMax(option.DataCenterIdBits) {
            return ErrInitial
        }
    }
    if option.WorkerIdBits > 0 {
        if workerId > bitsToMax(option.WorkerIdBits) {
            return ErrInitial
        }
    }

    return nil
}
