/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package pool

import (
	"context"
	"github.com/wentaojin/transferdb/common"
	"sync"
	"time"
)

type IPool interface {
	// AddTask adds a task to the pool
	AddTask(t Task)
	// Wait waits for all tasks to be dispatched and completed.
	Wait()
	// Release releases the pool and all its workers.
	Release()
	// RunningWorkerCount returns the number of running workers.
	RunningWorkerCount() int
	// GetPoolWorkerCount returns the number of workers.
	GetPoolWorkerCount() int
}

type Task struct {
	Attr  string
	Stage string
	Job   interface{}
}

type Result struct {
	Task     Task
	WorkerID int
	Err      error
}

type pool struct {
	maxWorkers int
	// workerStack represent worker stack, used to judge current task all worker whether done
	workerStack []int
	// workers represents worker do nums
	workers []*worker
	// tasks are added to this channel first, then dispatched to workers. Default buffer size is 1 million.
	taskQueue chan Task
	// Set by WithTaskQueueSize(), used to set the size of the task queue. Default is 1024.
	taskQueueSize int
	// Set by WithResultCallback(), used to handle the result of a task. Default is nil.
	resultCallback func(r Result)
	// Set by WithRetryCount(), used to retry the error of a task, Default is 1
	retryCount int
	// Set by WithExecuteTimeout(), used to set a timeout for a task. Default is 0, which means no timeout.
	executeTimeout time.Duration
	// Set by WithExecuteTask(), used to represents the worker need process task.
	executeTaskFn func(t Task) error
	ctx           context.Context
	// cancel is used to cancel the context. It is called when Release() is called.
	cancel context.CancelFunc

	lock sync.Locker
	// Conditional signals are a type of blocking wait between multiple goroutines. sync.Cond can be used to wait and notify goroutine mechanisms so that they can wait or continue execution under specific conditions
	cond *sync.Cond

	// adjustInterval is the interval to adjust the number of workers. Default is 1 second.
	adjustInterval time.Duration
}

// NewPool creates a new pool of workers.
func NewPool(maxWorkers int, opts ...Option) IPool {
	cancelCtx, cancel := context.WithCancel(context.Background())
	p := &pool{
		maxWorkers:     maxWorkers,
		retryCount:     0,
		taskQueue:      nil,
		taskQueueSize:  common.ChannelBufferSize,
		resultCallback: nil,
		lock:           new(sync.Mutex),
		adjustInterval: 1 * time.Second,

		executeTaskFn: nil,
		ctx:           cancelCtx,
		cancel:        cancel,
	}
	// options
	for _, opt := range opts {
		opt(p)
	}

	p.taskQueue = make(chan Task, p.taskQueueSize)
	p.workers = make([]*worker, p.maxWorkers)
	p.workerStack = make([]int, p.maxWorkers)

	if p.cond == nil {
		p.cond = sync.NewCond(p.lock)
	}

	// Create workers with the minimum number
	for i := 0; i < p.maxWorkers; i++ {
		w := newWorker()
		p.workers[i] = w
		p.workerStack[i] = i
		w.start(p, i)
	}

	// task dispatch worker
	go p.dispatch()
	return p
}

func (p *pool) AddTask(t Task) {
	p.taskQueue <- t
}

func (p *pool) Wait() {
	for {
		p.lock.Lock()
		workerStackLen := len(p.workerStack)
		p.lock.Unlock()
		if len(p.taskQueue) == 0 && workerStackLen == len(p.workers) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// RunningWorkerCount returns the number of workers that are currently working.
func (p *pool) RunningWorkerCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.workers) - len(p.workerStack)
}

// GetPoolWorkerCount returns the number of workers in the pool.
func (p *pool) GetPoolWorkerCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.workers)
}

func (p *pool) Release() {
	close(p.taskQueue)
	p.cancel()
	p.cond.L.Lock()
	for len(p.workerStack) != p.maxWorkers {
		p.cond.Wait()
	}
	p.cond.L.Unlock()
	for _, w := range p.workers {
		close(w.taskQueue)
	}
	p.workers = nil
	p.workerStack = nil
}

// dispatch tasks to workers.
func (p *pool) dispatch() {
	for t := range p.taskQueue {
		p.cond.L.Lock()
		for len(p.workerStack) == 0 {
			p.cond.Wait()
		}
		p.cond.L.Unlock()
		workerIndex := p.popWorker()
		p.workers[workerIndex].taskQueue <- t
	}
}

func (p *pool) popWorker() int {
	p.lock.Lock()
	workerIndex := p.workerStack[len(p.workerStack)-1]
	p.workerStack = p.workerStack[:len(p.workerStack)-1]
	p.lock.Unlock()
	return workerIndex
}

func (p *pool) pushWorker(workerIndex int) {
	p.lock.Lock()
	p.workerStack = append(p.workerStack, workerIndex)
	p.lock.Unlock()
	p.cond.Signal()
}
