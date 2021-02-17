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
package taskflow

import (
	"log"
)

// 定义任务接口
// 所有实现接口即实现工作池
type Task interface {
	Do() error       // 任务处理
	Marshal() string // 任务序列化
}

// 定义工作结构体
type Job struct {
	Task Task
}

// 定义工作者
// 执行任务 job
type Worker struct {
	WorkerID   int           // 工作者 ID -> 线程 ID
	WorkerPool chan chan Job // 工人对象池
	JobQueue   chan Job      // 工作任务队列 ->管道取 Job
	Quit       chan bool
}

// 新建工作者
func NewWorker(workerID int, workerPool chan chan Job) *Worker {
	return &Worker{
		WorkerID:   workerID,
		WorkerPool: workerPool,
		JobQueue:   make(chan Job),
		Quit:       make(chan bool),
	}
}

// Start 方法启动工作程序的运行循环，侦听退出通道
func (w *Worker) Start() {
	go func() {
		for {
			// 注册任务队列到工作池
			w.WorkerPool <- w.JobQueue
			select {
			case job := <-w.JobQueue:
				// 从工作队列中取任务并处理
				if err := job.Task.Do(); err != nil {
					log.Fatalf("threadid: %d,error: %v", w.WorkerID, err)
					//zlog.Logger.Error("Start",
					//	zap.Int("threadID", w.WorkerID),
					//	zap.String("task", job.Task.Marshal()),
					//	zap.String("error", err.Error()))
				}
				log.Printf("threadid: %d, msg: %v", w.WorkerID, job.Task.Marshal())

			case <-w.Quit:
				// 接受退出信号量，停止任务
				// 监听信号量并退出处理
				//zlog.Logger.Warn("Stopping", zap.Int("threadID", w.WorkerID))
				return
			}
		}
	}()
}

// Stop 退出执行工作
func (w *Worker) Stop() {
	go func() {
		w.Quit <- true
	}()
}

// 工作分发者结构体
type Dispatcher struct {
	MaxWorker  int           // 最大并发数
	WorkerPool chan chan Job // 向工作分发者注册工作通道
	JobQueue   chan Job      // 任务队列
	Quit       chan bool     //退出信号
}

// 新建工作分发者,返回一个新调度分发对象
func NewDispatcher(maxWorker int, jobQueue chan Job) *Dispatcher {
	return &Dispatcher{
		JobQueue:   jobQueue,                       // 工作任务队列
		MaxWorker:  maxWorker,                      // 最大并发数 -> 工作者数
		WorkerPool: make(chan chan Job, maxWorker), // 将工作者放到工作池
		Quit:       make(chan bool),
	}
}

// 工作分发器 -> 运行
func (d *Dispatcher) Run() {
	// 初始化一定数量的工作者
	for i := 1; i <= d.MaxWorker; i++ {
		worker := NewWorker(i, d.WorkerPool)
		worker.Start()
	}
	// 监控任务调度发送给工作者
	go d.Dispatch()
}

// 退出任务调度发送工作
func (d *Dispatcher) Stop() {
	go func() {
		d.Quit <- true
	}()
}

// 任务调度分发
func (d *Dispatcher) Dispatch() {
	for {
		select {
		// 接收到任务
		case job := <-d.JobQueue:
			go func(job Job) {
				// 尝试获取可用的工作机会渠道
				// 这将阻塞直到工人空闲
				jobChan := <-d.WorkerPool
				// 分发任务 job 到工作 job 队列 channel
				jobChan <- job
			}(job)
		// 退出任务分发
		case <-d.Quit:
			return
		}
	}
}

// 初始化对象池
func InitWorkerPool(maxWorker, maxQueue int) chan Job {
	// 创建任务缓冲队列
	jobQueue := make(chan Job, maxQueue)
	// 初始化一个任务发送者,指定工作者数量
	dispatcher := NewDispatcher(maxWorker, jobQueue)
	// 一直运行任务调度
	dispatcher.Run()
	return jobQueue
}
