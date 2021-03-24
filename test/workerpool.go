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
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func main() {
	startTime := time.Now()
	numOfWorkers := 3

	var (
		done    = make(chan bool)
		tasks   = make(chan Task, 2)
		results = make(chan Result, 2)
	)
	go GetResult(done, results)
	go allocate(tasks)
	go CreateWorkerPool(numOfWorkers, tasks, results)
	// 必须在allocate()和getResult()之后创建工作池
	<-done
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}

func allocate(jobQueue chan Task) {
	for i := 0; i < 10; i++ {
		randnum := rand.Intn(999)
		task := Task{i, randnum}
		jobQueue <- task
	}
	//close(jobQueue)
}

type Task struct {
	id      int
	randnum int
}

type Result struct {
	task   Task
	status bool
}

func (p *Task) Run() error {
	if p.id == 2 {
		return fmt.Errorf("meet error")
	} else {
		fmt.Println(p.id)
	}
	return nil
}

// 序列化
func (p *Task) Marshal() string {
	b, err := json.Marshal(&p)
	if err != nil {
		log.Error("error", zap.Error(err))
	}
	return string(b)
}

func CreateWorkerPool(numOfWorkers int, jobQueue chan Task, resultQueue chan Result) {
	var wg sync.WaitGroup
	for i := 0; i < numOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, jobQueue, resultQueue)
	}
	wg.Wait()
	close(resultQueue)
}

func GetResult(done chan bool, resultQueue chan Result) {
	for result := range resultQueue {
		if !result.status {
			log.Fatal("task record",
				zap.String("payload", result.task.Marshal()))
		}
	}
	done <- true
}

func worker(wg *sync.WaitGroup, jobQueue chan Task, resultQueue chan Result) {
	defer wg.Done()
	for job := range jobQueue {
		if err := job.Run(); err != nil {
			result := Result{
				task:   job,
				status: false,
			}
			resultQueue <- result
		}
		result := Result{
			task:   job,
			status: true,
		}
		resultQueue <- result
	}
}
