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
	"log"
	"time"

	"github.com/WentaoJin/transferdb/pkg/taskflow"
)

func main() {
	for {
		testWorker()
	}

}

func testWorker() {
	for i := 1; i <= 4; i++ {
		jobQueue := taskflow.InitWorkerPool(2, 3)

		// 注册任务到 Job 队列
		jobQueue <- taskflow.Job{Task: &Test{Num: i}}
	}
	//time.Sleep(1 * time.Second)
	//执行结束,关闭管道
	//close(jobQueue)
}

type Test struct {
	Num int
}

// 实现 worker 的 Task 任务接口
func (t *Test) Run() error {
	//if t.Num == 3 {
	//	return fmt.Errorf("error")
	//}
	fmt.Printf("这是任务:%d号,执行时间为:%s \n", t.Num, fmt.Sprintf("%s", time.Now()))
	return nil
}

func (t *Test) Marshal() string {
	b, err := json.Marshal(&t)
	if err != nil {
		log.Fatalf("err: %v\n", err)
	}
	return string(b)
}
