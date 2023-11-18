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
	"fmt"
	"github.com/wentaojin/transferdb/module/pool"
)

func main() {
	p := pool.NewPool(100,
		pool.WithTaskQueueSize(1024),
		pool.WithResultCallback(func(result pool.Result) {
			if result.Err != nil {
				fmt.Printf("task attr [%s] task stage [%v] workerID [%v] job [%v] failed [%v].\n", result.Task.Attr, result.Task.Stage, result.WorkerID, result.Task.Job, result.Err)
			} else {
				fmt.Printf("task attr [%s] task stage [%v] workerID [%v] job [%v] success.\n", result.Task.Attr, result.Task.Stage, result.WorkerID, result.Task.Job)
			}
		}),
		pool.WithExecuteTask(func(t pool.Task) error {
			if t.Job == 5 {
				fmt.Println("策四")
				return fmt.Errorf("task meet error")
			}
			return nil
		}))

	defer p.Release()

	for i := 0; i < 100; i++ {
		p.AddTask(pool.Task{
			Attr:  "test",
			Stage: "test",
			Job:   i,
		})
	}

	p.Wait()
}
