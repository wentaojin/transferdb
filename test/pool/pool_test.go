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
	"golang.org/x/sync/errgroup"
	"testing"
	"time"
)

const (
	PoolSize = 1e4
	TaskNum  = 1e6
)

func BenchmarkPool(b *testing.B) {
	p := pool.NewPool(PoolSize,
		pool.WithResultCallback(func(result pool.Result) {
			if result.Err != nil {
				fmt.Printf("task attr [%s] task stage [%v] workerID [%v] job [%v] failed [%v].\n", result.Task.Attr, result.Task.Stage, result.WorkerID, result.Task.Job, result.Err)
			}
		}),
		pool.WithExecuteTask(func(t pool.Task) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}))

	defer p.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for num := 0; num < TaskNum; num++ {
			p.AddTask(pool.Task{})
		}
		p.Wait()
	}
	b.StopTimer()
}

func BenchmarkErrGroup(b *testing.B) {
	w := &errgroup.Group{}
	w.SetLimit(PoolSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for num := 0; num < TaskNum; num++ {
			w.Go(func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			})
		}
		err := w.Wait()
		if err != nil {
			fmt.Println(err)
		}
	}
	b.StopTimer()
}
