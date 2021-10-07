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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/wentaojin/transferdb/pkg/check"
)

func main() {
	pwdDir, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
	}
	file, err := os.OpenFile(filepath.Join(pwdDir, "transferdb.sql"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	wr := &check.FileMW{Mutex: sync.Mutex{}, Writer: file}

	wp := workpool.New(10)
	for i := 0; i < 1000; i++ {
		// 变量替换，直接使用原变量会导致并发输出有问题
		variables := i
		fileWR := wr
		wp.Do(func() error {
			if _, err := fmt.Fprintln(fileWR, fmt.Sprintf("%v %d", time.Now(), variables)); err != nil {
				return err
			}
			return nil
		})
	}
	if err = wp.Wait(); err != nil {
		fmt.Println(err)
	}

	if !wp.IsDone() {
		fmt.Println("not done")
	}
}
