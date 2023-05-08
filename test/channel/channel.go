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
	"golang.org/x/sync/errgroup"
	"strings"
)

func main() {
	f()
}

func f() {
	g1 := &errgroup.Group{}
	g1.SetLimit(10)

	for i := 0; i < 1; i++ {
		m := i
		g1.Go(func() error {
			err := It(newm(m))
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := g1.Wait(); err != nil {
		panic(err)
	}
}

type marvin struct {
	m  int
	ts chan []map[int]string
	t  chan string
}

func newm(m int) *marvin {
	t := make(chan string, 5)
	ts := make(chan []map[int]string, 5)

	return &marvin{
		m:  m,
		ts: ts,
		t:  t,
	}
}

func (m *marvin) read() error {
	r(m.ts)
	close(m.ts)
	return nil
}

func (m *marvin) trans() error {
	for dataC := range m.ts {
		for _, dMap := range dataC {
			// 按字段名顺序遍历获取对应值
			var (
				rowsTMP []string
			)

			for _, column := range []int{1, 5, 10, 20} {
				if val, ok := dMap[column]; ok {
					rowsTMP = append(rowsTMP, val)
				}
			}

			// csv 文件行数据输入
			m.t <- strings.Join(rowsTMP, "|+|")
		}
	}

	close(m.t)

	return nil
}

func (m *marvin) apply() error {
	for dataC := range m.t {
		if !strings.EqualFold(dataC, "") {
			fmt.Println(dataC)
		}
	}
	return nil
}

func r(ts chan []map[int]string) {
	// 临时数据存放
	var rowsTMP []map[int]string
	rowsMap := make(map[int]string)

	for i := 0; i < 12; i++ {

		rowsMap[i] = "test"

		// 临时数组
		rowsTMP = append(rowsTMP, rowsMap)

		// 清空
		rowsMap = make(map[int]string)

		// batch 批次
		if len(rowsTMP) == 5 {
			ts <- rowsTMP
			// 数组清空
			rowsTMP = make([]map[int]string, 0)
		}
	}

	// 非 batch 批次
	if len(rowsTMP) > 0 {
		ts <- rowsTMP
	}
}

type t interface {
	read() error
	trans() error
	apply() error
}

func It(t t) error {
	g := &errgroup.Group{}

	g.Go(func() error {
		err := t.trans()
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := t.apply()
		if err != nil {
			return err
		}
		return nil
	})

	err := t.read()
	if err != nil {
		return err
	}

	err = g.Wait()
	if err != nil {
		return err
	}
	return nil
}
