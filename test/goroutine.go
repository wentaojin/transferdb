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
	"strings"

	"github.com/wentaojin/transferdb/pkg/taskflow"
	"github.com/wentaojin/transferdb/service"

	"github.com/xxjwxc/gowp/workpool"
)

func main() {
	tableList := []string{"a", "b", "c"}

	var (
		lcMap map[string][]service.LogminerContent
		lc    []service.LogminerContent
	)
	lcMap = make(map[string][]service.LogminerContent)

	for _, table := range tableList {
		lcMap[strings.ToUpper(table)] = lc
	}
	fmt.Println(lcMap)

	c := make(chan struct{})
	// new 了这个 job 后，该 job 就开始准备从 channel 接收数据了
	s := taskflow.NewScheduleJob(1, lcMap, func() { c <- struct{}{} })

	data := []service.LogminerContent{
		{
			SCN:       0,
			SegOwner:  "c",
			TableName: "a",
			SQLRedo:   "",
			SQLUndo:   "",
			Operation: "",
		},
		{
			SCN:       1,
			SegOwner:  "c",
			TableName: "b",
			SQLRedo:   "",
			SQLUndo:   "",
			Operation: "",
		},
		{
			SCN:       3,
			SegOwner:  "c",
			TableName: "a",
			SQLRedo:   "",
			SQLUndo:   "",
			Operation: "",
		},
	}

	wp := workpool.New(3)
	for _, dt := range data {
		r := dt
		wp.DoWait(func() error {
			if r.SCN >= 1 {
				s.AddData(r)
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		fmt.Println(err)
	}
	s.Close()
	<-c
	fmt.Println(lcMap)
}
