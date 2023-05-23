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
package public

import (
	"github.com/wentaojin/transferdb/common"
)

type Job struct {
	ch   chan Logminer         // 定义 channel
	data map[string][]Logminer // 表级别存储数据的 slice
}

func (j *Job) Schedule() {
	// 从 channel 接收数据
	for c := range j.ch {
		if _, ok := j.data[common.StringUPPER(c.SourceTable)]; ok {
			j.data[common.StringUPPER(c.SourceTable)] = append(j.data[common.StringUPPER(c.SourceTable)], c)
		}
	}
}

func (j *Job) Close() {
	// 最后关闭 channel
	close(j.ch)
}

func (j *Job) AddData(data Logminer) {
	j.ch <- data // 发送数据到 channel
}

func NewScheduleJob(size int, lcMap map[string][]Logminer, done func()) *Job {
	s := &Job{
		ch:   make(chan Logminer, size),
		data: lcMap,
	}

	go func() {
		// 并发地 append 数据到 slice
		s.Schedule()
		done()
	}()
	return s
}
