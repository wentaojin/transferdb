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
	"strings"

	"github.com/WentaoJin/transferdb/db"
)

type LogminerData struct {
	ch   chan db.LogminerContent         // 用来 同步的channel
	data map[string][]db.LogminerContent // 存储数据的slice
}

func (s *LogminerData) Schedule() {
	// 从 channel 接收数据
	for i := range s.ch {
		if _, ok := s.data[strings.ToUpper(i.TableName)]; ok {
			s.data[strings.ToUpper(i.TableName)] = append(s.data[strings.ToUpper(i.TableName)], i)
		}
	}
}

func (s *LogminerData) Close() {
	// 最后关闭 channel
	close(s.ch)
}

func (s *LogminerData) AddData(v db.LogminerContent) {
	s.ch <- v // 发送数据到 channel
}

func NewScheduleJob(size int, lcMap map[string][]db.LogminerContent, done func()) *LogminerData {
	s := &LogminerData{
		ch:   make(chan db.LogminerContent, size),
		data: lcMap,
	}

	go func() {
		// 并发地 append 数据到 slice
		s.Schedule()
		done()
	}()

	return s
}
