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
package reverser

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/wentaojin/transferdb/service"
	"go.uber.org/zap"
)

type Job struct {
	Table            Table
	ReverseSQL       string
	CompatibilitySQL string
}

func (j *Job) String() string {
	jsonStr, _ := json.Marshal(j)
	return string(jsonStr)
}

func Produce(wp *workpool.WorkPool, tables []Table, jobChan chan Job) {
	for _, tblName := range tables {
		service.Logger.Info("reverse table rule",
			zap.String("rule", tblName.String()))
		// 变量替换，直接使用原变量会导致并发输出有问题
		tbl := tblName
		wp.DoWait(func() error {
			createSQL, compatibilitySQL, err := tbl.GenerateAndExecMySQLCreateSQL()
			if err != nil {
				return err
			}
			jobChan <- Job{
				Table:            tbl,
				ReverseSQL:       createSQL,
				CompatibilitySQL: compatibilitySQL,
			}
			return nil
		})
	}
}

func Consume(revFile, compFile *os.File, job chan Job) {
	for j := range job {
		if j.ReverseSQL != "" {
			_, err := fmt.Fprintln(revFile, j.ReverseSQL)
			if err != nil {
				service.Logger.Error("reverse table oracle to mysql failed",
					zap.String("type", "reverse sql"),
					zap.String("info", j.String()),
					zap.Error(fmt.Errorf("reverse table task failed, please clear and rerunning")),
					zap.Error(err))

				revFile.Close()
				return
			}
		}
		if j.CompatibilitySQL != "" {
			_, err := fmt.Fprintln(compFile, j.CompatibilitySQL)
			if err != nil {
				service.Logger.Error("reverse table oracle to mysql failed",
					zap.String("type", "compatibility sql"),
					zap.String("info", j.String()),
					zap.Error(fmt.Errorf("reverse table task failed, please clear and rerunning")),
					zap.Error(err))

				compFile.Close()
				return
			}
		}
	}
}
