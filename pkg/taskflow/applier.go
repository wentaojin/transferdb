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
	"fmt"
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/db"
)

// 表数据应用 -> 全量任务
func applierTableFullRecord(targetSchemaName, targetTableName string, workerThreads int, sqlSlice []string, engine *db.Engine) error {
	startTime := time.Now()
	zlog.Logger.Info("single full table data applier start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName))

	wp := workpool.New(workerThreads)
	for _, sql := range sqlSlice {
		s := sql
		wp.Do(func() error {
			_, err := engine.MysqlDB.Exec(s)
			if err != nil {
				return fmt.Errorf("single full table data bulk insert mysql [%s] falied:%v", sql, err)
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	endTime := time.Now()
	zlog.Logger.Info("single full table data applier finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
