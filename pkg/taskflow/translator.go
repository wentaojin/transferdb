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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/util"
)

// 全量数据导出导入期间，运行安全模式
// INSERT INTO 语句替换成 REPLACE INTO 语句
// 转换表数据 -> 全量任务
func translatorTableFullRecord(
	targetSchemaName, targetTableName string,
	columns []string, rowsResult []string, workerThreads, insertBatchSize int, safeMode bool) ([]string, error) {
	startTime := time.Now()
	zlog.Logger.Info("single full table data translator start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName))

	var sqlSlice []string
	sqlPrefix := generateMySQLPrepareInsertSQLStatement(targetSchemaName, targetTableName, columns, safeMode)
	rowCounts := len(rowsResult)

	if rowCounts <= insertBatchSize {
		sqlSlice = append(sqlSlice, fmt.Sprintf("%s %s", sqlPrefix, strings.Join(rowsResult, ",")))
	} else {
		// 数据行按照 batch 拼接拆分
		// 向上取整，多切 batch，防止数据丢失
		splitsNums := math.Ceil(float64(rowCounts) / float64(insertBatchSize))
		multiBatchRows := util.SplitMultipleStringSlice(rowsResult, int64(splitsNums))

		// 保证并发 Slice Append 安全
		var lock sync.Mutex
		wp := workpool.New(workerThreads)
		for _, batchRows := range multiBatchRows {
			rows := batchRows
			wp.Do(func() error {
				lock.Lock()
				sqlSlice = append(sqlSlice, fmt.Sprintf("%s %s", sqlPrefix, strings.Join(rows, ",")))
				lock.Unlock()
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return sqlSlice, err
		}
	}
	endTime := time.Now()
	zlog.Logger.Info("single full table data translator finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return sqlSlice, nil
}

// 拼接 SQL 语句
func generateMySQLPrepareInsertSQLStatement(targetSchemaName, targetTableName string, columns []string, safeMode bool) string {
	var prepareSQL string
	column := fmt.Sprintf("(%s)", strings.Join(columns, ","))
	if safeMode {
		prepareSQL = fmt.Sprintf("REPLACE INTO %s.%s %s VALUES ",
			targetSchemaName,
			targetTableName,
			column,
		)
	} else {
		prepareSQL = fmt.Sprintf("INSERT INTO %s.%s %s VALUES ",
			targetSchemaName,
			targetTableName,
			column,
		)
	}
	return prepareSQL
}
