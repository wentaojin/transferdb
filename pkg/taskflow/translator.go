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
	"strings"

	"github.com/WentaoJin/transferdb/util"
)

// 全量数据导出导入期间，运行安全模式
// INSERT INTO 语句替换成 REPLACE INTO 语句
func translatorTableFullRecord(schemaName, tableName string, columns []string, rows [][]string, insertBatchSize int, safeMode bool) []string {
	var (
		sqlSlice []string
		val      string
	)
	sql := generateMySQLPrepareInsertSQLStatement(schemaName, tableName, columns, safeMode)
	rowCounts := len(rows)
	if rowCounts <= insertBatchSize {
		for _, row := range rows {
			val += fmt.Sprintf("(%s),", strings.Join(row, ","))
		}
		sqlSlice = append(sqlSlice, fmt.Sprintf("%s%s", sql, util.TrimLastChar(val)))
	} else {
		// 数据行按照 batch 拼接拆分
		splitsNums := rowCounts / insertBatchSize
		multiBatchRows := util.SplitMultipleStringSlice(rows, int64(splitsNums))
		for _, batchRows := range multiBatchRows {
			for _, rows := range batchRows {
				val += fmt.Sprintf("(%s),", strings.Join(rows, ","))
			}
			sqlSlice = append(sqlSlice, fmt.Sprintf("%s%s", sql, util.TrimLastChar(val)))
		}
	}
	return sqlSlice
}

// 拼接 SQL 语句
func generateMySQLPrepareInsertSQLStatement(schemaName, tableName string, columns []string, safeMode bool) string {
	var prepareSQL string
	column := fmt.Sprintf("(%s)", strings.Join(columns, ","))
	if safeMode {
		prepareSQL = fmt.Sprintf("REPLACE INTO %s.%s %s VALUES ",
			schemaName,
			tableName,
			column,
		)
	} else {
		prepareSQL = fmt.Sprintf("INSERT INTO %s.%s %s VALUES ",
			schemaName,
			tableName,
			column,
		)
	}
	return prepareSQL
}
