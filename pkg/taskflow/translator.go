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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WentaoJin/transferdb/db"

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

// 替换 Oracle 事务 SQL
// ORACLE 数据库同步需要开附加日志且表需要捕获字段列日志，Logminer 内容 UPDATE/DELETE/INSERT 语句会带所有字段信息
func translatorOracleIncrementRecord(engine *db.Engine, targetSchema string, rowsResult []map[string]string, globalSCN int, workerThreads int) ([]*IncrementPayload, error) {
	var (
		lps  []*IncrementPayload
		lock sync.Mutex
	)
	wp := workpool.New(workerThreads)
	for _, rows := range rowsResult {
		r := rows
		wp.Do(func() error {
			sourceTableSCN, err := strconv.Atoi(r["SCN"])
			if err != nil {
				return err
			}

			// 如果 sqlRedo 存在记录则继续处理，不存在记录则继续下一个
			if r["SQL_REDO"] == "" {
				return fmt.Errorf("does not meet expectations [oracle sql redo is be null], please check")
			}

			// 移除引号
			sqlRedo := util.ReplaceQuotesString(r["SQL_REDO"])
			// 移除分号
			sqlRedo = util.ReplaceSpecifiedString(sqlRedo, ";", "")

			zlog.Logger.Info("oracle sql redo", zap.String("sql", sqlRedo))

			var sqlUndo string
			if r["SQL_UNDO"] != "" {
				sqlUndo = util.ReplaceQuotesString(r["SQL_UNDO"])
				sqlUndo = util.ReplaceSpecifiedString(sqlUndo, ";", "")
				sqlUndo = util.ReplaceSpecifiedString(sqlUndo, fmt.Sprintf("%s.", r["SEG_OWNER"]), fmt.Sprintf("%s.", strings.ToUpper(targetSchema)))
			}

			// 比如：INSERT INTO MARVIN.MARVIN1 (ID,NAME) VALUES (1,'marvin')
			// 比如：DELETE FROM MARVIN.MARVIN7 WHERE ID = 5 and NAME = 'pyt'
			// 比如：UPDATE MARVIN.MARVIN1 SET ID = 2 , NAME = 'marvin' WHERE ID = 2 AND NAME = 'pty'
			// 比如: drop table marvin.marvin7
			// 比如: truncate table marvin.marvin7
			mysqlRedo, operationType, err := translatorOracleToMySQLSQL(sqlRedo, sqlUndo, strings.ToUpper(targetSchema))
			if err != nil {
				return err
			}

			// 并发 slice 安全
			lock.Lock()
			defer lock.Unlock()
			lps = append(lps, &IncrementPayload{
				Engine:         engine,
				GlobalSCN:      globalSCN,
				SourceTableSCN: sourceTableSCN,
				SourceSchema:   r["SEG_OWNER"],
				SourceTable:    r["TABLE_NAME"],
				TargetSchema:   strings.ToUpper(targetSchema),
				TargetTable:    r["TABLE_NAME"],
				OracleRedo:     sqlRedo,
				MySQLRedo:      mysqlRedo,
				Operation:      r["OPERATION"],
				OperationType:  operationType,
			})
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return lps, err
	}

	if !wp.IsDone() {
		return lps, fmt.Errorf("oracle schema translator increment table data failed")
	}
	return lps, nil
}

// SQL 转换
// 1、INSERT INTO / REPLACE INTO
// 2、UPDATE / DELETE、REPLACE INTO
func translatorOracleToMySQLSQL(oracleSQLRedo, oracleSQLUndo, targetSchema string) ([]string, string, error) {
	var (
		sqls          []string
		operationType string
	)
	astNode, err := parseSQL(oracleSQLRedo)
	if err != nil {
		return []string{}, operationType, fmt.Errorf("parse error: %v\n", err.Error())
	}

	stmt := extractStmt(astNode)
	switch {
	case stmt.Operation == util.UpdateOperation:
		operationType = util.UpdateOperation
		astUndoNode, err := parseSQL(oracleSQLUndo)
		if err != nil {
			return []string{}, operationType, fmt.Errorf("parse error: %v\n", err.Error())
		}
		undoStmt := extractStmt(astUndoNode)

		stmt.Data = undoStmt.Before
		for column, _ := range stmt.Before {
			stmt.Columns = append(stmt.Columns, strings.ToUpper(column))
		}

		var deleteSQL string
		stmt.Schema = targetSchema

		if stmt.WhereExpr == "" {
			deleteSQL = fmt.Sprintf("DELETE FROM %s.%s",
				stmt.Schema,
				stmt.Table)
		} else {
			deleteSQL = fmt.Sprintf("DELETE FROM %s.%s %s",
				stmt.Schema,
				stmt.Table,
				stmt.WhereExpr)
		}
		var (
			values []string
		)
		for _, col := range stmt.Columns {
			values = append(values, stmt.Data[col].(string))
		}
		insertSQL := fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)",
			stmt.Schema,
			stmt.Table,
			strings.Join(stmt.Columns, ","),
			strings.Join(values, ","))

		sqls = append(sqls, deleteSQL)
		sqls = append(sqls, insertSQL)

	case stmt.Operation == util.InsertOperation:
		operationType = util.InsertOperation
		stmt.Schema = targetSchema

		var values []string

		for _, col := range stmt.Columns {
			values = append(values, stmt.Data[col].(string))
		}
		replaceSQL := fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s)",
			stmt.Schema,
			stmt.Table,
			strings.Join(stmt.Columns, ","),
			strings.Join(values, ","))

		sqls = append(sqls, replaceSQL)

	case stmt.Operation == util.DeleteOperation:
		operationType = util.DeleteOperation
		stmt.Schema = targetSchema
		var deleteSQL string

		if stmt.WhereExpr == "" {
			deleteSQL = fmt.Sprintf("DELETE FROM %s.%s",
				stmt.Schema,
				stmt.Table)
		} else {
			deleteSQL = fmt.Sprintf("DELETE FROM %s.%s %s",
				stmt.Schema,
				stmt.Table,
				stmt.WhereExpr)
		}

		sqls = append(sqls, deleteSQL)

	case stmt.Operation == util.TruncateOperation:
		operationType = util.TruncateTableOperation
		stmt.Schema = targetSchema

		truncateSQL := fmt.Sprintf("TRUNCATE TABLE %s.%s",
			stmt.Schema,
			stmt.Table)

		sqls = append(sqls, truncateSQL)

	case stmt.Operation == util.DropOperation:
		operationType = util.DropTableOperation
		stmt.Schema = targetSchema

		dropSQL := fmt.Sprintf("DROP TABLE %s.%s",
			stmt.Schema,
			stmt.Table)

		sqls = append(sqls, dropSQL)
	}
	return sqls, operationType, nil
}
