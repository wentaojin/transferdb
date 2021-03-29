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
		sqlSlice = append(sqlSlice, util.StringsBuilder(sqlPrefix, " ", strings.Join(rowsResult, ",")))
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
			wp.DoWait(func() error {
				lock.Lock()
				sqlSlice = append(sqlSlice, util.StringsBuilder(sqlPrefix, " ", strings.Join(rows, ",")))
				lock.Unlock()
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return sqlSlice, err
		}
		if !wp.IsDone() {
			return sqlSlice, fmt.Errorf("translatorTableFullRecord concurrency meet error")
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
	column := util.StringsBuilder("(", strings.Join(columns, ","), ")")
	if safeMode {
		prepareSQL = util.StringsBuilder(`REPLACE INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)

	} else {
		prepareSQL = util.StringsBuilder(`INSERT INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
	}
	return prepareSQL
}

// 替换 Oracle 事务 SQL
// ORACLE 数据库同步需要开附加日志且表需要捕获字段列日志，Logminer 内容 UPDATE/DELETE/INSERT 语句会带所有字段信息
func translatorAndApplyOracleIncrementRecord(
	engine *db.Engine,
	sourceTableName string,
	targetSchema string,
	rowsResult []db.LogminerContent, taskQueue chan IncrementPayload) error {

	startTime := time.Now()
	zlog.Logger.Info("oracle table increment log apply start",
		zap.String("mysql schema", targetSchema),
		zap.String("table", sourceTableName),
		zap.Time("start time", startTime))

	for _, rows := range rowsResult {
		// 如果 sqlRedo 存在记录则继续处理，不存在记录则报错
		if rows.SQLRedo == "" {
			return fmt.Errorf("does not meet expectations [oracle sql redo is be null], please check")
		}

		if rows.Operation == util.DDLOperation {
			zlog.Logger.Info("translator oracle payload", zap.String("ORACLE DDL", rows.SQLRedo))
		}

		// 移除引号
		rows.SQLRedo = util.ReplaceQuotesString(rows.SQLRedo)
		// 移除分号
		rows.SQLRedo = util.ReplaceSpecifiedString(rows.SQLRedo, ";", "")

		if rows.SQLUndo != "" {
			rows.SQLUndo = util.ReplaceQuotesString(rows.SQLUndo)
			rows.SQLUndo = util.ReplaceSpecifiedString(rows.SQLUndo, ";", "")
			rows.SQLUndo = util.ReplaceSpecifiedString(rows.SQLUndo,
				util.StringsBuilder(rows.SegOwner, "."),
				util.StringsBuilder(strings.ToUpper(targetSchema), "."))
		}

		// 比如：INSERT INTO MARVIN.MARVIN1 (ID,NAME) VALUES (1,'marvin')
		// 比如：DELETE FROM MARVIN.MARVIN7 WHERE ID = 5 and NAME = 'pyt'
		// 比如：UPDATE MARVIN.MARVIN1 SET ID = 2 , NAME = 'marvin' WHERE ID = 2 AND NAME = 'pty'
		// 比如: drop table marvin.marvin7
		// 比如: truncate table marvin.marvin7
		mysqlRedo, operationType, err := translatorOracleToMySQLSQL(rows.SQLRedo, rows.SQLUndo, strings.ToUpper(targetSchema))
		if err != nil {
			return err
		}

		// 注册任务到 Job 队列
		lp := IncrementPayload{
			Engine:         engine,
			GlobalSCN:      rows.SCN, // 更新元数据 GLOBAL_SCN 至当前消费的 SCN 号
			SourceTableSCN: rows.SCN,
			SourceSchema:   rows.SegOwner,
			SourceTable:    rows.TableName,
			TargetSchema:   strings.ToUpper(targetSchema),
			TargetTable:    rows.TableName,
			OracleRedo:     rows.SQLRedo,
			MySQLRedo:      mysqlRedo,
			Operation:      rows.Operation,
			OperationType:  operationType}

		// 避免太多日志输出
		// zlog.Logger.Info("translator oracle payload", zap.String("payload", lp.Marshal()))
		taskQueue <- lp
	}

	endTime := time.Now()
	zlog.Logger.Info("oracle table increment log apply finished",
		zap.String("mysql schema", targetSchema),
		zap.String("table", sourceTableName),
		zap.String("status", "success"),
		zap.Time("start time", startTime),
		zap.Time("end time", endTime),
		zap.String("cost time", time.Since(startTime).String()))

	// 任务结束，关闭通道
	close(taskQueue)

	return nil
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
			deleteSQL = util.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table)
		} else {
			deleteSQL = util.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table, ` `, stmt.WhereExpr)
		}

		var (
			values []string
		)
		for _, col := range stmt.Columns {
			values = append(values, stmt.Data[col].(string))
		}
		insertSQL := util.StringsBuilder(`REPLACE INTO `, stmt.Schema, ".", stmt.Table,
			"(",
			strings.Join(stmt.Columns, ","),
			")",
			` VALUES `,
			"(",
			strings.Join(values, ","),
			")")

		sqls = append(sqls, deleteSQL)
		sqls = append(sqls, insertSQL)

	case stmt.Operation == util.InsertOperation:
		operationType = util.InsertOperation
		stmt.Schema = targetSchema

		var values []string

		for _, col := range stmt.Columns {
			values = append(values, stmt.Data[col].(string))
		}
		replaceSQL := util.StringsBuilder(`REPLACE INTO `, stmt.Schema, ".", stmt.Table,
			"(",
			strings.Join(stmt.Columns, ","),
			")",
			` VALUES `,
			"(",
			strings.Join(values, ","),
			")")

		sqls = append(sqls, replaceSQL)

	case stmt.Operation == util.DeleteOperation:
		operationType = util.DeleteOperation
		stmt.Schema = targetSchema
		var deleteSQL string

		if stmt.WhereExpr == "" {
			deleteSQL = util.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table)
		} else {
			deleteSQL = util.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table, ` `, stmt.WhereExpr)
		}

		sqls = append(sqls, deleteSQL)

	case stmt.Operation == util.TruncateOperation:
		operationType = util.TruncateTableOperation
		stmt.Schema = targetSchema

		truncateSQL := util.StringsBuilder(`TRUNCATE TABLE `, stmt.Schema, ".", stmt.Table)
		sqls = append(sqls, truncateSQL)

	case stmt.Operation == util.DropOperation:
		operationType = util.DropTableOperation
		stmt.Schema = targetSchema

		dropSQL := util.StringsBuilder(`DROP TABLE `, stmt.Schema, ".", stmt.Table)

		sqls = append(sqls, dropSQL)
	}
	return sqls, operationType, nil
}
