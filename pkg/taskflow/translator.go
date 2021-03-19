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
func translatorOracleIncrementRecord(
	engine *db.Engine,
	targetSchema string,
	transferTableMetaMap map[string]int, rowsResult []db.LogminerContent,
	logfileStartSCN int, jobQueue chan Job) error {
	startTime := time.Now()
	for _, rows := range rowsResult {
		r := rows
		// 如果 sqlRedo 存在记录则继续处理，不存在记录则报错
		if r.SQLRedo == "" {
			return fmt.Errorf("does not meet expectations [oracle sql redo is be null], please check")
		}

		// 筛选过滤 Oracle Redo SQL
		// 1、数据同步只同步 INSERT/DELETE/UPDATE DML以及只同步 truncate table/ drop table 限定 DDL
		// 2、根据元数据表 table_increment_meta 对应表已经同步写入得 SCN SQL 记录,过滤 Oracle 提交记录 SCN 号，过滤,防止重复写入
		if r.SCN > transferTableMetaMap[strings.ToUpper(r.TableName)] {
			if r.Operation == util.DDLOperation {
				splitDDL := strings.Split(r.SQLRedo, ` `)
				ddl := fmt.Sprintf("%s %s", splitDDL[0], splitDDL[1])
				if strings.ToUpper(ddl) == util.DropTableOperation {
					// 处理 drop table marvin8 AS "BIN$vVWfliIh6WfgU0EEEKzOvg==$0"
					r.SQLRedo = strings.Split(strings.ToUpper(r.SQLRedo), "AS")[0]
				}
			}

			// 移除引号
			sqlRedo := util.ReplaceQuotesString(r.SQLRedo)
			// 移除分号
			sqlRedo = util.ReplaceSpecifiedString(sqlRedo, ";", "")

			var sqlUndo string
			if r.SQLUndo != "" {
				sqlUndo = util.ReplaceQuotesString(r.SQLUndo)
				sqlUndo = util.ReplaceSpecifiedString(sqlUndo, ";", "")
				sqlUndo = util.ReplaceSpecifiedString(sqlUndo, fmt.Sprintf("%s.", r.SegOwner), fmt.Sprintf("%s.", strings.ToUpper(targetSchema)))
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

			// 注册任务到 Job 队列

			lp := &IncrementPayload{
				Engine:         engine,
				GlobalSCN:      logfileStartSCN,
				SourceTableSCN: r.SCN,
				SourceSchema:   r.SegOwner,
				SourceTable:    r.TableName,
				TargetSchema:   strings.ToUpper(targetSchema),
				TargetTable:    r.TableName,
				OracleRedo:     sqlRedo,
				MySQLRedo:      mysqlRedo,
				Operation:      r.Operation,
				OperationType:  operationType}

			zlog.Logger.Info("translator oracle payload", zap.String("payload", lp.Marshal()))

			jobQueue <- Job{Task: lp}

		} else {
			// 如果 source_table_scn 小于 global_scn 说明，该表一直在当前日志文件内未发现 DML 事务变更
			// global_scn 表示日志文件起始 SCN 号
			// 更新增量元数据表 SCN 位点信息
			if err := engine.UpdateTableIncrementMetaOnlyGlobalSCNRecord(
				r.SegOwner,
				r.TableName, logfileStartSCN); err != nil {
				return err
			}
			// 考虑日志可能输出太多，忽略输出
			//zlog.Logger.Warn("filter oracle sql redo",
			//	zap.Int("source table scn", sourceTableSCN),
			//	zap.Int("target table scn", transferTableMetaMap[r["TABLE_NAME"]]),
			//	zap.String("source_schema", r["SEG_OWNER"]),
			//	zap.String("source_table", r["TABLE_NAME"]),
			//	zap.String("sql redo", r["SQL_REDO"]),
			//	zap.String("status", "update global scn and skip apply"))
		}

	}

	endTime := time.Now()
	zlog.Logger.Info("increment table log translator",
		zap.String("status", "success"),
		zap.Time("start time", startTime),
		zap.Time("end time", endTime),
		zap.String("cost time", time.Since(startTime).String()))

	return nil
}

// SQL 转换
// 1、INSERT INTO / REPLACE INTO
// 2、UPDATE / DELETE、INSERT INTO
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
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
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
