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
	"time"

	"github.com/thinkeridea/go-extend/exstrings"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

const (
	// 事务幂等性
	safeMode = true
)

// 全量数据导出导入期间，运行安全模式
// INSERT INTO 语句替换成 REPLACE INTO 语句
// 转换表数据 -> 全量任务
func translatorTableFullRecord(
	targetSchemaName, targetTableName, rowidSQL string, columnFields []string, rowsResult []interface{}, insertBatchSize int, safeMode bool) (string, [][]interface{}, string, [][]interface{}) {
	startTime := time.Now()
	columnCounts := len(columnFields)

	// bindVars
	actualBindVarsCounts := len(rowsResult)
	planBindVarsCounts := insertBatchSize * columnCounts

	// 计算可切分数，向下取整
	splitNums := int(math.Floor(float64(actualBindVarsCounts) / float64(planBindVarsCounts)))

	// 计算切分元素在 actualBindVarsCounts 位置
	planIntegerBinds := splitNums * planBindVarsCounts
	// 计算差值
	differenceBinds := actualBindVarsCounts - planIntegerBinds
	// 计算行数
	rowCounts := actualBindVarsCounts / columnCounts

	var (
		args1, args2 [][]interface{}
		prepareSQL1  string
		prepareSQL2  string
	)
	if differenceBinds == 0 {
		// batch 写入
		// 切分 batch
		args1 = utils.SplitMultipleSlice(rowsResult, int64(splitNums))

		// 计算占位符
		rowBatchCounts := actualBindVarsCounts / columnCounts / splitNums

		prepareSQL1 = utils.StringsBuilder(
			GenerateMySQLInsertSQLStatementPrefix(targetSchemaName, targetTableName, columnFields, safeMode),
			GenerateMySQLPrepareBindVarStatement(columnCounts, rowBatchCounts))
	} else {
		if planIntegerBinds > 0 {
			// batch 写入
			// 切分 batch
			args1 = utils.SplitMultipleSlice(rowsResult[:planIntegerBinds], int64(splitNums))

			// 计算占位符
			rowBatchCounts := planIntegerBinds / columnCounts / splitNums

			prepareSQL1 = utils.StringsBuilder(
				GenerateMySQLInsertSQLStatementPrefix(targetSchemaName, targetTableName, columnFields, safeMode),
				GenerateMySQLPrepareBindVarStatement(columnCounts, rowBatchCounts))
		}

		// 单次写入
		args2 = append(args2, rowsResult[planIntegerBinds:])
		// 计算占位符
		rowBatchCounts := differenceBinds / columnCounts

		prepareSQL2 = utils.StringsBuilder(
			GenerateMySQLInsertSQLStatementPrefix(targetSchemaName, targetTableName, columnFields, safeMode),
			GenerateMySQLPrepareBindVarStatement(columnCounts, rowBatchCounts))
	}
	endTime := time.Now()
	service.Logger.Info("single full table rowid data translator",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("rowid sql", rowidSQL),
		zap.Int("rowid rows", rowCounts),
		zap.Int("insert batch size", insertBatchSize),
		zap.Int("split sql nums", len(args1)+len(args2)),
		zap.Bool("write safe mode", safeMode),
		zap.String("cost", endTime.Sub(startTime).String()))

	return prepareSQL1, args1, prepareSQL2, args2
}

// SQL Prefix 语句
func GenerateMySQLInsertSQLStatementPrefix(targetSchemaName, targetTableName string, columns []string, safeMode bool) string {
	var prefixSQL string
	column := utils.StringsBuilder(" (", strings.Join(columns, ","), ")")
	if safeMode {
		prefixSQL = utils.StringsBuilder(`REPLACE INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)

	} else {
		prefixSQL = utils.StringsBuilder(`INSERT INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
	}
	return prefixSQL
}

// SQL Prepare 语句
func GenerateMySQLPrepareBindVarStatement(columns, bindVarBatch int) string {
	var (
		bindVars []string
		bindVar  []string
	)

	for i := 0; i < columns; i++ {
		bindVar = append(bindVar, "?")
	}

	singleBindVar := utils.StringsBuilder("(", exstrings.Join(bindVar, ","), ")")
	for i := 0; i < bindVarBatch; i++ {
		bindVars = append(bindVars, singleBindVar)
	}

	return exstrings.Join(bindVars, ",")
}

// 替换 Oracle 事务 SQL
// ORACLE 数据库同步需要开附加日志且表需要捕获字段列日志，Logminer 内容 UPDATE/DELETE/INSERT 语句会带所有字段信息
func translatorAndApplyOracleIncrementRecord(
	engine *service.Engine,
	sourceTableName string,
	targetSchema string,
	rowsResult []service.LogminerContent, taskQueue chan IncrPayload) error {

	startTime := time.Now()
	service.Logger.Info("oracle table increment log apply start",
		zap.String("mysql schema", targetSchema),
		zap.String("table", sourceTableName),
		zap.Time("start time", startTime))

	for _, rows := range rowsResult {
		// 如果 sqlRedo 存在记录则继续处理，不存在记录则报错
		if rows.SQLRedo == "" {
			return fmt.Errorf("does not meet expectations [oracle sql redo is be null], please check")
		}

		if rows.Operation == utils.DDLOperation {
			service.Logger.Info("translator oracle payload", zap.String("ORACLE DDL", rows.SQLRedo))
		}

		// 移除引号
		rows.SQLRedo = utils.ReplaceQuotesString(rows.SQLRedo)
		// 移除分号
		rows.SQLRedo = utils.ReplaceSpecifiedString(rows.SQLRedo, ";", "")

		if rows.SQLUndo != "" {
			rows.SQLUndo = utils.ReplaceQuotesString(rows.SQLUndo)
			rows.SQLUndo = utils.ReplaceSpecifiedString(rows.SQLUndo, ";", "")
			rows.SQLUndo = utils.ReplaceSpecifiedString(rows.SQLUndo,
				utils.StringsBuilder(rows.SegOwner, "."),
				utils.StringsBuilder(strings.ToUpper(targetSchema), "."))
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
		lp := IncrPayload{
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
	service.Logger.Info("oracle table increment log apply finished",
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
	case stmt.Operation == utils.UpdateOperation:
		operationType = utils.UpdateOperation
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
			deleteSQL = utils.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table)
		} else {
			deleteSQL = utils.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table, ` `, stmt.WhereExpr)
		}

		var (
			values []string
		)
		for _, col := range stmt.Columns {
			values = append(values, stmt.Data[col].(string))
		}
		insertSQL := utils.StringsBuilder(`REPLACE INTO `, stmt.Schema, ".", stmt.Table,
			"(",
			strings.Join(stmt.Columns, ","),
			")",
			` VALUES `,
			"(",
			strings.Join(values, ","),
			")")

		sqls = append(sqls, deleteSQL)
		sqls = append(sqls, insertSQL)

	case stmt.Operation == utils.InsertOperation:
		operationType = utils.InsertOperation
		stmt.Schema = targetSchema

		var values []string

		for _, col := range stmt.Columns {
			values = append(values, stmt.Data[col].(string))
		}
		replaceSQL := utils.StringsBuilder(`REPLACE INTO `, stmt.Schema, ".", stmt.Table,
			"(",
			strings.Join(stmt.Columns, ","),
			")",
			` VALUES `,
			"(",
			strings.Join(values, ","),
			")")

		sqls = append(sqls, replaceSQL)

	case stmt.Operation == utils.DeleteOperation:
		operationType = utils.DeleteOperation
		stmt.Schema = targetSchema
		var deleteSQL string

		if stmt.WhereExpr == "" {
			deleteSQL = utils.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table)
		} else {
			deleteSQL = utils.StringsBuilder(`DELETE FROM `, stmt.Schema, ".", stmt.Table, ` `, stmt.WhereExpr)
		}

		sqls = append(sqls, deleteSQL)

	case stmt.Operation == utils.TruncateOperation:
		operationType = utils.TruncateTableOperation
		stmt.Schema = targetSchema

		truncateSQL := utils.StringsBuilder(`TRUNCATE TABLE `, stmt.Schema, ".", stmt.Table)
		sqls = append(sqls, truncateSQL)

	case stmt.Operation == utils.DropOperation:
		operationType = utils.DropTableOperation
		stmt.Schema = targetSchema

		dropSQL := utils.StringsBuilder(`DROP TABLE `, stmt.Schema, ".", stmt.Table)

		sqls = append(sqls, dropSQL)
	}
	return sqls, operationType, nil
}
