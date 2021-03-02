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
// INSERT INTO -> REPLACE INTO
// UPDATE -> DELETE/INSERT
func translatorOracleIncrementRecordBySafeMode(
	engine *db.Engine,
	targetSchema string,
	rowsResult []map[string]string, globalSCN int) ([]LogminerPayload, error) {
	var (
		lps []LogminerPayload
		sql []string
	)

	for _, r := range rowsResult {
		sourceTableSCN, err := strconv.Atoi(r["SCN"])
		if err != nil {
			return []LogminerPayload{}, err
		}
		// 移除引号
		sqlRedo := util.ReplaceQuotesString(r["SQL_REDO"])
		// 移除分号
		sqlRedo = util.ReplaceSpecifiedString(sqlRedo, ";", "")

		var lp LogminerPayload
		lp.Engine = engine
		lp.GlobalSCN = globalSCN
		lp.SourceTableSCN = sourceTableSCN
		lp.Operation = r["OPERATION"]
		lp.SourceSchema = r["SEG_OWNER"]
		lp.SourceTable = r["SEG_NAME"]
		lps = append(lps, lp)
		if r["OPERATION"] == "INSERT" {
			translatorIncrementInsertDML(sqlRedo, targetSchema, lp)
		}
		if r["OPERATION"] == "DELETE" {
			translatorIncrementDeleteDML(sqlRedo, targetSchema, lp)
		}
		if r["OPERATION"] == "UPDATE" {
			translatorIncrementUpdateDML(sqlRedo, targetSchema, lp)
		}
		if r["OPERATION"] == "DDL" {
			splitDDL := strings.Split(r["SQL_REDO"], "")
			ddl := fmt.Sprintf("%s %s", splitDDL[0], splitDDL[1])
			targetTableName := strings.Split(splitDDL[2], ".")[1]
			if strings.ToUpper(ddl) == "DROP TABLE" {
				// 比如: drop table marvin.marvin7
				targetSchemaTbl := fmt.Sprintf("%s.%s", targetSchema, targetTableName)
				sql = append(sql, fmt.Sprintf("%s %s", ddl, targetSchemaTbl))
			}
			if strings.ToUpper(ddl) == "TRUNCATE TABLE" {
				// 比如: truncate table marvin.marvin7
				targetSchemaTbl := fmt.Sprintf("%s.%s", targetSchema, targetTableName)
				sql = append(sql, fmt.Sprintf("%s %s", ddl, targetSchemaTbl))
			}
			lp.TargetSchema = targetSchema
			lp.TargetTable = targetTableName
			lp.SQLRedo = sql
		}

	}
	return lps, nil
}

func translatorIncrementInsertDML(sqlRedo, targetSchema string, lp LogminerPayload) {
	var (
		data        map[string]interface{}
		before      map[string]interface{}
		sql         []string
		targetTable string
	)
	data = make(map[string]interface{})
	before = make(map[string]interface{})
	splitInsert := strings.Split(sqlRedo, "")
	insert := fmt.Sprintf("%s %s", splitInsert[0], splitInsert[1])
	if strings.ToUpper(insert) == "INSERT INTO" {
		// 比如：INSERT INTO MARVIN.MARVIN1 (ID,NAME) VALUES (1,'marvin')
		replaceSQL := util.ReplaceSpecifiedString(sqlRedo, insert, "REPLACE INTO")
		// 替换 schema以及表名
		targetTable = strings.Split(splitInsert[2], ".")[1]
		targetSchemaTbl := fmt.Sprintf("%s.%s", targetSchema, targetTable)
		r := util.ReplaceSpecifiedString(replaceSQL, splitInsert[2], targetSchemaTbl)
		sql = append(sql, r)
		// 获取字段名
		columnStr := strings.TrimLeft(splitInsert[3], "(")
		columnStr = strings.TrimLeft(splitInsert[3], ")")
		columns := strings.Split(columnStr, ",")

		// 获取 value 值
		valueStr := strings.TrimLeft(splitInsert[5], "(")
		valueStr = strings.TrimLeft(splitInsert[5], ")")
		values := strings.Split(valueStr, ",")

		// 获取数据 data
		for i, col := range columns {
			data[col] = values[i]
		}
	}
	lp.TargetSchema = targetSchema
	lp.TargetTable = targetTable
	lp.SQLRedo = sql
	lp.Data = data
	lp.Before = before
}

func translatorIncrementUpdateDML(sqlRedo, targetSchema string, lp LogminerPayload) {
	var (
		data        map[string]interface{}
		before      map[string]interface{}
		sql         []string
		targetTable string
	)
	data = make(map[string]interface{})
	before = make(map[string]interface{})
	splitUP := strings.Split(sqlRedo, "")
	if strings.ToUpper(splitUP[0]) == "UPDATE" {
		// ORACLE 数据库同步需要开附加日志且表需要捕获字段列日志，Logminer 内容 UPDATE 语句会带所有字段信息
		//比如：UPDATE MARVIN.MARVIN1 SET ID = 2 , NAME = 'marvin' WHERE ID = 2 AND NAME = 'pty'
		targetTable = strings.Split(splitUP[1], ".")[1]
		targetSchemaTbl := fmt.Sprintf("%s.%s", targetSchema, targetTable)

		splitStr1 := util.ReSplit(sqlRedo, "WHERE")

		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s", targetSchemaTbl, splitStr1[1])

		// 获取修改前后字段名以及值
		splitWhereStr := util.ReSplit(splitStr1[1], "AND")
		splitSetStr := strings.Split(util.ReSplit(splitStr1[0], "SET")[1], ",")
		var (
			columns []string
			values  []string
		)
		// data
		for _, set := range splitSetStr {
			s := strings.Split(set, "=")
			data[s[0]] = s[1]
			columns = append(columns, s[0])
			values = append(values, s[1])
		}
		// before
		for _, set := range splitWhereStr {
			s := strings.Split(set, "=")
			before[s[0]] = s[1]
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", targetSchemaTbl, strings.Join(columns, ","), strings.Join(values, ","))

		// 先 delete 后 insert
		sql = append(sql, deleteSQL)
		sql = append(sql, insertSQL)
	}
	lp.TargetSchema = targetSchema
	lp.TargetTable = targetTable
	lp.SQLRedo = sql
	lp.Data = data
	lp.Before = before
}

func translatorIncrementDeleteDML(sqlRedo, targetSchema string, lp LogminerPayload) {
	var (
		data        map[string]interface{}
		before      map[string]interface{}
		sql         []string
		targetTable string
	)
	data = make(map[string]interface{})
	before = make(map[string]interface{})

	splitDels := strings.Split(sqlRedo, "")
	del := fmt.Sprintf("%s %s", splitDels[0], splitDels[1])
	if strings.ToUpper(del) == "DELETE FROM" {
		// 比如：DELETE FROM MARVIN.MARVIN7 WHERE ID = 5 and NAME = 'pyt'
		delSQL := util.ReplaceSpecifiedString(sqlRedo, del, "DELETE FROM")
		// 替换 schema以及表名
		targetTable = strings.Split(splitDels[2], ".")[1]
		targetSchemeTbl := fmt.Sprintf("%s.%s", targetSchema, targetTable)
		r := util.ReplaceSpecifiedString(delSQL, splitDels[2], targetSchemeTbl)
		sql = append(sql, r)

		// 获取字段名
		splitStr1 := util.ReSplit(sqlRedo, "WHERE")
		splitWhereStr := util.ReSplit(splitStr1[1], "AND")

		// before
		for _, set := range splitWhereStr {
			s := strings.Split(set, "=")
			before[s[0]] = s[1]
		}
	}
	lp.TargetSchema = targetSchema
	lp.TargetTable = targetTable
	lp.SQLRedo = sql
	lp.Data = data
	lp.Before = before
}
