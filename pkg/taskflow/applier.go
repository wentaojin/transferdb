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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"github.com/xxjwxc/gowp/workpool"

	"go.uber.org/zap"
)

// 表数据应用 -> 全量任务
func applierTableFullRecord(engine *service.Engine,
	targetSchemaName, targetTableName, rowidSQL string,
	sourceSchemaName, sourceTableName string,
	prepareSQL string,
	rows *sql.Rows,
	insertBatchSize int) error {
	startTime := time.Now()
	service.Logger.Info("single full table rowid data applier start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("rowid sql", rowidSQL))

	// Close Rows
	defer rows.Close()

	// 如果不存在数据记录，直接返回
	if !rows.Next() {
		service.Logger.Warn("oracle schema table rowid data return null rows, skip",
			zap.String("schema", sourceSchemaName),
			zap.String("table", sourceTableName),
			zap.String("sql", rowidSQL))
		return nil
	}

	var (
		err        error
		rowsResult []interface{}
	)

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// 用于判断字段值是数字还是字符
	var columnTypes []string
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	// 数据 Scan
	columns := len(cols)
	rawResult := make([][]byte, columns)
	dest := make([]interface{}, columns)
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	// 表行数读取
	batchBindVars := insertBatchSize * columns

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		for i, raw := range rawResult {
			// 注意 Oracle/Mysql NULL VS 空字符串区别
			// Oracle 空字符串与 NULL 归于一类，统一 NULL 处理 （is null 可以查询 NULL 以及空字符串值，空字符串查询无法查询到空字符串值）
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			// 按照 Oracle 特性来，转换同步统一转换成 NULL 即可，但需要注意业务逻辑中空字符串得写入，需要变更
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if raw == nil {
				rowsResult = append(rowsResult, sql.NullString{})
			} else if string(raw) == "" {
				rowsResult = append(rowsResult, sql.NullString{})
			} else {
				switch columnTypes[i] {
				case "int64":
					r, err := utils.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return err
					}
					rowsResult = append(rowsResult, r)
				case "uint64":
					r, err := utils.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return err
					}
					rowsResult = append(rowsResult, r)
				case "float32":
					r, err := utils.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return err
					}
					rowsResult = append(rowsResult, r)
				case "float64":
					r, err := utils.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return err
					}
					rowsResult = append(rowsResult, r)
				case "rune":
					r, err := utils.StrconvRune(string(raw))
					if err != nil {
						return err
					}
					rowsResult = append(rowsResult, r)
				default:
					ok := utils.IsNum(string(raw))
					if ok {
						r, err := decimal.NewFromString(string(raw))
						if err != nil {
							return err
						}
						if r.IsInteger() {
							r, err := utils.StrconvIntBitSize(string(raw), 64)
							if err != nil {
								return err
							}
							rowsResult = append(rowsResult, r)
						} else {
							r, err := utils.StrconvFloatBitSize(string(raw), 64)
							if err != nil {
								return err
							}
							rowsResult = append(rowsResult, r)
						}
					} else {
						rowsResult = append(rowsResult, string(raw))
					}
				}
			}
		}

		// batch 写入
		if len(rowsResult) == batchBindVars {
			_, err := engine.MysqlDB.Exec(prepareSQL, rowsResult...)
			if err != nil {
				return fmt.Errorf("single full table [%s.%s] prepare sql [%v] prepare args [%v] data bulk insert mysql falied: %v",
					targetSchemaName, targetTableName, prepareSQL, rowsResult, err)
			}
			// 数组清空
			rowsResult = rowsResult[0:0]
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// 单条写入
	// 计算占位符
	rowCounts := len(rowsResult)
	if rowCounts > 0 {
		rowBatchCounts := rowCounts / columns

		prepareSQL2 := utils.StringsBuilder(
			GenerateMySQLInsertSQLStatementPrefix(targetSchemaName, targetTableName, cols, safeMode),
			GenerateMySQLPrepareBindVarStatement(columns, rowBatchCounts))

		_, err = engine.MysqlDB.Exec(prepareSQL2, rowsResult...)
		if err != nil {
			return fmt.Errorf("single full table [%s.%s] prepare sql [%v] prepare args [%v] data bulk insert mysql falied: %v",
				targetSchemaName, targetTableName, prepareSQL2, rowsResult, err)
		}
	}

	endTime := time.Now()
	service.Logger.Info("single full table rowid data applier finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("rowid sql", rowidSQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

// 表数据应用 -> 增量任务
func applierTableIncrementRecord(p *IncrPayload) error {
	sql := strings.Join(p.MySQLRedo, ";")
	//zlog.Logger.Info("increment applier sql", zap.String("sql", sql))
	_, err := p.Engine.MysqlDB.Exec(sql)
	if err != nil {
		return fmt.Errorf("single increment table data insert mysql [%s] falied:%v", sql, err)
	}
	// 数据写入完毕，更新元数据 checkpoint 表
	// 如果同步中断，数据同步使用会以 global_scn 为准，也就是会进行重复消费
	if err := p.Engine.UpdateTableIncrementMetaALLSCNRecord(p.SourceSchema, p.SourceTable, p.OperationType, p.GlobalSCN, p.SourceTableSCN); err != nil {
		service.Logger.Error("update table increment scn record failed",
			zap.String("payload", p.Marshal()),
			zap.Error(err))
		return err
	}
	return nil
}

func applyOracleRedoIncrementRecord(cfg *service.CfgFile, engine *service.Engine, logminerContentMap map[string][]service.LogminerContent) error {
	// 应用当前日志文件中所有记录
	wp := workpool.New(cfg.AllConfig.ApplyThreads)
	for tableName, lcs := range logminerContentMap {
		rowsResult := lcs
		tbl := tableName
		wp.DoWait(func() error {
			if len(rowsResult) > 0 {
				var (
					done        = make(chan bool)
					taskQueue   = make(chan IncrPayload, cfg.AllConfig.WorkerQueue)
					resultQueue = make(chan IncrResult, cfg.AllConfig.WorkerQueue)
				)
				// 获取增量执行结果
				go GetIncrResult(done, resultQueue)

				// 转换捕获内容以及数据应用
				go func(engine *service.Engine, tbl, targetSchemaName string, rowsResult []service.LogminerContent, taskQueue chan IncrPayload) {
					defer func() {
						if err := recover(); err != nil {
							service.Logger.Fatal("translatorAndApplyOracleIncrementRecord",
								zap.String("schema", cfg.TargetConfig.SchemaName),
								zap.String("table", tbl),
								zap.Error(fmt.Errorf("%v", err)))
						}
					}()
					if err := translatorAndApplyOracleIncrementRecord(
						engine,
						tbl,
						targetSchemaName,
						rowsResult, taskQueue); err != nil {
						return
					}
				}(engine, tbl, cfg.TargetConfig.SchemaName, rowsResult, taskQueue)

				// 必须在任务分配和获取结果后创建工作池
				go CreateWorkerPool(cfg.AllConfig.WorkerThreads, taskQueue, resultQueue)
				// 等待执行完成
				<-done

				return nil
			}
			service.Logger.Warn("increment table log file logminer null data, transferdb will continue to capture",
				zap.String("mysql schema", cfg.TargetConfig.SchemaName),
				zap.String("table", tbl),
				zap.String("status", "success"))
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	if !wp.IsDone() {
		return fmt.Errorf("logminerContentMap concurrency meet error")
	}
	return nil
}
