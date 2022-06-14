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
	"github.com/wentaojin/transferdb/utils"
	"time"

	"github.com/wentaojin/transferdb/service"

	"github.com/xxjwxc/gowp/workpool"

	"go.uber.org/zap"
)

// 表数据应用 -> 全量任务
func applierTableFullRecord(engine *service.Engine,
	targetSchemaName, targetTableName, querySQL string, applyThreads int, columns, rowsResult []string) error {
	startTime := time.Now()
	zap.L().Info("single full table rowid data applier start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("query sql", querySQL))

	// batch 并发写
	if err := engine.BatchWriteMySQLTableData(
		targetSchemaName,
		targetTableName,
		GenerateMySQLInsertSQLStatementPrefix(targetSchemaName, targetTableName, columns, safeMode),
		rowsResult, applyThreads); err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("single full table rowid data applier finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("query sql", querySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

// 表数据应用 -> 增量任务
func applierTableIncrementRecord(p *IncrPayload) error {
	//zap.L().Info("increment applier sql", zap.String("sql", sql))
	if p.OperationType == utils.UpdateOperation {
		// update 语句拆分 delete/replace 放一个事务内
		txn, err := p.Engine.MysqlDB.Begin()
		if err != nil {
			return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql redo [%v] transaction start falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
		}
		for _, sql := range p.MySQLRedo {
			if _, err = txn.Exec(sql); err != nil {
				return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql [%v] transaction doing falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
			}
		}
		if err = txn.Commit(); err != nil {
			return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql [%v] transaction commit falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
		}
	} else {
		for _, sql := range p.MySQLRedo {
			_, err := p.Engine.MysqlDB.Exec(sql)
			if err != nil {
				return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql [%v] exec falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
			}
		}
	}
	// 数据写入完毕，更新元数据 checkpoint 表
	// 如果同步中断，数据同步使用会以 global_scn 为准，也就是会进行重复消费
	if err := p.Engine.UpdateTableIncrementMetaALLSCNRecord(p.SourceSchema, p.SourceTable, p.OperationType, p.GlobalSCN, p.SourceTableSCN); err != nil {
		zap.L().Error("update table increment scn record failed",
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
							zap.L().Fatal("translatorAndApplyOracleIncrementRecord",
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
			zap.L().Warn("increment table log file logminer null data, transferdb will continue to capture",
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
