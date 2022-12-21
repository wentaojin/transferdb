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
package o2m

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"sync"
)

type IncrTask struct {
	Ctx            context.Context `json:"-"`
	GlobalSCN      uint64          `json:"global_scn"`
	SourceTableSCN uint64          `json:"source_table_scn"`
	SourceSchema   string          `json:"source_schema"`
	SourceTable    string          `json:"source_table"`
	TargetSchema   string          `json:"target_schema"`
	TargetTable    string          `json:"target_table"`
	Operation      string          `json:"operation"`
	OracleRedo     string          `json:"oracle_redo"` // Oracle SQL
	MySQLRedo      []string        `json:"mysql_redo"`  // MySQL 待执行 SQL
	OperationType  string          `json:"operation_type"`
	MySQL          *mysql.MySQL    `json:"-"`
	MetaDB         *meta.Meta      `json:"-"`
}

type IncrResult struct {
	Task   IncrTask
	Status bool
}

// 应用当前日志文件中所有记录
func applyOracleIncrRecord(metaDB *meta.Meta, mysqlDB *mysql.MySQL, cfg *config.Config, logminerMap map[string][]logminer) error {
	g := &errgroup.Group{}
	g.SetLimit(cfg.AllConfig.ApplyThreads)

	for tableName, lcs := range logminerMap {
		rowsResult := lcs
		tbl := tableName
		g.Go(func() error {
			if len(rowsResult) > 0 {
				var (
					done        = make(chan bool)
					taskQueue   = make(chan IncrTask, cfg.AllConfig.WorkerQueue)
					resultQueue = make(chan IncrResult, cfg.AllConfig.WorkerQueue)
				)
				// 获取增量执行结果
				go getIncrResult(done, resultQueue)

				// 转换捕获内容以及数据应用
				go func(mysql *mysql.MySQL, tbl, targetSchema string, rowsResult []logminer, taskQueue chan IncrTask) {
					defer func() {
						if err := recover(); err != nil {
							zap.L().Fatal("translatorAndApplyOracleIncrementRecord",
								zap.String("schema", cfg.MySQLConfig.SchemaName),
								zap.String("table", tbl),
								zap.Error(fmt.Errorf("%v", err)))
						}
					}()
					if err := translateAndAddOracleIncrRecord(
						metaDB,
						mysql,
						tbl,
						targetSchema,
						rowsResult, taskQueue); err != nil {
						return
					}
				}(mysqlDB, tbl, cfg.MySQLConfig.SchemaName, rowsResult, taskQueue)

				// 必须在任务分配和获取结果后创建工作池
				go createWorkerPool(cfg.AllConfig.WorkerThreads, taskQueue, resultQueue)
				// 等待执行完成
				<-done

				return nil
			}
			zap.L().Warn("increment table log file logminer null data, transferdb will continue to capture",
				zap.String("mysql schema", cfg.MySQLConfig.SchemaName),
				zap.String("table", tbl),
				zap.String("status", "success"))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("logminerContentMap concurrency meet error: %v", err)
	}

	return nil
}

// 任务同步
func (p *IncrTask) IncrApply() error {
	// 数据写入并更新元数据表
	//zap.L().Info("increment applier sql", zap.String("sql", sql))
	if p.OperationType == common.MigrateOperationUpdate {
		// update 语句拆分 delete/replace 放一个事务内
		txn, err := p.MySQL.MySQLDB.BeginTx(p.Ctx, &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql redo [%v] transaction start falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
		}
		for _, sql := range p.MySQLRedo {
			if _, err = txn.ExecContext(p.Ctx, sql); err != nil {
				return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql [%v] transaction doing falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
			}
		}
		if err = txn.Commit(); err != nil {
			return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql [%v] transaction commit falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
		}
	} else {
		for _, sql := range p.MySQLRedo {
			_, err := p.MySQL.MySQLDB.ExecContext(p.Ctx, sql)
			if err != nil {
				return fmt.Errorf("single increment table [%s] data oracle redo [%v] insert mysql [%v] exec falied: %v", p.SourceTable, p.OracleRedo, p.MySQLRedo, err)
			}
		}
	}
	// 数据写入完毕，更新元数据 checkpoint 表
	// 如果同步中断，数据同步使用会以 global_scn_s 为准，也就是会进行重复消费
	if p.Operation == common.MigrateOperationDropTable {
		err := meta.NewCommonModel(p.MetaDB).DeleteIncrSyncMetaAndWaitSyncMeta(p.Ctx, &meta.IncrSyncMeta{
			DBTypeS:     common.TaskDBOracle,
			DBTypeT:     common.TaskDBMySQL,
			SchemaNameS: p.SourceSchema,
			TableNameS:  p.SourceTable,
		}, &meta.WaitSyncMeta{
			SchemaNameS: p.SourceSchema,
			TableNameS:  p.SourceTable,
			Mode:        common.AllO2MMode,
		})
		if err != nil {
			zap.L().Error("update table increment scn record failed",
				zap.String("task", p.String()),
				zap.Error(err))
			return err
		}
	} else {
		err := meta.NewIncrSyncMetaModel(p.MetaDB).UpdateIncrSyncMeta(p.Ctx, &meta.IncrSyncMeta{
			DBTypeS:     common.TaskDBOracle,
			DBTypeT:     common.TaskDBMySQL,
			SchemaNameS: p.SourceSchema,
			TableNameS:  p.SourceTable,
			GlobalScnS:  p.GlobalSCN,
			TableScnS:   p.SourceTableSCN,
		})
		if err != nil {
			zap.L().Error("update table increment scn record failed",
				zap.String("task", p.String()),
				zap.Error(err))
			return err
		}
	}
	return nil
}

// 序列化
func (p *IncrTask) String() string {
	b, err := json.Marshal(&p)
	if err != nil {
		zap.L().Error("marshal task to string",
			zap.String("string", string(b)),
			zap.Error(err))
	}
	return string(b)
}

func createWorkerPool(numOfWorkers int, jobQueue chan IncrTask, resultQueue chan IncrResult) {
	var wg sync.WaitGroup
	for i := 0; i < numOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, jobQueue, resultQueue)
	}
	wg.Wait()
	close(resultQueue)
}

func getIncrResult(done chan bool, resultQueue chan IncrResult) {
	for result := range resultQueue {
		if !result.Status {
			zap.L().Fatal("task increment table record",
				zap.String("payload", result.Task.String()))
		}
	}
	done <- true
}

func worker(wg *sync.WaitGroup, jobQueue chan IncrTask, resultQueue chan IncrResult) {
	defer wg.Done()
	for job := range jobQueue {
		if err := job.IncrApply(); err != nil {
			result := IncrResult{
				Task:   job,
				Status: false,
			}
			resultQueue <- result
		}
		result := IncrResult{
			Task:   job,
			Status: true,
		}
		resultQueue <- result
	}
}
