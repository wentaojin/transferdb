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

import "C"
import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/WentaoJin/transferdb/zlog"

	"go.uber.org/zap"
)

/*
	全量同步任务
*/
// 全量数据导出导入
func LoaderOracleTableFullRecordToMySQLByFullMode(cfg *config.CfgFile, engine *db.Engine) error {
	startTime := time.Now()
	zlog.Logger.Info("Welcome to transferdb", zap.String("config", cfg.String()))
	zlog.Logger.Info("all full table data loader start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	transferTableSlice, err := getTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [table_full_meta],重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateMySQLTableFullMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
	}

	fullTblSlice, _, err := engine.IsNotExistFullStageMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice)
	if err != nil {
		return err
	}

	// 全量同步前，获取 SCN
	globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}

	//根据配置文件生成同步表元数据 [table_full_meta]
	if err := generateTableFullTaskCheckpointMeta(cfg, engine, fullTblSlice, globalSCN); err != nil {
		return err
	}

	// 表同步任务开始
	if err := loaderOracleTableTask(cfg, engine, transferTableSlice); err != nil {
		return err
	}

	endTime := time.Now()
	zlog.Logger.Info("all full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func LoaderOracleTableFullRecordToMySQLByAllMode(cfg *config.CfgFile, engine *db.Engine) error {
	startTime := time.Now()
	zlog.Logger.Info("Welcome to transferdb", zap.String("config", cfg.String()))
	zlog.Logger.Info("all full table data loader start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	transferTableSlice, err := getTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}
	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [table_full_meta],重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateMySQLTableFullMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
	}

	fullTblSlice, incrementTblSlice, err := engine.IsNotExistFullStageMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice)
	if err != nil {
		return err
	}

	// 全量同步前，获取 SCN
	// 1、获取一致性 SCN 写入增量表元数据（用于增量同步起始点，可能存在重复执行，默认前 10 分钟 safe-mode）
	globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}

	//根据配置文件生成同步表元数据 [table_full_meta]
	if err := generateTableFullTaskCheckpointMeta(cfg, engine, fullTblSlice, globalSCN); err != nil {
		return err
	}

	// 表同步任务
	if err := loaderOracleTableTask(cfg, engine, transferTableSlice); err != nil {
		return err
	}

	// 全量任务结束，写入增量源数据表起始 SCN 号
	//根据配置文件生成同步表元数据 [table_increment_meta]
	if err = generateTableIncrementTaskCheckpointMeta(cfg, engine, incrementTblSlice, globalSCN); err != nil {
		return err
	}

	endTime := time.Now()
	zlog.Logger.Info("all full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func loaderOracleTableTask(cfg *config.CfgFile, engine *db.Engine, transferTableSlice []string) error {
	for _, table := range transferTableSlice {
		oracleRowIDSQL, err := getMySQLTableFullMetaRowIDSQL(cfg, engine, table)
		if err != nil {
			return err
		}

		startTime := time.Now()
		zlog.Logger.Info("single full table data loader start",
			zap.String("schema", cfg.SourceConfig.SchemaName))

		wp := workpool.New(cfg.FullConfig.TableThreads)
		for _, rowidSQL := range oracleRowIDSQL {
			sql := rowidSQL
			wp.Do(func() error {
				// 抽取 Oracle 数据
				columns, rowsResult, err := extractorTableFullRecord(engine, cfg.SourceConfig.SchemaName, table, sql)
				if err != nil {
					return err
				}

				if len(rowsResult) == 0 {
					return fmt.Errorf("oracle schema [%v] table [%v] data return null rows", cfg.SourceConfig.SchemaName, table)
				}

				// 转换 Oracle 数据 -> MySQL
				mysqlSQLSlice, err := translatorTableFullRecord(
					cfg.TargetConfig.SchemaName,
					table,
					columns,
					rowsResult,
					cfg.FullConfig.WorkerThreads,
					cfg.AppConfig.InsertBatchSize,
					true,
				)
				if err != nil {
					return err
				}

				// 应用 Oracle 数据 -> MySQL
				if err := applierTableFullRecord(cfg.TargetConfig.SchemaName, table, cfg.FullConfig.WorkerThreads, mysqlSQLSlice,
					engine); err != nil {
					return err
				}

				// 表数据同步成功，清理断点记录
				if err := clearMySQLTableFullMetaRowIDSQL(cfg, engine, table, sql); err != nil {
					return err
				}
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return err
		}

		endTime := time.Now()
		if !wp.IsDone() {
			zlog.Logger.Fatal("single full table data loader failed",
				zap.String("schema", cfg.SourceConfig.SchemaName),
				zap.String("table", table),
				zap.String("cost", endTime.Sub(startTime).String()))
		} else {
			zlog.Logger.Info("single full table data loader finished",
				zap.String("schema", cfg.SourceConfig.SchemaName),
				zap.String("table", table),
				zap.String("cost", endTime.Sub(startTime).String()))
		}
	}
	return nil
}

/*
	增量同步任务
*/
func SyncOracleTableIncrementRecordToMySQLByAllMode(cfg *config.CfgFile, engine *db.Engine, jobQueue chan Job) error {
	// 获取增量元数据表内所需同步表
	transferTableSlice, err := engine.GetMySQLTableIncrementMetaRecord(cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}

	// 获取增量所需得日志文件
	logFiles, err := getOracleTableIncrementRecordLogFile(cfg, engine)
	if err != nil {
		return err
	}
	for _, log := range logFiles {
		// 获取日志文件起始 SCN
		startSCN, err := strconv.Atoi(log["FIRST_CHANGE"])
		if err != nil {
			return err
		}

		// logminer 开启
		if err = engine.AddOracleLogminerlogFile(log["LOG_FILE"]); err != nil {
			return err
		}

		if err = engine.StartOracleLogminerStoredProcedure(startSCN); err != nil {
			return err
		}

		// 捕获数据
		rowsResult, err := extractorTableIncrementRecord(engine, cfg.SourceConfig.SchemaName, transferTableSlice)
		if err != nil {
			return err
		}

		// logminer 关闭
		if err = engine.EndOracleLogminerStoredProcedure(); err != nil {
			return err
		}

		// 转换捕获内容
		lps, err := translatorOracleIncrementRecordBySafeMode(engine, cfg.TargetConfig.SchemaName, rowsResult, startSCN)
		if err != nil {
			return err
		}
		// 任务注册到 job 对列
		for _, lp := range lps {
			// 注册任务到 Job 队列
			jobQueue <- Job{Task: &lp}
		}
	}
	return nil
}

func getOracleTableIncrementRecordLogFile(cfg *config.CfgFile, engine *db.Engine) ([]map[string]string, error) {
	// 获取增量表起始最小 SCN 号
	globalSCN, err := engine.GetMySQLTableIncrementMetaMinSCNTime(cfg.TargetConfig.MetaSchema)
	if err != nil {
		return []map[string]string{}, err
	}
	// 判断数据是在 archived log Or redo log
	// 如果 redoSCN 等于 0，说明数据在归档日志
	redoScn, err := engine.GetOracleRedoLogSCN(globalSCN)
	if err != nil {
		return []map[string]string{}, err
	}
	archivedScn, err := engine.GetOracleArchivedLogSCN(globalSCN)
	if err != nil {
		return []map[string]string{}, err
	}

	var (
		logFiles []map[string]string
	)
	// 获取所需挖掘的日志文件
	if redoScn == 0 {
		logFiles, err = engine.GetOracleArchivedLogFile(archivedScn)
		if err != nil {
			return logFiles, err
		}
	} else {
		logFiles, err = engine.GetOracleRedoLogFile(redoScn)
		if err != nil {
			return logFiles, err
		}
	}
	return logFiles, nil
}

type LogminerPayload struct {
	Engine         *db.Engine
	GlobalSCN      int                    `json:"global_scn"`
	SourceTableSCN int                    `json:"source_table_scn"`
	SourceSchema   string                 `json:"source_schema"`
	SourceTable    string                 `json:"source_table"`
	TargetSchema   string                 `json:"target_schema"`
	TargetTable    string                 `json:"target_table"`
	SQLRedo        []string               `json:"sql_redo"` // 经过转换幂等 SQL 语句
	Operation      string                 `json:"operation"`
	Data           map[string]interface{} `json:"data"`
	Before         map[string]interface{} `json:"before"`
}

// 任务同步
func (p *LogminerPayload) Do() error {
	zlog.Logger.Info("oracle table increment applier start",
		zap.String("config", p.Marshal()))

	// 数据写入
	if err := applierTableIncrementRecord(p.SQLRedo, p.Engine); err != nil {
		return err
	}

	// 数据写入完毕，更新元数据 checkpoint 表
	// 如果同步中断，数据同步使用会以 global_scn 为准，也就是会进行重复消费
	if err := p.Engine.UpdateTableIncrementMetaRecord(p.SourceSchema, p.SourceTable, p.GlobalSCN, p.SourceTableSCN); err != nil {
		return err
	}

	zlog.Logger.Info("oracle table increment applier success",
		zap.String("source-schema", p.SourceSchema),
		zap.String("source-table", p.SourceTable),
		zap.String("target-schema", p.TargetSchema),
		zap.String("target-schema", p.TargetTable),
	)
	return nil
}

// 序列化
func (p *LogminerPayload) Marshal() string {
	b, err := json.Marshal(&p)
	if err != nil {
		zlog.Logger.Error("MarshalTaskToString",
			zap.String("string", string(b)),
			zap.String("error", fmt.Sprintf("json marshal task failed: %v", err)))
	}
	return string(b)
}
