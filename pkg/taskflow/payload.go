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

	// 表同步任务
	if err = loaderOracleTableTask(cfg, engine); err != nil {
		return err
	}

	endTime := time.Now()
	zlog.Logger.Info("all full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func loaderOracleTableFullRecordToMySQLByAllMode(cfg *config.CfgFile, engine *db.Engine) error {
	startTime := time.Now()
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
	// 1、获取一致性 SCN 写入增量表元数据
	globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}

	//根据配置文件生成同步表元数据 [table_full_meta]
	if err = generateTableFullTaskCheckpointMeta(cfg, engine, fullTblSlice, globalSCN); err != nil {
		return err
	}

	// 表同步任务
	if err = loaderOracleTableTask(cfg, engine); err != nil {
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

func loaderOracleTableTask(cfg *config.CfgFile, engine *db.Engine) error {
	transferTables, err := engine.GetMySQLTableFullMetaSchemaTableRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	for _, table := range transferTables {
		startTime := time.Now()
		zlog.Logger.Info("single full table data loader start",
			zap.String("schema", cfg.SourceConfig.SchemaName))

		oraRowIDSQL, err := engine.GetMySQLTableFullMetaRowIDRecord(cfg.SourceConfig.SchemaName, table)
		if err != nil {
			return err
		}
		wp := workpool.New(cfg.FullConfig.TableThreads)
		for _, rowidSQL := range oraRowIDSQL {
			sql := rowidSQL
			wp.Do(func() error {
				// 抽取 Oracle 数据
				columns, rowsResult, err := extractorTableFullRecord(engine, cfg.SourceConfig.SchemaName, table, sql)
				if err != nil {
					return err
				}

				if len(rowsResult) == 0 {
					zlog.Logger.Warn("oracle schema table rowid data return null rows, skip",
						zap.String("schema", cfg.SourceConfig.SchemaName),
						zap.String("table", table),
						zap.String("sql", sql))
					// 清理断点记录
					if err := engine.ClearMySQLTableFullMetaRecord(cfg.SourceConfig.SchemaName, table, sql); err != nil {
						return err
					}
					return nil
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
				if err := engine.ClearMySQLTableFullMetaRecord(cfg.SourceConfig.SchemaName, table, sql); err != nil {
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

			return fmt.Errorf("oracle schema [%s] single full table [%v] data loader failed",
				cfg.SourceConfig.SchemaName, table)
		}
		zlog.Logger.Info("single full table data loader finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("table", table),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}

/*
	增量同步任务
*/

func SyncOracleTableAllRecordToMySQLByAllMode(cfg *config.CfgFile, engine *db.Engine) error {
	zlog.Logger.Info("Welcome to transferdb", zap.String("config", cfg.String()))

	// 全量数据导出导入，初始化全量元数据表以及导入完成初始化增量元数据表
	// 如果下游数据库增量元数据表 table_increment_meta 存在记录，说明进行过全量，则跳过全量步骤，直接增量数据同步
	// 如果下游数据库增量元数据表 table_increment_meta 不存在记录，说明未进行过数据同步，则进行全量 + 增量数据同步
	isNotExist, err := engine.IsNotExistMySQLTableIncrementMetaRecord()
	if err != nil {
		return err
	}
	if isNotExist {
		if err := loaderOracleTableFullRecordToMySQLByAllMode(cfg, engine); err != nil {
			return err
		}
	}
	// 增量数据同步
	for {
		if err := syncOracleTableIncrementRecordToMySQLByAllMode(cfg, engine); err != nil {
			return err
		}
	}
}

func syncOracleTableIncrementRecordToMySQLByAllMode(cfg *config.CfgFile, engine *db.Engine) error {
	// 获取增量元数据表内所需同步表
	transferTableMetaSlice, transferTableMetaMap, err := engine.GetMySQLTableIncrementMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}

	// 获取增量所需得日志文件
	logFiles, err := getOracleTableIncrementRecordLogFile(cfg, engine)
	if err != nil {
		return err
	}

	zlog.Logger.Info("increment table log file get",
		zap.String("logfile", fmt.Sprintf("%v", logFiles)))

	// 遍历所有日志文件
	for _, log := range logFiles {
		zlog.Logger.Info("increment table log file logminer",
			zap.String("logfile", log["LOG_FILE"]))

		// 获取日志文件起始 SCN
		logfileStartSCN, err := strconv.Atoi(log["FIRST_CHANGE"])
		if err != nil {
			return err
		}

		// 获取 logminer query 起始 SCN
		logminerStartSCN, err := engine.GetMySQLTableIncrementMetaMinSourceTableSCNTime(cfg.TargetConfig.MetaSchema)
		if err != nil {
			return err
		}
		// logminer 运行
		if err = engine.AddOracleLogminerlogFile(log["LOG_FILE"]); err != nil {
			return err
		}

		if err = engine.StartOracleLogminerStoredProcedure(logfileStartSCN); err != nil {
			return err
		}

		// 捕获数据
		rowsResult, err := extractorTableIncrementRecord(
			engine,
			cfg.SourceConfig.SchemaName,
			transferTableMetaSlice,
			transferTableMetaMap,
			logfileStartSCN,
			logminerStartSCN)
		if err != nil {
			return err
		}

		// logminer 关闭
		if err = engine.EndOracleLogminerStoredProcedure(); err != nil {
			return err
		}

		if len(rowsResult) > 0 {
			// 转换捕获内容
			lps, err := translatorOracleIncrementRecord(engine, cfg.TargetConfig.SchemaName, rowsResult, logfileStartSCN, cfg.AllConfig.TranslatorThreads)
			if err != nil {
				return err
			}

			// 数据应用
			wp := workpool.New(cfg.AllConfig.SyncThreads)
			for _, lp := range lps {
				lpStruct := lp
				wp.Do(func() error {
					if err := lpStruct.Do(); err != nil {
						return err
					}
					return nil
				})
			}
			if err := wp.Wait(); err != nil {
				return err
			}
			if !wp.IsDone() {
				return fmt.Errorf("mysql schema applier increment table data failed")
			}
		} else {
			zlog.Logger.Info("increment table log file logminer null data, transferdb will continue to capture")
		}
	}
	return nil
}

func getOracleTableIncrementRecordLogFile(cfg *config.CfgFile, engine *db.Engine) ([]map[string]string, error) {
	// 获取增量表起始最小 SCN 号
	globalSCN, err := engine.GetMySQLTableIncrementMetaMinGlobalSCNTime(cfg.TargetConfig.MetaSchema)
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

type IncrementPayload struct {
	Engine         *db.Engine `json:"-"`
	GlobalSCN      int        `json:"global_scn"`
	SourceTableSCN int        `json:"source_table_scn"`
	SourceSchema   string     `json:"source_schema"`
	SourceTable    string     `json:"source_table"`
	TargetSchema   string     `json:"target_schema"`
	TargetTable    string     `json:"target_table"`
	Operation      string     `json:"operation"`
	OracleRedo     string     `json:"oracle_redo"` // Oracle 已执行 SQL
	MySQLRedo      []string   `json:"mysql_redo"`  // MySQL 待执行 SQL
	OperationType  string     `json:"operation_type"`
}

// 任务同步
func (p *IncrementPayload) Do() error {
	zlog.Logger.Info("oracle table increment applier start",
		zap.String("config", p.marshal()))

	// 数据写入
	if err := applierTableIncrementRecord(p.MySQLRedo, p.Engine); err != nil {
		return err
	}

	// 数据写入完毕，更新元数据 checkpoint 表
	// 如果同步中断，数据同步使用会以 global_scn 为准，也就是会进行重复消费
	if err := p.Engine.UpdateTableIncrementMetaALLSCNRecord(p.SourceSchema, p.SourceTable, p.OperationType, p.GlobalSCN, p.SourceTableSCN); err != nil {
		return err
	}

	zlog.Logger.Info("oracle table increment applier success",
		zap.String("source-schema", p.SourceSchema),
		zap.String("source-table", p.SourceTable),
		zap.String("target-schema", p.TargetSchema),
		zap.String("target-table", p.TargetTable),
	)
	return nil
}

// 序列化
func (p *IncrementPayload) marshal() string {
	b, err := json.Marshal(&p)
	if err != nil {
		zlog.Logger.Error("marshal task to string",
			zap.String("string", string(b)),
			zap.String("error", fmt.Sprintf("json marshal task failed: %v", err)))
	}
	return string(b)
}
