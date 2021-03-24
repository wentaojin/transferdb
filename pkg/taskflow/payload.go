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
	"fmt"
	"strconv"
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/WentaoJin/transferdb/util"

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

	tableMetas, transferTableList, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	if len(tableMetas) == 0 {
		return fmt.Errorf("mysql meta schema [%v] table [%v] can't null, please run reverse mode",
			cfg.TargetConfig.MetaSchema,
			"table_meta")
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [table_full_meta],重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateMySQLTableFullMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
		if err := engine.ClearMySQLTableMetaRecord(cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName); err != nil {
			return err
		}
		if err := engine.TruncateMySQLTableRecord(cfg.SourceConfig.SchemaName, tableMetas); err != nil {
			return err
		}
	}

	isOK, waitInitTableList, panicTableList, err := engine.AdjustFullStageMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, tableMetas)
	if err != nil {
		return err
	}
	if !isOK {
		return fmt.Errorf("meet panic table list [%v] case, can't breakpoint resume, need set enable-checkpoint = false ,and reruning again", panicTableList)
	}

	// 表同步任务
	if err = loaderOracleTableTask(cfg, engine, transferTableList, waitInitTableList); err != nil {
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

	tableMetas, transferTableList, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	if len(tableMetas) == 0 {
		return fmt.Errorf("mysql meta schema [%v] table [%v] can't null, please run reverse mode",
			cfg.TargetConfig.MetaSchema,
			"table_meta")
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [table_full_meta],重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateMySQLTableFullMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
		if err := engine.ClearMySQLTableMetaRecord(cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName); err != nil {
			return err
		}
		if err := engine.TruncateMySQLTableRecord(cfg.SourceConfig.SchemaName, tableMetas); err != nil {
			return err
		}
	}

	isOK, waitInitTableList, panicTableList, err := engine.AdjustFullStageMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, tableMetas)
	if err != nil {
		return err
	}
	if !isOK {
		return fmt.Errorf("meet panic table list [%v] case, can't breakpoint resume, need set enable-checkpoint = false ,and reruning again", panicTableList)
	}

	// 表同步任务
	if err = loaderOracleTableTask(cfg, engine, transferTableList, waitInitTableList); err != nil {
		return err
	}

	// 全量任务结束，写入增量源数据表起始 SCN 号
	//根据配置文件生成同步表元数据 [table_increment_meta]
	if err = generateTableIncrementTaskCheckpointMeta(cfg.SourceConfig.SchemaName, engine); err != nil {
		return err
	}

	endTime := time.Now()
	zlog.Logger.Info("all full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func loaderOracleTableTask(cfg *config.CfgFile, engine *db.Engine, transferTableList, waitInitTableList []string) error {
	if len(waitInitTableList) > 0 {
		if len(waitInitTableList) == len(transferTableList) {
			if err := loaderTableFullTaskBySCN(cfg, engine, waitInitTableList); err != nil {
				return err
			}
			return nil
		}
		if err := loaderTableFullTaskBySCN(cfg, engine, waitInitTableList); err != nil {
			return err
		}
		diffTableSlice := util.FilterDifferenceStringItems(transferTableList, waitInitTableList)
		if len(diffTableSlice) > 0 {
			if err := loaderTableFullTaskByCheckpoint(cfg, engine, diffTableSlice); err != nil {
				return err
			}
		}
		return nil
	} else {
		transferTables, err := engine.GetMySQLTableFullMetaSchemaTableRecord(cfg.SourceConfig.SchemaName)
		if err != nil {
			return err
		}
		if err := loaderTableFullTaskByCheckpoint(cfg, engine, transferTables); err != nil {
			return err
		}
		return nil
	}
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
	for range time.Tick(300 * time.Millisecond) {
		if err := syncOracleTableIncrementRecordToMySQLByAllMode(cfg, engine); err != nil {
			return err
		}
	}
	return nil
}

func syncOracleTableIncrementRecordToMySQLByAllMode(cfg *config.CfgFile, engine *db.Engine) error {
	// 获取增量元数据表内所需同步表信息
	transferTableSlice, transferTableMetaMap, err := engine.GetMySQLTableIncrementMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}

	// 获取增量所需得日志文件
	logFiles, err := getOracleTableIncrementRecordLogFile(engine, cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}

	zlog.Logger.Info("increment table log file get",
		zap.String("logfile", fmt.Sprintf("%v", logFiles)))

	// 遍历所有日志文件
	for _, log := range logFiles {
		// 获取日志文件起始 SCN
		logFileStartSCN, err := strconv.Atoi(log["FIRST_CHANGE"])
		if err != nil {
			return err
		}

		// 获取日志文件结束 SCN
		logFileEndSCN, err := strconv.Atoi(log["NEXT_CHANGE"])
		if err != nil {
			return err
		}

		zlog.Logger.Info("increment table log file logminer",
			zap.String("logfile", log["LOG_FILE"]),
			zap.Int("logfile start scn", logFileStartSCN),
			zap.Int("logminer start scn", logFileStartSCN),
			zap.Int("logfile end scn", logFileEndSCN))

		// logminer 运行
		if err = engine.AddOracleLogminerlogFile(log["LOG_FILE"]); err != nil {
			return err
		}

		if err = engine.StartOracleLogminerStoredProcedure(log["FIRST_CHANGE"]); err != nil {
			return err
		}

		// 获取 logminer query 起始最小 SCN
		minSourceTableSCN, err := engine.GetMySQLTableIncrementMetaMinSourceTableSCNTime(
			cfg.SourceConfig.SchemaName)
		if err != nil {
			return err
		}

		// 捕获数据
		rowsResult, err := extractorTableIncrementRecord(
			engine,
			cfg.SourceConfig.SchemaName,
			transferTableSlice,
			log["LOG_FILE"],
			logFileStartSCN,
			minSourceTableSCN)
		if err != nil {
			return err
		}

		// logminer 关闭
		if err = engine.EndOracleLogminerStoredProcedure(); err != nil {
			return err
		}

		if len(rowsResult) > 0 {
			// 按表级别筛选数据
			logminerContentMap, err := filterOracleRedoRecordByTable(
				rowsResult,
				transferTableSlice,
				transferTableMetaMap,
				cfg.AllConfig.FilterThreads)
			if err != nil {
				return err
			}

			wp := workpool.New(cfg.AllConfig.ApplyThreads)
			for tableName, lcs := range logminerContentMap {
				rowsResult := lcs
				tbl := tableName
				logStartSCN := logFileStartSCN
				logEndSCN := logFileEndSCN
				wp.DoWait(func() error {
					var (
						done        = make(chan bool)
						taskQueue   = make(chan IncrementPayload, cfg.AllConfig.WorkerQueue)
						resultQueue = make(chan IncrementResult, cfg.AllConfig.WorkerQueue)
					)
					// 获取增量执行结果
					go GetIncrementResult(done, resultQueue)

					// 转换捕获内容以及数据应用
					go func(engine *db.Engine, tbl, targetSchemaName string, rowsResult []db.LogminerContent, taskQueue chan IncrementPayload) {
						defer func() {
							if err := recover(); err != nil {
								zlog.Logger.Panic("panic", zap.Error(fmt.Errorf("%v", err)))
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

					// 数据转换并应用完毕，判断当前元数据 checkpoint 是否需要更新
					if err := engine.UpdateSingleTableIncrementMetaSCNByLogFileEndSCN(
						cfg.SourceConfig.SchemaName,
						tbl,
						logStartSCN,
						logEndSCN,
					); err != nil {
						return err
					}
					return nil
				})
			}
			if err := wp.Wait(); err != nil {
				return err
			}
			if !wp.IsDone() {
				return fmt.Errorf("logminerContentMap concurrency meet error")
			}
		} else {
			if err := engine.UpdateALLTableIncrementMetaSCNRecordByLogFileEndSCN(
				cfg.SourceConfig.SchemaName,
				logFileStartSCN,
				logFileEndSCN); err != nil {
				return err
			}
			zlog.Logger.Info("increment table log file logminer null data, transferdb will continue to capture")
		}
	}
	return nil
}

func getOracleTableIncrementRecordLogFile(engine *db.Engine, sourceSchemaName string) ([]map[string]string, error) {
	// 获取增量表起始最小 SCN 号
	globalSCN, err := engine.GetMySQLTableIncrementMetaMinGlobalSCNTime(sourceSchemaName)
	if err != nil {
		return []map[string]string{}, err
	}
	strGlobalSCN := strconv.Itoa(globalSCN)
	// 判断数据是在 archived log Or redo log
	// 如果 redoSCN 等于 0，说明数据在归档日志
	redoScn, err := engine.GetOracleRedoLogSCN(strGlobalSCN)
	if err != nil {
		return []map[string]string{}, err
	}

	archivedScn, err := engine.GetOracleArchivedLogSCN(strGlobalSCN)
	if err != nil {
		return []map[string]string{}, err
	}

	var (
		logFiles []map[string]string
	)
	// 获取所需挖掘的日志文件
	if redoScn == 0 {
		strArchivedSCN := strconv.Itoa(archivedScn)
		logFiles, err = engine.GetOracleArchivedLogFile(strArchivedSCN)
		if err != nil {
			return logFiles, err
		}
	} else {
		strRedoCN := strconv.Itoa(redoScn)
		logFiles, err = engine.GetOracleRedoLogFile(strRedoCN)
		if err != nil {
			return logFiles, err
		}
	}
	return logFiles, nil
}
