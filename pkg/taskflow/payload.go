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
	"strconv"
	"time"

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

	// 获取配置文件待同步表列表
	transferTableSlice, err := getTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	_, tableMetaList, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	if len(tableMetaList) == 0 {
		// 初始化同步表
		if err := engine.InitMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice); err != nil {
			return err
		}
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
		tableMetas, _, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
		if err != nil {
			return err
		}
		if len(tableMetas) == 0 {
			return fmt.Errorf("mysql meta schema [%v] table [%v] can't null, meet panic",
				cfg.TargetConfig.MetaSchema,
				"table_meta")
		}
		if err := engine.TruncateMySQLTableRecord(cfg.TargetConfig.SchemaName, tableMetas); err != nil {
			return err
		}
	}

	tableMetas, transferTableList, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	if len(tableMetas) == 0 {
		return fmt.Errorf("mysql meta schema [%v] table [%v] can't null, meet panic",
			cfg.TargetConfig.MetaSchema,
			"table_meta")
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

	// 获取配置文件待同步表列表
	transferTableSlice, err := getTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	_, tableMetaList, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	if len(tableMetaList) == 0 {
		// 初始化同步表
		if err := engine.InitMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice); err != nil {
			return err
		}
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
		tableMetas, _, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
		if err != nil {
			return err
		}
		if len(tableMetas) == 0 {
			return fmt.Errorf("mysql meta schema [%v] table [%v] can't null, meet panic",
				cfg.TargetConfig.MetaSchema,
				"table_meta")
		}
		if err := engine.TruncateMySQLTableRecord(cfg.TargetConfig.SchemaName, tableMetas); err != nil {
			return err
		}
	}

	tableMetas, transferTableList, err := engine.GetMySQLTableMetaRecord(cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}
	if len(tableMetas) == 0 {
		return fmt.Errorf("mysql meta schema [%v] table [%v] can't null, meet panic",
			cfg.TargetConfig.MetaSchema,
			"table_meta")
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
		diffTableSlice := util.FilterDifferenceStringItems(transferTableList, waitInitTableList)
		if len(diffTableSlice) > 0 {
			if err := loaderTableFullTaskByCheckpoint(cfg, engine, diffTableSlice); err != nil {
				return err
			}
		}
		if err := loaderTableFullTaskBySCN(cfg, engine, waitInitTableList); err != nil {
			return err
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

// 从配置文件获取需要迁移同步的表列表
func getTransferTableSliceByCfg(cfg *config.CfgFile, engine *db.Engine) ([]string, error) {
	err := engine.IsExistOracleSchema(cfg.SourceConfig.SchemaName)
	if err != nil {
		return []string{}, err
	}
	var exporterTableSlice []string

	switch {
	case len(cfg.SourceConfig.IncludeTable) != 0 && len(cfg.SourceConfig.ExcludeTable) == 0:
		if err := engine.IsExistOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.IncludeTable); err != nil {
			return exporterTableSlice, err
		}
		exporterTableSlice = append(exporterTableSlice, cfg.SourceConfig.IncludeTable...)
	case len(cfg.SourceConfig.IncludeTable) == 0 && len(cfg.SourceConfig.ExcludeTable) != 0:
		exporterTableSlice, err = engine.FilterDifferenceOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.ExcludeTable)
		if err != nil {
			return exporterTableSlice, err
		}
	case len(cfg.SourceConfig.IncludeTable) == 0 && len(cfg.SourceConfig.ExcludeTable) == 0:
		exporterTableSlice, err = engine.GetOracleTable(cfg.SourceConfig.SchemaName)
		if err != nil {
			return exporterTableSlice, err
		}
	default:
		return exporterTableSlice, fmt.Errorf("source config params include-table/exclude-table cannot exist at the same time")
	}

	if len(exporterTableSlice) == 0 {
		return exporterTableSlice, fmt.Errorf("exporter table slice can not null by extractor task")
	}

	return exporterTableSlice, nil
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

		// 获取增量元数据表内所需同步表信息
		transferTableSlice, transferTableMetaMap, err := engine.GetMySQLTableIncrementMetaRecord(cfg.SourceConfig.SchemaName)
		if err != nil {
			return err
		}

		// 获取 logminer query 起始最小 SCN
		minSourceTableSCN, err := engine.GetMySQLTableIncrementMetaMinSourceTableSCNTime(
			cfg.SourceConfig.SchemaName)
		if err != nil {
			return err
		}

		// logminer 运行
		if err = engine.AddOracleLogminerlogFile(log["LOG_FILE"]); err != nil {
			return err
		}

		if err = engine.StartOracleLogminerStoredProcedure(log["FIRST_CHANGE"]); err != nil {
			return err
		}

		// 捕获数据
		rowsResult, err := extractorTableIncrementRecord(
			engine,
			cfg.SourceConfig.SchemaName,
			transferTableSlice,
			log["LOG_FILE"],
			logFileStartSCN,
			minSourceTableSCN,
			cfg.AllConfig.LogminerQueryTimeout)
		if err != nil {
			return err
		}

		// logminer 关闭
		if err = engine.EndOracleLogminerStoredProcedure(); err != nil {
			return err
		}

		// 获取 Oracle 所有 REDO 列表
		redoLogList, err := engine.GetOracleALLRedoLogFile()
		if err != nil {
			return err
		}

		//获取当前 CURRENT REDO LOG 信息
		currentRedoLogFirstChange, currentRedoLogMaxSCN, currentRedoLogFileName, err := engine.GetOracleCurrentRedoMaxSCN()
		if err != nil {
			return err
		}

		// 按表级别筛选数据
		var (
			logminerContentMap map[string][]db.LogminerContent
		)
		if len(rowsResult) > 0 {
			// 判断当前日志文件是否是重做日志文件
			if util.IsContainString(redoLogList, log["LOG_FILE"]) {
				// 判断是否是当前重做日志文件
				// 如果当前日志文件是当前重做日志文件则 filterOracleRedoGreaterOrEqualRecordByTable 只运行一次大于或等于，也就是只重放一次已消费得 SCN
				if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
					logminerContentMap, err = filterOracleRedoGreaterOrEqualRecordByTable(
						rowsResult,
						transferTableSlice,
						transferTableMetaMap,
						cfg.AllConfig.FilterThreads,
						util.CurrentResetFlag,
					)
					if err != nil {
						return err
					}
					zlog.Logger.Warn("oracle current redo log reset flag", zap.Int("CurrentResetFlag", util.CurrentResetFlag))
					util.CurrentResetFlag = 1
				} else {
					logminerContentMap, err = filterOracleRedoGreaterOrEqualRecordByTable(
						rowsResult,
						transferTableSlice,
						transferTableMetaMap,
						cfg.AllConfig.FilterThreads,
						0,
					)
					if err != nil {
						return err
					}
				}

				if len(logminerContentMap) > 0 {
					// 数据应用
					if err := applyOracleRedoIncrementRecord(cfg, engine, logminerContentMap); err != nil {
						return err
					}

					if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
						// 当前所有日志文件内容应用完毕，判断是否直接更新 GLOBAL_SCN 至当前重做日志文件起始 SCN
						if err := engine.UpdateSingleTableIncrementMetaSCNByCurrentRedo(
							cfg.SourceConfig.SchemaName,
							currentRedoLogMaxSCN,
							logFileStartSCN,
							logFileEndSCN,
						); err != nil {
							return err
						}
					} else {
						// 当前所有日志文件内容应用完毕，直接更新 GLOBAL_SCN 至日志文件结束 SCN
						if err := engine.UpdateSingleTableIncrementMetaSCNByNonCurrentRedo(
							cfg.SourceConfig.SchemaName,
							currentRedoLogMaxSCN,
							logFileStartSCN,
							logFileEndSCN,
							transferTableSlice,
						); err != nil {
							return err
						}
					}

					continue
				}
				zlog.Logger.Warn("increment table log file logminer data that needn't to be consumed by current redo, transferdb will continue to capture")
				continue
			}
			logminerContentMap, err = filterOracleRedoGreaterOrEqualRecordByTable(
				rowsResult,
				transferTableSlice,
				transferTableMetaMap,
				cfg.AllConfig.FilterThreads,
				0,
			)
			if err != nil {
				return err
			}
			if len(logminerContentMap) > 0 {
				// 数据应用
				if err := applyOracleRedoIncrementRecord(cfg, engine, logminerContentMap); err != nil {
					return err
				}
				// 当前所有日志文件内容应用完毕，直接更新 GLOBAL_SCN 至日志文件结束 SCN
				if err := engine.UpdateSingleTableIncrementMetaSCNByArchivedLog(
					cfg.SourceConfig.SchemaName,
					logFileEndSCN,
					transferTableSlice,
				); err != nil {
					return err
				}
				continue
			}
			zlog.Logger.Warn("increment table log file logminer data that needn't to be consumed by logfile, transferdb will continue to capture")
			continue
		}

		// 当前日志文件不存在数据记录
		if util.IsContainString(redoLogList, log["LOG_FILE"]) {
			if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
				// 当前所有日志文件内容应用完毕，判断是否直接更新 GLOBAL_SCN 至当前重做日志文件起始 SCN
				if err := engine.UpdateSingleTableIncrementMetaSCNByCurrentRedo(
					cfg.SourceConfig.SchemaName,
					currentRedoLogMaxSCN,
					logFileStartSCN,
					logFileEndSCN,
				); err != nil {
					return err
				}
			} else {
				// 当前所有日志文件内容应用完毕，判断是否更新 GLOBAL_SCN 至日志文件结束 SCN
				if err := engine.UpdateSingleTableIncrementMetaSCNByNonCurrentRedo(
					cfg.SourceConfig.SchemaName,
					currentRedoLogMaxSCN,
					logFileStartSCN,
					logFileEndSCN,
					transferTableSlice,
				); err != nil {
					return err
				}
			}
		} else {
			// 当前所有日志文件内容应用完毕，直接更新 GLOBAL_SCN 至日志文件结束 SCN
			if err := engine.UpdateSingleTableIncrementMetaSCNByArchivedLog(
				cfg.SourceConfig.SchemaName,
				logFileEndSCN,
				transferTableSlice,
			); err != nil {
				return err
			}
		}
		zlog.Logger.Warn("increment table log file logminer null data, transferdb will continue to capture")
		continue
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
