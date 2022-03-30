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

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

const (
	FullSyncMode = "FULL"
	ALLSyncMode  = "ALL"
)

/*
	全量同步任务
*/
// 全量数据导出导入
func FullSyncOracleTableRecordToMySQL(cfg *service.CfgFile, engine *service.Engine) error {
	startTime := time.Now()
	service.Logger.Info("all full table data sync start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if utils.VersionOrdinal(oraDBVersion) < utils.VersionOrdinal(utils.OracleSYNCRequireDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oraDBVersion)
	}

	// 获取配置文件待同步表列表
	transferTableSlice, err := GetTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	// 判断并记录待同步表列表
	for _, tableName := range transferTableSlice {
		isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, FullSyncMode)
		if err != nil {
			return err
		}
		if !isExist {
			if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, FullSyncMode); err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [wait_sync_meta],重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateFullSyncTableMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
		for _, tableName := range transferTableSlice {
			if err := engine.DeleteWaitSyncTableMetaRecord(cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName, tableName, FullSyncMode); err != nil {
				return err
			}

			if err := engine.TruncateMySQLTableRecord(cfg.TargetConfig.SchemaName, tableName); err != nil {
				return err
			}
			// 判断并记录待同步表列表
			isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, FullSyncMode)
			if err != nil {
				return err
			}
			if !isExist {
				if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, FullSyncMode); err != nil {
					return err
				}
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	waitSyncTableMetas, waitSyncTableInfo, err := engine.GetWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, FullSyncMode)
	if err != nil {
		return err
	}

	partSyncTableMetas, partSyncTableInfo, err := engine.GetPartSyncTableMetaRecord(cfg.SourceConfig.SchemaName, FullSyncMode)
	if err != nil {
		return err
	}
	if len(waitSyncTableMetas) == 0 && len(partSyncTableMetas) == 0 {
		endTime := time.Now()
		service.Logger.Info("all full table data sync finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return nil
	}

	// 判断能否断点续传
	panicCheckpointTables, err := engine.JudgingCheckpointResume(cfg.SourceConfig.SchemaName, partSyncTableMetas, FullSyncMode)
	if err != nil {
		return err
	}
	if len(panicCheckpointTables) != 0 {
		endTime := time.Now()
		service.Logger.Error("all full table data loader error",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Strings("panic tables", panicCheckpointTables))

		return fmt.Errorf("checkpoint isn't consistent, please reruning [enable-checkpoint = fase]")
	}

	// 启动全量同步任务
	if err = startOracleTableFullSync(cfg, engine, waitSyncTableInfo, partSyncTableInfo, FullSyncMode); err != nil {
		return err
	}

	endTime := time.Now()
	service.Logger.Info("all full table data sync finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

/*
	增量同步任务
*/
func IncrementSyncOracleTableRecordToMySQL(cfg *service.CfgFile, engine *service.Engine) error {
	service.Logger.Info("oracle to mysql increment sync table data start", zap.String("schema", cfg.SourceConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if utils.VersionOrdinal(oraDBVersion) < utils.VersionOrdinal(utils.OracleSYNCRequireDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oraDBVersion)
	}

	// 获取配置文件待同步表列表
	transferTableSlice, err := GetTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	// 全量数据导出导入，初始化全量元数据表以及导入完成初始化增量元数据表
	existTableList, isNotExistTableList, err := engine.IsExistIncrementSyncMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice)
	if err != nil {
		return err
	}

	// 如果下游数据库增量元数据表 increment_sync_meta 存在迁移表记录
	if len(existTableList) > 0 {
		// 配置文件获取表列表等于元数据库表列表，直接增量数据同步
		if len(existTableList) == len(transferTableSlice) {
			// 判断表全量是否完成
			panicTables, err := engine.IsFinishFullSyncMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice, ALLSyncMode)
			if err != nil {
				return err
			}
			if len(panicTables) != 0 {
				return fmt.Errorf("table list [%s] can't incremently sync, because table full sync isn't finished", panicTables)
			}
			// 增量数据同步
			for range time.Tick(300 * time.Millisecond) {
				if err := syncOracleTableIncrementRecordToMySQLUsingAllMode(cfg, engine); err != nil {
					return err
				}
			}
			return nil
		}

		// 配置文件获取的表列表不等于 increment_sync_meta 表列表数，不能直接增量同步，需要手工调整
		return fmt.Errorf("there is a migration table record for increment_sync_meta, but the configuration table list is not equal to the number of increment_sync_meta table lists, and it cannot be directly incrementally synchronized, please manually adjust to a list of meta-database tables [%v]", existTableList)
	}

	// 如果下游数据库增量元数据表 increment_sync_meta 不存在任何记录，说明未进行过数据同步，则进行全量 + 增量数据同步
	if len(existTableList) == 0 && len(isNotExistTableList) == len(transferTableSlice) {
		// 全量同步
		if err = syncOracleFullTableRecordToMySQLUsingAllMode(cfg, engine, transferTableSlice, ALLSyncMode); err != nil {
			return err
		}
		// 增量数据同步
		for range time.Tick(300 * time.Millisecond) {
			if err = syncOracleTableIncrementRecordToMySQLUsingAllMode(cfg, engine); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("increment sync taskflow condition isn't match, can't sync")
}

func syncOracleFullTableRecordToMySQLUsingAllMode(cfg *service.CfgFile, engine *service.Engine, transferTableSlice []string, syncMode string) error {
	startTime := time.Now()
	service.Logger.Info("all full table data loader start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	// 判断并记录待同步表列表
	for _, tableName := range transferTableSlice {
		isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, syncMode)
		if err != nil {
			return err
		}
		if !isExist {
			if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, syncMode); err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [wait_sync_meta],重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateFullSyncTableMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
		for _, tableName := range transferTableSlice {
			if err := engine.DeleteWaitSyncTableMetaRecord(
				cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName, tableName, syncMode); err != nil {
				return err
			}

			if err := engine.TruncateMySQLTableRecord(cfg.TargetConfig.SchemaName, tableName); err != nil {
				return err
			}
			// 判断并记录待同步表列表
			isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, syncMode)
			if err != nil {
				return err
			}
			if !isExist {
				if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, syncMode); err != nil {
					return err
				}
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	waitSyncTableMetas, waitSyncTableInfo, err := engine.GetWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, syncMode)
	if err != nil {
		return err
	}

	partSyncTableMetas, partSyncTableInfo, err := engine.GetPartSyncTableMetaRecord(cfg.SourceConfig.SchemaName, syncMode)
	if err != nil {
		return err
	}
	if len(waitSyncTableMetas) == 0 && len(partSyncTableMetas) == 0 {
		endTime := time.Now()
		service.Logger.Info("all full table data loader finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return nil
	}

	// 判断能否断点续传
	panicCheckpointTables, err := engine.JudgingCheckpointResume(cfg.SourceConfig.SchemaName, partSyncTableMetas, syncMode)
	if err != nil {
		return err
	}
	if len(panicCheckpointTables) != 0 {
		endTime := time.Now()
		service.Logger.Error("all full table data loader error",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Strings("panic tables", panicCheckpointTables))

		return fmt.Errorf("checkpoint isn't consistent, please reruning [enable-checkpoint = fase]")
	}

	// 启动全量同步任务
	if err = startOracleTableFullSync(cfg, engine, waitSyncTableInfo, partSyncTableInfo, syncMode); err != nil {
		return err
	}

	// 全量任务结束，写入增量源数据表起始 SCN 号
	//根据配置文件生成同步表元数据 [increment_sync_meta]
	if err = generateTableIncrementTaskCheckpointMeta(cfg.SourceConfig.SchemaName, engine, syncMode); err != nil {
		return err
	}

	endTime := time.Now()
	service.Logger.Info("all full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func syncOracleTableIncrementRecordToMySQLUsingAllMode(cfg *service.CfgFile, engine *service.Engine) error {
	// 获取增量所需得日志文件
	logFiles, err := getOracleTableIncrementRecordLogFile(engine, cfg.SourceConfig.SchemaName)
	if err != nil {
		return err
	}

	service.Logger.Info("increment table log file get",
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

		service.Logger.Info("increment table log file logminer",
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
			logminerContentMap map[string][]service.LogminerContent
		)
		if len(rowsResult) > 0 {
			// 判断当前日志文件是否是重做日志文件
			if utils.IsContainString(redoLogList, log["LOG_FILE"]) {
				// 判断是否是当前重做日志文件
				// 如果当前日志文件是当前重做日志文件则 filterOracleRedoGreaterOrEqualRecordByTable 只运行一次大于或等于，也就是只重放一次已消费得 SCN
				if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
					logminerContentMap, err = filterOracleRedoGreaterOrEqualRecordByTable(
						rowsResult,
						transferTableSlice,
						transferTableMetaMap,
						cfg.AllConfig.FilterThreads,
						utils.CurrentResetFlag,
					)
					if err != nil {
						return err
					}
					service.Logger.Warn("oracle current redo log reset flag", zap.Int("CurrentResetFlag", utils.CurrentResetFlag))
					utils.CurrentResetFlag = 1
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
				service.Logger.Warn("increment table log file logminer data that needn't to be consumed by current redo, transferdb will continue to capture")
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
			service.Logger.Warn("increment table log file logminer data that needn't to be consumed by logfile, transferdb will continue to capture")
			continue
		}

		// 当前日志文件不存在数据记录
		if utils.IsContainString(redoLogList, log["LOG_FILE"]) {
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
		service.Logger.Warn("increment table log file logminer null data, transferdb will continue to capture")
		continue
	}
	return nil
}

func getOracleTableIncrementRecordLogFile(engine *service.Engine, sourceSchemaName string) ([]map[string]string, error) {
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

/*
	全量/增量 FUNCTION
*/
func startOracleTableFullSync(cfg *service.CfgFile, engine *service.Engine, waitSyncTableInfo, partSyncTableInfo []string, syncMode string) error {
	if len(partSyncTableInfo) > 0 {
		if err := startOracleTableConsumeByCheckpoint(cfg, engine, partSyncTableInfo, syncMode); err != nil {
			return err
		}
	}
	if len(waitSyncTableInfo) > 0 {
		// 初始化表任务
		if err := initOracleTableConsumeRowID(cfg, engine, waitSyncTableInfo, FullSyncMode); err != nil {
			return err
		}
		if err := startOracleTableConsumeByCheckpoint(cfg, engine, waitSyncTableInfo, syncMode); err != nil {
			return err
		}
	}
	return nil
}

// 从配置文件获取需要迁移同步的表列表
func GetTransferTableSliceByCfg(cfg *service.CfgFile, engine *service.Engine) ([]string, error) {
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
