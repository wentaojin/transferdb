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
	"strings"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"

	"github.com/xxjwxc/gowp/workpool"
)

// 捕获全量数据
func extractorTableFullRecord(engine *service.Engine, sourceSchemaName, sourceTableName, oracleQuery string) ([]string, []string, error) {
	startTime := time.Now()
	service.Logger.Info("single full table data extractor start",
		zap.String("schema", sourceSchemaName),
		zap.String("table", sourceTableName))

	columns, rowsResult, err := engine.GetOracleTableRecordByRowIDSQL(oracleQuery)
	if err != nil {
		return columns, rowsResult, err
	}

	endTime := time.Now()
	service.Logger.Info("single full table data extractor finished",
		zap.String("schema", sourceSchemaName),
		zap.String("table", sourceTableName),
		zap.String("cost", endTime.Sub(startTime).String()))

	return columns, rowsResult, nil
}

// 捕获增量数据
func extractorTableIncrementRecord(engine *service.Engine,
	sourceSchemaName string,
	sourceTableNameList []string,
	logFileName string,
	logFileStartSCN int, lastCheckpoint, logminerQueryTimeout int) ([]service.LogminerContent, error) {
	rowsResult, err := engine.GetOracleLogminerContentToMySQL(
		sourceSchemaName,
		utils.StringArrayToCapitalChar(sourceTableNameList),
		strconv.Itoa(lastCheckpoint),
		logminerQueryTimeout)
	if err != nil {
		return []service.LogminerContent{}, err
	}
	service.Logger.Info("increment table log extractor", zap.String("logfile", logFileName),
		zap.Int("logfile start scn", logFileStartSCN),
		zap.Int("source table last scn", lastCheckpoint),
		zap.Int("row counts", len(rowsResult)))

	return rowsResult, nil
}

// 按表级别筛选区别数据
func filterOracleRedoGreaterOrEqualRecordByTable(
	rowsResult []service.LogminerContent,
	transferTableList []string,
	transferTableMetaMap map[string]int,
	workerThreads, currentResetFlag int) (map[string][]service.LogminerContent, error) {
	var (
		lcMap map[string][]service.LogminerContent
		lc    []service.LogminerContent
	)
	lcMap = make(map[string][]service.LogminerContent)

	for _, table := range transferTableList {
		lcMap[strings.ToUpper(table)] = lc
	}

	startTime := time.Now()
	service.Logger.Info("oracle table redo filter start",
		zap.Time("start time", startTime))

	c := make(chan struct{})
	// 开始准备从 channel 接收数据了
	s := NewScheduleJob(workerThreads, lcMap, func() { c <- struct{}{} })

	wp := workpool.New(workerThreads)
	for _, rs := range rowsResult {
		tfMap := transferTableMetaMap
		rows := rs
		isFirstR := currentResetFlag
		wp.DoWait(func() error {
			// 筛选过滤 Oracle Redo SQL
			// 1、数据同步只同步 INSERT/DELETE/UPDATE DML以及只同步 truncate table/ drop table 限定 DDL
			// 2、根据元数据表 table_increment_meta 对应表已经同步写入得 SCN SQL 记录,过滤 Oracle 提交记录 SCN 号，过滤,防止重复写入
			if isFirstR == 0 {
				if rows.SCN >= tfMap[strings.ToUpper(rows.TableName)] {
					if rows.Operation == utils.DDLOperation {
						splitDDL := strings.Split(rows.SQLRedo, ` `)
						ddl := utils.StringsBuilder(splitDDL[0], ` `, splitDDL[1])
						if strings.ToUpper(ddl) == utils.DropTableOperation {
							// 处理 drop table marvin8 AS "BIN$vVWfliIh6WfgU0EEEKzOvg==$0"
							rows.SQLRedo = strings.Split(strings.ToUpper(rows.SQLRedo), "AS")[0]
							s.AddData(rows)
						}
						if strings.ToUpper(ddl) == utils.TruncateTableOperation {
							// 处理 truncate table marvin8
							s.AddData(rows)
						}
					} else {
						s.AddData(rows)
					}
				}
				return nil

			} else if isFirstR == 1 {
				if rows.SCN > tfMap[strings.ToUpper(rows.TableName)] {
					if rows.Operation == utils.DDLOperation {
						splitDDL := strings.Split(rows.SQLRedo, ` `)
						ddl := utils.StringsBuilder(splitDDL[0], ` `, splitDDL[1])
						if strings.ToUpper(ddl) == utils.DropTableOperation {
							// 处理 drop table marvin8 AS "BIN$vVWfliIh6WfgU0EEEKzOvg==$0"
							rows.SQLRedo = strings.Split(strings.ToUpper(rows.SQLRedo), "AS")[0]
							s.AddData(rows)
						}
						if strings.ToUpper(ddl) == utils.TruncateTableOperation {
							// 处理 truncate table marvin8
							s.AddData(rows)
						}
					} else {
						s.AddData(rows)
					}
				}
				return nil
			} else {
				return fmt.Errorf("filterOracleRedoGreaterOrEqualRecordByTable meet error, isFirstRun value error")
			}
		})
	}
	if err := wp.Wait(); err != nil {
		return lcMap, err
	}
	if !wp.IsDone() {
		return lcMap, fmt.Errorf("filter oracle redo record by table error")
	}

	s.Close()
	<-c

	endTime := time.Now()
	service.Logger.Info("oracle table filter finished",
		zap.String("status", "success"),
		zap.Time("start time", startTime),
		zap.Time("end time", endTime),
		zap.String("cost time", time.Since(startTime).String()))

	return lcMap, nil
}

// 1、根据当前表的 SCN 初始化元数据据表
// 2、根据元数据表记录全量导出导入
func syncFullTableTaskUsingSCN(cfg *service.CfgFile, engine *service.Engine, fullTblSlice []string, syncMode string) error {
	wp := workpool.New(cfg.FullConfig.WorkerThreads)
	for _, table := range fullTblSlice {
		// 变量替换，直接使用原变量会导致并发输出有问题
		tbl := table
		workerBatch := cfg.FullConfig.WorkerBatch
		insertBatchSize := cfg.AppConfig.InsertBatchSize
		sourceSchemaName := cfg.SourceConfig.SchemaName
		wp.DoWait(func() error {
			// 全量同步前，获取 SCN
			globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
			if err != nil {
				return err
			}
			if err := engine.InitWaitAndFullSyncMetaRecord(sourceSchemaName,
				tbl, globalSCN, workerBatch, insertBatchSize, syncMode); err != nil {
				return err
			}
			if err := syncOracleSingleTableTask(cfg, engine, tbl, syncMode); err != nil {
				return err
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	if !wp.IsDone() {
		return fmt.Errorf("syncFullTableTaskUsingSCN concurrency table meet error")
	}
	return nil
}

// 1、根据元数据表记录全量导出导入
func syncFullTableTaskUsingCheckpoint(cfg *service.CfgFile, engine *service.Engine, transferTables []string, syncMode string) error {
	wp := workpool.New(cfg.FullConfig.WorkerThreads)
	for _, table := range transferTables {
		// 变量替换，直接使用原变量会导致并发输出有问题
		tbl := table
		mode := syncMode
		wp.DoWait(func() error {
			if err := syncOracleSingleTableTask(cfg, engine, tbl, mode); err != nil {
				return err
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	if !wp.IsDone() {
		return fmt.Errorf("syncFullTableTaskUsingCheckpoint concurrency table meet error")
	}
	return nil
}

func syncOracleSingleTableTask(cfg *service.CfgFile, engine *service.Engine, table, syncMode string) error {
	startTime := time.Now()
	service.Logger.Info("single full table data loader start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	oraRowIDSQL, err := engine.GetFullSyncMetaRowIDRecord(cfg.SourceConfig.SchemaName, table)
	if err != nil {
		return err
	}
	wp := workpool.New(cfg.FullConfig.TableThreads)
	for _, rowidSQL := range oraRowIDSQL {
		sql := rowidSQL
		mode := syncMode
		wp.Do(func() error {
			// 抽取 Oracle 数据
			columns, rowsResult, err := extractorTableFullRecord(engine, cfg.SourceConfig.SchemaName, table, sql)
			if err != nil {
				return err
			}

			if len(rowsResult) == 0 {
				service.Logger.Warn("oracle schema table rowid data return null rows, skip",
					zap.String("schema", cfg.SourceConfig.SchemaName),
					zap.String("table", table),
					zap.String("sql", sql))
				// 清理记录以及更新记录
				if err := engine.ModifyWaitAndFullSyncTableMetaRecord(
					cfg.TargetConfig.MetaSchema,
					cfg.SourceConfig.SchemaName, table, sql, mode); err != nil {
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

			// 清理记录以及更新记录
			if err := engine.ModifyWaitAndFullSyncTableMetaRecord(
				cfg.TargetConfig.MetaSchema,
				cfg.SourceConfig.SchemaName, table, sql, mode); err != nil {
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
		service.Logger.Fatal("single full table data loader failed",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("table", table),
			zap.String("cost", endTime.Sub(startTime).String()))
		return fmt.Errorf("oracle schema [%s] single full table [%v] data loader failed",
			cfg.SourceConfig.SchemaName, table)
	}
	service.Logger.Info("single full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("table", table),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

// 根据配置文件以及起始 SCN 生成同步表元数据 [increment_sync_meta]
func generateTableIncrementTaskCheckpointMeta(sourceSchemaName string, engine *service.Engine, syncMode string) error {
	tableMeta, _, err := engine.GetFinishFullSyncMetaRecord(sourceSchemaName, syncMode)
	if err != nil {
		return err
	}

	for _, tm := range tableMeta {
		if err := engine.InitIncrementSyncMetaRecord(tm.SourceSchemaName, tm.SourceTableName, tm.FullGlobalSCN); err != nil {
			return err
		}
	}
	return nil
}
