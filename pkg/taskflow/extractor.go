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

	"github.com/WentaoJin/transferdb/util"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/xxjwxc/gowp/workpool"
)

// 捕获全量数据
func extractorTableFullRecord(engine *db.Engine, sourceSchemaName, sourceTableName, oracleQuery string) ([]string, []string, error) {
	startTime := time.Now()
	zlog.Logger.Info("single full table data extractor start",
		zap.String("schema", sourceSchemaName),
		zap.String("table", sourceTableName))

	columns, rowsResult, err := engine.GetOracleTableRecordByRowIDSQL(oracleQuery)
	if err != nil {
		return columns, rowsResult, err
	}

	endTime := time.Now()
	zlog.Logger.Info("single full table data extractor finished",
		zap.String("schema", sourceSchemaName),
		zap.String("table", sourceTableName),
		zap.String("cost", endTime.Sub(startTime).String()))

	return columns, rowsResult, nil
}

// 捕获增量数据
func extractorTableIncrementRecord(engine *db.Engine,
	sourceSchemaName string,
	transferTableMetaSlice []string,
	transferTableMetaMap map[string]int,
	logFileStartSCN, logminerStartSCN int) ([]map[string]string, error) {
	rowsResult, err := engine.GetOracleLogminerContentToMySQL(sourceSchemaName, transferTableMetaSlice, logminerStartSCN)
	if err != nil {
		return []map[string]string{}, err
	}

	rows, err := filterOracleSQLRedo(engine, rowsResult, transferTableMetaMap, logFileStartSCN)
	if err != nil {
		return []map[string]string{}, err
	}
	return rows, nil
}

// 过滤筛选 Oracle Redo SQL
// 1、数据同步只同步 INSERT/DELETE/UPDATE DML以及只同步 truncate table/ drop table 限定 DDL
// 2、根据元数据表 table_increment_meta 对应表已经同步写入得 SCN SQL 记录,过滤 Oracle 提交记录 SCN 号，过滤,防止重复写入
func filterOracleSQLRedo(engine *db.Engine,
	rowsResult []map[string]string,
	transferTableMetaMap map[string]int,
	logFileStartSCN int) ([]map[string]string, error) {
	var rows []map[string]string
	for i, r := range rowsResult {
		sourceTableSCN, err := strconv.Atoi(r["SCN"])
		if err != nil {
			return rows, err
		}

		if sourceTableSCN > transferTableMetaMap[r["TABLE_NAME"]] {
			if r["OPERATION"] == util.InsertOperation {
				rows = append(rows, rowsResult[i])
			}
			if r["OPERATION"] == util.DeleteOperation {
				rows = append(rows, rowsResult[i])
			}
			if r["OPERATION"] == util.UpdateOperation {
				rows = append(rows, rowsResult[i])
			}
			if r["OPERATION"] == util.DDLOperation {
				splitDDL := strings.Split(r["SQL_REDO"], ` `)
				ddl := fmt.Sprintf("%s %s", splitDDL[0], splitDDL[1])
				if strings.ToUpper(ddl) == util.DropTableOperation {
					// 处理 drop table marvin8 AS "BIN$vVWfliIh6WfgU0EEEKzOvg==$0"
					rowsResult[i]["SQL_REDO"] = strings.Split(strings.ToUpper(rowsResult[i]["SQL_REDO"]), "AS")[0]
					rows = append(rows, rowsResult[i])
				}
				if strings.ToUpper(ddl) == util.TruncateTableOperation {
					rows = append(rows, rowsResult[i])
				}
			}
		} else {
			// 如果 source_table_scn 小于 global_scn 说明，该表一直在当前日志文件内未发现 DML 事务变更
			// global_scn 表示日志文件起始 SCN 号
			// 更新增量元数据表 SCN 位点信息
			if err := engine.UpdateTableIncrementMetaOnlyGlobalSCNRecord(
				r["SEG_OWNER"],
				r["TABLE_NAME"], logFileStartSCN); err != nil {
				return rows, err
			}
			// 考虑日志可能输出太多，忽略输出
			//zlog.Logger.Warn("filter oracle sql redo",
			//	zap.Int("source table scn", sourceTableSCN),
			//	zap.Int("target table scn", transferTableMetaMap[r["TABLE_NAME"]]),
			//	zap.String("source_schema", r["SEG_OWNER"]),
			//	zap.String("source_table", r["TABLE_NAME"]),
			//	zap.String("sql redo", r["SQL_REDO"]),
			//	zap.String("status", "update global scn and skip apply"))
		}
	}
	return rows, nil
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

// 根据配置文件生成同步表元数据 [table_full_meta]
func generateTableFullTaskCheckpointMeta(cfg *config.CfgFile, engine *db.Engine, fullTblSlice []string, globalSCN int) error {
	// 设置工作池
	// 设置 goroutine 数
	if len(fullTblSlice) > 0 {
		wp := workpool.New(cfg.FullConfig.WorkerThreads)
		for _, table := range fullTblSlice {
			// 变量替换，直接使用原变量会导致并发输出有问题
			tbl := table
			workerBatch := cfg.FullConfig.WorkerBatch
			insertBatchSize := cfg.AppConfig.InsertBatchSize
			sourceSchemaName := cfg.SourceConfig.SchemaName
			wp.DoWait(func() error {
				if err := engine.InitMySQLTableFullMeta(sourceSchemaName, tbl, globalSCN, workerBatch, insertBatchSize); err != nil {
					return err
				}
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return err
		}
	}
	return nil
}

// 根据配置文件以及起始 SCN 生成同步表元数据 [table_increment_meta]
func generateTableIncrementTaskCheckpointMeta(cfg *config.CfgFile, engine *db.Engine, incrementTblSlice []string, globalSCN int) error {
	// 设置工作池
	// 设置 goroutine 数
	if len(incrementTblSlice) > 0 {
		wp := workpool.New(cfg.FullConfig.WorkerThreads)
		for _, table := range incrementTblSlice {
			// 变量替换，直接使用原变量会导致并发输出有问题
			tbl := table
			sourceSchemaName := cfg.SourceConfig.SchemaName
			wp.DoWait(func() error {
				// 判断是否全量元数据表对应表是否存在 SCN 记录，如果存在则继承，如果不存在则取用新的 SCN
				startSCN, err := engine.GetMySQLTableFullMetaSCN(sourceSchemaName, tbl)
				if err != nil {
					return err
				}

				if startSCN == 0 {
					if err := engine.InitMySQLTableIncrementMeta(sourceSchemaName, tbl, globalSCN); err != nil {
						return err
					}
				} else {
					if err := engine.InitMySQLTableIncrementMeta(sourceSchemaName, tbl, startSCN); err != nil {
						return err
					}
				}
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return err
		}
	}
	return nil
}
