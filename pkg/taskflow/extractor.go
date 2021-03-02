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
	"strings"
	"time"

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
func extractorTableIncrementRecord(engine *db.Engine, sourceSchemaName string, sourceTableSlice []string) ([]map[string]string, error) {
	rowsResult, err := engine.GetOracleLogminerContentToMySQL(sourceSchemaName, sourceTableSlice)
	if err != nil {
		return []map[string]string{}, err
	}
	rows := filterOracleSQLRedo(rowsResult)
	return rows, nil
}

// DML 同步只同步 INSERT/DELETE/UPDATE 以及限定 DDL，只同步 truncate table/ drop table
func filterOracleSQLRedo(rowsResult []map[string]string) []map[string]string {
	var rows []map[string]string
	for i, r := range rowsResult {
		if r["OPERATION"] == "INSERT" {
			rows = append(rows, rowsResult[i])
		}
		if r["OPERATION"] == "DELETE" {
			rows = append(rows, rowsResult[i])
		}
		if r["OPERATION"] == "UPDATE" {
			rows = append(rows, rowsResult[i])
		}
		if r["OPERATION"] == "DDL" {
			splitDDL := strings.Split(r["SQL_REDO"], "")
			ddl := fmt.Sprintf("%s %s", splitDDL[0], splitDDL[1])
			if strings.ToUpper(ddl) == "DROP TABLE" {
				rows = append(rows, rowsResult[i])
			}
			if strings.ToUpper(ddl) == "TRUNCATE TABLE" {
				rows = append(rows, rowsResult[i])
			}
		}
	}
	return rows
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
				if err := engine.InitMySQLTableIncrementMeta(sourceSchemaName, tbl, globalSCN); err != nil {
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

// 从元数据表 [table_full_meta]，获取用于导出 oracle 表记录 SQL
func getMySQLTableFullMetaRowIDSQL(cfg *config.CfgFile, engine *db.Engine, tableName string) ([]string, error) {
	oraSQL, err := engine.GetMySQLTableFullMetaRowIDSQLRecord(
		cfg.TargetConfig.MetaSchema,
		"table_full_meta",
		fmt.Sprintf("WHERE upper(source_schema_name)=upper('%s') AND upper(source_table_name)=upper('%s')",
			cfg.SourceConfig.SchemaName, tableName),
	)
	if err != nil {
		return []string{}, err
	}

	return oraSQL, nil
}

// 清理元数据表 [table_full_meta] 断点记录
func clearMySQLTableFullMetaRowIDSQL(cfg *config.CfgFile, engine *db.Engine, tableName, rowidSQL string) error {
	clearSQL := fmt.Sprintf(`
	DELETE FROM %s.%s 
		WHERE upper(source_schema_name)=upper('%s') 
		AND upper(source_table_name)=upper('%s')
		AND upper(rowid_sql)=upper("%s")`,
		cfg.TargetConfig.MetaSchema,
		"table_full_meta",
		cfg.SourceConfig.SchemaName, tableName, rowidSQL)
	if err := engine.ClearMySQLTableFullMetaRecord(cfg.TargetConfig.MetaSchema, tableName, clearSQL); err != nil {
		return err
	}
	return nil
}
