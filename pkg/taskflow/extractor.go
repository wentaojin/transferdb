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
	"time"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/xxjwxc/gowp/workpool"
)

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

// 根据配置文件生成同步表元数据
// 1、获取一致性 SCN 写入增量表元数据（用于增量同步起始点，可能存在重复执行，默认前 10 分钟 safe-mode）
// 2、按要求切分，写入全量表数据并按照要求全量导出并导入
func generateCheckpointMeta(cfg *config.CfgFile, engine *db.Engine, fullTblSlice, incrementTblSlice []string) error {
	// 全量同步前，获取 SCN ，ALL 模式下增量同步起始点，需要获取 table_increment_meta 中 global_scn 最小 POS 位点开始
	globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}

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
				if err := engine.InitMySQLTableFullMeta(sourceSchemaName, tbl, workerBatch, insertBatchSize); err != nil {
					return err
				}
				return nil
			})
		}
		if err = wp.Wait(); err != nil {
			return err
		}
	}
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
		if err = wp.Wait(); err != nil {
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
