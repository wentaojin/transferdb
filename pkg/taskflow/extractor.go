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

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/xxjwxc/gowp/workpool"
)

// 捕获表全量数据
func extractorTableFullRecord(engine *db.Engine, oraSQL string) ([]string, [][]string, error) {
	cols, rows, err := engine.GetOracleTableRecordByRowIDSQL(oraSQL)
	if err != nil {
		return cols, rows, err
	}
	return cols, rows, nil
}

// 从配置文件获取需要迁移同步的表列表
func GetTransferTableSliceByCfg(cfg *config.CfgFile, engine *db.Engine) ([]string, error) {
	err := engine.IsExistOracleSchema(cfg.SourceConfig.SchemaName)
	if err != nil {
		return []string{}, err
	}
	var exporterTableSlice []string
	if len(cfg.SourceConfig.IncludeTable) != 0 {
		if err := engine.IsExistOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.IncludeTable); err != nil {
			return exporterTableSlice, err
		}
		exporterTableSlice = append(exporterTableSlice, cfg.SourceConfig.IncludeTable...)
	}

	if len(cfg.SourceConfig.ExcludeTable) != 0 {
		exporterTableSlice, err = engine.FilterDifferenceOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.ExcludeTable)
		if err != nil {
			return exporterTableSlice, err
		}
	}
	if len(exporterTableSlice) == 0 {
		return exporterTableSlice, fmt.Errorf("exporter table slice can not null")
	}

	return exporterTableSlice, nil
}

// 根据配置文件生成同步表元数据
// 1、获取一致性 SCN 写入增量表元数据（用于增量同步起始点，可能存在重复执行，默认前 10 分钟 safe-mode）
// 2、按要求切分，写入全量表数据并按照要求全量导出并导入
func GenerateCheckpointMeta(cfg *config.CfgFile, engine *db.Engine, fullTblSlice, incrementTblSlice []string) error {
	// 全量同步前，获取 SCN ，ALL 模式下增量同步起始点，需要获取 table_increment_meta 中 global_scn 最小 POS 位点开始
	globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}

	// 设置工作池
	// 设置 goroutine 数
	if len(fullTblSlice) > 0 {
		wp := workpool.New(cfg.FullConfig.WorkerBatch)
		for _, tbl := range fullTblSlice {
			wp.DoWait(func() error {
				if err := engine.InitMySQLTableFullMeta(cfg.SourceConfig.SchemaName, tbl, cfg.FullConfig.WorkerBatch); err != nil {
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
		wp := workpool.New(cfg.FullConfig.WorkerBatch)
		for _, tbl := range incrementTblSlice {
			wp.DoWait(func() error {
				if err := engine.InitMySQLTableIncrementMeta(cfg.SourceConfig.SchemaName, tbl, globalSCN); err != nil {
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

// 根据配置文件表，获取用于导出 oracle 表记录 SQL 以及清理元数据库表记录删除 SQL
func GenerateMySQLTableFullMetaSQL(cfg *config.CfgFile, engine *db.Engine, tableName string) ([]string, string, error) {
	oraSQL, err := engine.GetMySQLTableFullMetaRowIDSQLRecord(
		cfg.TargetConfig.MetaSchema,
		"table_full_meta",
		fmt.Sprintf("WHERE upper(source_schema_name)=upper('%s') AND upper(source_table_name)=upper('%s')",
			cfg.SourceConfig.SchemaName, tableName),
	)
	if err != nil {
		return []string{}, "", err
	}

	sqlDelete := fmt.Sprintf("DELETE FROM %s WHERE %s",
		fmt.Sprintf("%s.%s", cfg.TargetConfig.MetaSchema, "table_full_meta"),
		fmt.Sprintf("upper(source_schema_name)=upper('%s') AND upper(source_table_name)=upper('%s')",
			cfg.SourceConfig.SchemaName, tableName))

	return oraSQL, sqlDelete, nil
}
