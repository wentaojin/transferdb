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
package reverser

import (
	"fmt"
	"strings"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/WentaoJin/transferdb/util"
	"github.com/WentaoJin/transferdb/zlog"

	"github.com/xxjwxc/gowp/workpool"
	"go.uber.org/zap"
)

func ReverseOracleToMySQLTable(engine *db.Engine, cfg *config.CfgFile) error {
	if err := reverseOracleToMySQLTableInspect(engine, cfg); err != nil {
		return err
	}

	tables, err := generateOracleToMySQLTables(engine, cfg)
	if err != nil {
		return err
	}

	// 设置工作池
	// 设置 goroutine 数
	wp := workpool.New(cfg.ReverseConfig.ReverseThreads)

	for _, tbl := range tables {
		wp.DoWait(func() error {
			if err := tbl.GenerateAndExecMySQLCreateTableSQL(); err != nil {
				return err
			}
			if err := tbl.GenerateAndExecMySQLCreateIndexSQL(); err != nil {
				return err
			}
			return nil
		})
	}
	if err = wp.Wait(); err != nil {
		return err
	}
	return nil
}

// 表转换前检查
func reverseOracleToMySQLTableInspect(engine *db.Engine, cfg *config.CfgFile) error {
	if err := engine.IsExistOracleSchema(cfg.SourceConfig.SchemaName); err != nil {
		return err
	}
	ok, err := engine.IsExistMySQLSchema(cfg.TargetConfig.SchemaName)
	if err != nil {
		return err
	}
	if !ok {
		_, _, err := db.Query(engine.MysqlDB, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, cfg.TargetConfig.SchemaName))
		if err != nil {
			return err
		}
		return nil
	}

	// 获取 oracle 导出转换表列表
	var exporterTableSlice []string
	if len(cfg.SourceConfig.IncludeTable) != 0 {
		if err := engine.IsExistOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.IncludeTable); err != nil {
			return err
		}
		exporterTableSlice = append(exporterTableSlice, cfg.SourceConfig.IncludeTable...)
	}

	if len(cfg.SourceConfig.ExcludeTable) != 0 {
		exporterTableSlice, err = engine.FilterDifferenceOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.ExcludeTable)
		if err != nil {
			return err
		}
	}

	// 检查源端 schema 导出表是否存在目标端 schema 内
	existMysqlTables, err := engine.FilterIntersectionMySQLTable(cfg.TargetConfig.SchemaName, exporterTableSlice)
	if err != nil {
		return err
	}
	if len(existMysqlTables) > 0 {
		for _, tbl := range existMysqlTables {
			if cfg.TargetConfig.Overwrite {
				if err := engine.RenameMySQLTableName(cfg.TargetConfig.SchemaName, tbl); err != nil {
					return err
				}
			} else {
				// 表跳过重命名以及创建
				zlog.Logger.Warn("appear warning",
					zap.String("schema", cfg.TargetConfig.SchemaName),
					zap.String("table", tbl),
					zap.String("warn",
						fmt.Sprintf("config file params overwrite value false, table skip create")))
			}
		}
	}
	return nil
}

// 转换表生成
func generateOracleToMySQLTables(engine *db.Engine, cfg *config.CfgFile) ([]Table, error) {
	var (
		exporterTableSlice []string
		err                error
	)

	if len(cfg.SourceConfig.IncludeTable) != 0 {
		if err := engine.IsExistOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.IncludeTable); err != nil {
			return []Table{}, err
		}
		exporterTableSlice = append(exporterTableSlice, cfg.SourceConfig.IncludeTable...)
	}

	if len(cfg.SourceConfig.ExcludeTable) != 0 {
		exporterTableSlice, err = engine.FilterDifferenceOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.ExcludeTable)
		if err != nil {
			return []Table{}, err
		}
	}

	// 数据库查询获取自定义表结构转换规则
	var (
		tables []Table
		// 表名转换
		tableNameSlice []map[string]TableName
		// 表字段类型转换
		columnTypesMap map[string][]ColumnType
	)
	columnTypesMap = make(map[string][]ColumnType)

	// todo: 自定义表名适配删除 - 数据同步未处理表名不一致
	//customTableNameSlice, err := engine.GetCustomTableNameMap(cfg.SourceConfig.SchemaName)
	//if err != nil {
	//	return []Table{}, err
	//}

	for _, tbl := range exporterTableSlice {
		tableNameSlice = append(tableNameSlice, map[string]TableName{
			tbl: {
				SourceTableName: tbl,
				TargetTableName: tbl,
			},
		})
		// todo: 自定义表名适配删除 - 数据同步未处理表名不一致
		//if len(customTableNameSlice) != 0 {
		//	for _, tblName := range customTableNameSlice {
		//		if strings.ToUpper(tbl) == strings.ToUpper(tblName.SourceTableName) {
		//			if tblName.TargetTableName != "" {
		//				tableNameSlice = append(tableNameSlice, map[string]TableName{
		//					tbl: {
		//						SourceTableName: tbl,
		//						TargetTableName: tblName.TargetTableName,
		//					},
		//				})
		//			}
		//		}
		//	}
		//} else {
		//	tableNameSlice = append(tableNameSlice, map[string]TableName{
		//		tbl: {
		//			SourceTableName: tbl,
		//			TargetTableName: tbl,
		//		},
		//	})
		//}
	}

	customSchemaColumnTypeSlice, err := engine.GetCustomSchemaColumnTypeMap(cfg.SourceConfig.SchemaName)
	if err != nil {
		return []Table{}, err
	}
	customTableColumnTypeSlice, err := engine.GetCustomTableColumnTypeMap(cfg.SourceConfig.SchemaName)
	if err != nil {
		return []Table{}, err
	}

	// 加载字段类型转换规则
	// 字段类型转换规则判断，默认采用内置默认字段类型转换
	switch {
	case len(customSchemaColumnTypeSlice) != 0 && len(customTableColumnTypeSlice) == 0:
		for _, tbl := range exporterTableSlice {
			var colTypes []ColumnType
			for _, tblName := range customSchemaColumnTypeSlice {
				colTypes = append(colTypes, ColumnType{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.GetCustomSchemaColumnType(),
				})
			}
			columnTypesMap[tbl] = colTypes
		}
	case len(customSchemaColumnTypeSlice) == 0 && len(customTableColumnTypeSlice) != 0:
		for _, tbl := range exporterTableSlice {
			var colTypes []ColumnType
			for _, tblName := range customTableColumnTypeSlice {
				colTypes = append(colTypes, ColumnType{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.GetCustomTableColumnType(tbl),
				})
			}
			columnTypesMap[tbl] = colTypes
		}
	case len(customSchemaColumnTypeSlice) != 0 && len(customTableColumnTypeSlice) != 0:
		// 表字段类型优先级 > 库级别
		var customTableSlice []string
		// 获取所有任务表库级别字段类型
		for _, tbl := range exporterTableSlice {
			var colTypes []ColumnType
			for _, tblName := range customSchemaColumnTypeSlice {
				colTypes = append(colTypes, ColumnType{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.GetCustomSchemaColumnType(),
				})
			}
			columnTypesMap[tbl] = colTypes
		}

		// 加载获取自定义表字段类型转换规则
		// 处理情况:
		// - 自定义表字段类型规则不存在，而库字段类型存在的情况，则使用库字段类型转换规则
		// - 自定义表字段类型规则存在，而库字段类型也存在的情况，则使用表字段类型转换规则
		// - 两者都不存在，则不追加任何转换规则，字段类型转换时使用内置类型转换规则
		for _, tblName := range customTableColumnTypeSlice {
			if util.IsContainString(exporterTableSlice, tblName.SourceTableName) {
				tmpColTypes := columnTypesMap[tblName.SourceTableName]
				for idx, col := range tmpColTypes {
					if strings.ToUpper(tblName.SourceColumnType) == strings.ToUpper(col.SourceColumnType) {
						columnTypesMap[tblName.SourceTableName][idx].TargetColumnType = tblName.GetCustomTableColumnType(tblName.SourceTableName)
					} else {
						columnTypesMap[tblName.SourceTableName] = append(columnTypesMap[tblName.SourceTableName], ColumnType{
							SourceColumnType: tblName.SourceColumnType,
							TargetColumnType: tblName.GetCustomTableColumnType(tblName.SourceTableName),
						})
					}
				}
				customTableSlice = append(customTableSlice, tblName.SourceTableName)
			}
		}

		// 筛选过滤不属于自定义表字段类型规则的表并加载获取转换规则
		notLayInCustomTableSlice := util.FilterDifferenceStringItems(exporterTableSlice, customTableSlice)
		for _, tbl := range notLayInCustomTableSlice {
			var colTypes []ColumnType
			for _, tblName := range customSchemaColumnTypeSlice {
				colTypes = append(colTypes, ColumnType{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.GetCustomSchemaColumnType(),
				})
			}
			columnTypesMap[tbl] = colTypes
		}

	}

	// 返回需要转换 schema table
	for _, tbl := range exporterTableSlice {
		var table Table
		table.SourceSchemaName = cfg.SourceConfig.SchemaName
		table.TargetSchemaName = cfg.TargetConfig.SchemaName
		// 表名规则
		for _, t := range tableNameSlice {
			if _, ok := t[tbl]; ok {
				table.SourceTableName = t[tbl].SourceTableName
				table.TargetTableName = t[tbl].TargetTableName
			} else {
				table.SourceTableName = tbl
			}
		}
		// 表字段类型规则
		if _, ok := columnTypesMap[tbl]; ok {
			table.ColumnTypes = columnTypesMap[tbl]
		}
		table.Engine = engine
		table.Overwrite = cfg.TargetConfig.Overwrite
		tables = append(tables, table)
	}
	return tables, nil
}
