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

	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
)

// 加载表以及库数据类型映射规则
func LoadOracleToMySQLMapRuleUsingTableAndSchema(engine *service.Engine, exporterTableSlice []string, sourceSchema, targetSchema string, overwrite bool) ([]Table, []string, error) {
	// 筛选过滤分区表并打印警告
	partitionTables, err := engine.FilterOraclePartitionTable(sourceSchema, exporterTableSlice)
	if err != nil {
		return []Table{}, partitionTables, err
	}

	if len(partitionTables) != 0 {
		service.Logger.Warn("partition tables",
			zap.String("schema", sourceSchema),
			zap.String("partition table list", fmt.Sprintf("%v", partitionTables)),
			zap.String("suggest", "if necessary, please manually convert and process the tables in the above list"))
	}

	// 数据库查询获取自定义表结构转换规则
	var (
		tables []Table
		// 表名映射 MAP
		tableNameSliceMap []map[string]TableNameMap
	)

	for _, tbl := range exporterTableSlice {
		tableNameSliceMap = append(tableNameSliceMap, map[string]TableNameMap{
			tbl: {
				SourceTableName: tbl,
				TargetTableName: tbl,
			},
		})
	}

	// 加载数据类型转换规则【处理表级别、库级别数据类型映射规则】
	// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
	columnTypesMap, err := LoadDataTypeSchemaOrTableMappingRule(sourceSchema, exporterTableSlice, engine)
	if err != nil {
		return []Table{}, partitionTables, err
	}

	// 返回需要转换 schema table
	for _, tbl := range exporterTableSlice {
		var table Table
		table.SourceSchemaName = sourceSchema
		table.TargetSchemaName = targetSchema
		// 表名规则
		for _, t := range tableNameSliceMap {
			if _, ok := t[tbl]; ok {
				table.SourceTableName = t[tbl].SourceTableName
				table.TargetTableName = t[tbl].TargetTableName
			} else {
				table.SourceTableName = tbl
			}
		}
		// 表字段类型规则
		if _, ok := columnTypesMap[tbl]; ok {
			table.ColumnTypesMap = columnTypesMap[tbl]
		}
		table.Engine = engine
		table.Overwrite = overwrite
		tables = append(tables, table)
	}
	return tables, partitionTables, nil
}

func LoadDataTypeSchemaOrTableMappingRule(sourceSchema string, exporterTableSlice []string, engine *service.Engine) (map[string][]ColumnTypeMap, error) {
	var columnTypesMap map[string][]ColumnTypeMap
	columnTypesMap = make(map[string][]ColumnTypeMap)

	tableDataTypeMapSlice, err := engine.GetTableDataTypeMap(sourceSchema)
	if err != nil {
		return columnTypesMap, err
	}
	schemaDataTypeMapSlice, err := engine.GetSchemaDataTypeMap(sourceSchema)
	if err != nil {
		return columnTypesMap, err
	}

	switch {
	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) == 0:
		columnTypesMap = loadDataTypeRuleOnlyUsingTable(exporterTableSlice, tableDataTypeMapSlice)

	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) != 0:
		columnTypesMap = loadDataTypeRuleUsingTableAndSchema(sourceSchema, exporterTableSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) != 0:
		columnTypesMap = loadDataTypeRuleOnlyUsingSchema(exporterTableSlice, sourceSchema, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) == 0:
		// 不作任何处理，默认使用内置数据规则映射
	default:
		return columnTypesMap, fmt.Errorf("oracle data type mapping failed, tableDataTypeMapSlice [%v],schemaDataTypeMapSlice [%v]", len(tableDataTypeMapSlice), len(schemaDataTypeMapSlice))
	}
	return columnTypesMap, nil
}

func loadDataTypeRuleOnlyUsingTable(exporterTableSlice []string, tableDataTypeMapSlice []service.TableDataTypeMap) map[string][]ColumnTypeMap {
	var columnTypesMap map[string][]ColumnTypeMap
	columnTypesMap = make(map[string][]ColumnTypeMap)

	for _, tbl := range exporterTableSlice {
		var colTypes []ColumnTypeMap
		for _, tblName := range tableDataTypeMapSlice {
			if strings.ToUpper(tbl) == strings.ToUpper(tblName.SourceTableName) {
				colTypes = append(colTypes, ColumnTypeMap{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.AdjustTableDataType(tblName.SourceTableName),
				})
			}
		}
		columnTypesMap[tbl] = colTypes
	}
	return columnTypesMap
}

func loadDataTypeRuleOnlyUsingSchema(exporterTableSlice []string, sourceSchema string, schemaDataTypeMapSlice []service.SchemaDataTypeMap) map[string][]ColumnTypeMap {
	var columnTypesMap map[string][]ColumnTypeMap
	columnTypesMap = make(map[string][]ColumnTypeMap)

	for _, tbl := range exporterTableSlice {
		var colTypes []ColumnTypeMap
		for _, tblName := range schemaDataTypeMapSlice {
			if strings.ToUpper(sourceSchema) == strings.ToUpper(tblName.SourceSchemaName) {
				colTypes = append(colTypes, ColumnTypeMap{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.AdjustSchemaDataType(),
				})
			}
		}
		columnTypesMap[tbl] = colTypes
	}
	return columnTypesMap
}

func loadDataTypeRuleUsingTableAndSchema(sourceSchema string, exporterTableSlice []string, tableDataTypeMapSlice []service.TableDataTypeMap, schemaDataTypeMapSlice []service.SchemaDataTypeMap) map[string][]ColumnTypeMap {
	var columnTypesMap map[string][]ColumnTypeMap
	columnTypesMap = make(map[string][]ColumnTypeMap)

	// 表字段类型优先级 > 库级别
	var customTableSlice []string
	// 获取所有任务表库级别字段类型
	for _, tbl := range exporterTableSlice {
		var colTypes []ColumnTypeMap
		for _, tblName := range schemaDataTypeMapSlice {
			if strings.ToUpper(sourceSchema) == strings.ToUpper(tblName.SourceSchemaName) {
				colTypes = append(colTypes, ColumnTypeMap{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.AdjustSchemaDataType(),
				})
			}
		}
		columnTypesMap[tbl] = colTypes
	}

	// 加载获取自定义表字段类型转换规则
	// 处理情况:
	// - 自定义表字段类型规则不存在，而库字段类型存在的情况，则使用库字段类型转换规则
	// - 自定义表字段类型规则存在，而库字段类型也存在的情况，则使用表字段类型转换规则
	// - 两者都不存在，则不追加任何转换规则，字段类型转换时使用内置类型转换规则
	for _, tblName := range tableDataTypeMapSlice {
		if utils.IsContainString(exporterTableSlice, tblName.SourceTableName) {
			tmpColTypes := columnTypesMap[tblName.SourceTableName]
			for idx, col := range tmpColTypes {
				if strings.ToUpper(tblName.SourceColumnType) == strings.ToUpper(col.SourceColumnType) {
					columnTypesMap[tblName.SourceTableName][idx].TargetColumnType = tblName.AdjustTableDataType(tblName.SourceTableName)
				} else {
					columnTypesMap[tblName.SourceTableName] = append(columnTypesMap[tblName.SourceTableName], ColumnTypeMap{
						SourceColumnType: tblName.SourceColumnType,
						TargetColumnType: tblName.AdjustTableDataType(tblName.SourceTableName),
					})
				}
			}
			customTableSlice = append(customTableSlice, tblName.SourceTableName)
		}
	}

	// 筛选过滤不属于自定义表字段类型规则的表并加载获取转换规则
	notLayInCustomTableSlice := utils.FilterDifferenceStringItems(exporterTableSlice, customTableSlice)
	for _, tbl := range notLayInCustomTableSlice {
		var colTypes []ColumnTypeMap
		for _, tblName := range schemaDataTypeMapSlice {
			if strings.ToUpper(sourceSchema) == strings.ToUpper(tblName.SourceSchemaName) {
				colTypes = append(colTypes, ColumnTypeMap{
					SourceColumnType: tblName.SourceColumnType,
					TargetColumnType: tblName.AdjustSchemaDataType(),
				})
			}
		}
		columnTypesMap[tbl] = colTypes
	}

	return columnTypesMap
}
