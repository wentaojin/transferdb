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
	"strconv"
	"strings"

	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
)

// 表数据类型转换
func ReverseOracleTableColumnMapRule(
	sourceSchema, sourceTableName, columnName, dataType, dataNullable, comments, dataDefault string,
	dataScaleValue, dataPrecisionValue, dataLengthValue string, planColumnTypes []ColumnTypeMap, customColumnDataTypeMap []service.ColumnDataTypeMap) (string, error) {
	var (
		// 字段元数据
		columnMeta string
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
		// 转换字段类型
		modifyColumnType string
	)
	dataLength, err := strconv.Atoi(dataLengthValue)
	if err != nil {
		return columnMeta, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, sourceTableName, err)
	}
	dataPrecision, err := strconv.Atoi(dataPrecisionValue)
	if err != nil {
		return columnMeta, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, sourceTableName, err)
	}
	dataScale, err := strconv.Atoi(dataScaleValue)
	if err != nil {
		return columnMeta, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, sourceTableName, err)
	}

	switch strings.ToUpper(dataType) {
	case "NUMBER":
		switch {
		case dataScale > 0:
			originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		case dataScale == 0:
			switch {
			case dataPrecision == 0 && dataScale == 0:
				originColumnType = "NUMBER"
				buildInColumnType = "DECIMAL(65,30)"
			case dataPrecision >= 1 && dataPrecision < 3:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "TINYINT"
			case dataPrecision >= 3 && dataPrecision < 5:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "SMALLINT"
			case dataPrecision >= 5 && dataPrecision < 9:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "INT"
			case dataPrecision >= 9 && dataPrecision < 19:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "BIGINT"
			case dataPrecision >= 19 && dataPrecision <= 38:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d)", dataPrecision)
			default:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,4)", dataPrecision)
			}
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DECIMAL":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DEC":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	default:
		if strings.Contains(dataType, "INTERVAL") {
			originColumnType = dataType
			buildInColumnType = "VARCHAR(30)"
		} else if strings.Contains(dataType, "TIMESTAMP") {
			originColumnType = dataType
			if strings.Contains(dataType, "WITH TIME ZONE") || strings.Contains(dataType, "WITH LOCAL TIME ZONE") {
				if dataPrecision <= 6 {
					buildInColumnType = fmt.Sprintf("DATETIME(%d)", dataPrecision)
				} else {
					buildInColumnType = fmt.Sprintf("DATETIME(%d)", 6)
				}
			} else {
				if dataPrecision <= 6 {
					buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", dataPrecision)
				} else {
					buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", 6)
				}
			}
		} else {
			originColumnType = dataType
			buildInColumnType = "TEXT"
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, planColumnTypes, buildInColumnType, customColumnDataTypeMap)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	}
	return columnMeta, nil
}

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
			strings.ToUpper(tbl): {
				SourceTableName: strings.ToUpper(tbl),
				TargetTableName: strings.ToUpper(tbl),
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
		upperTblName := strings.ToUpper(tblName.SourceTableName)

		if utils.IsContainString(exporterTableSlice, upperTblName) {
			tmpColTypes := columnTypesMap[upperTblName]
			for idx, col := range tmpColTypes {
				if strings.ToUpper(tblName.SourceColumnType) == strings.ToUpper(col.SourceColumnType) {
					columnTypesMap[upperTblName][idx].TargetColumnType = tblName.AdjustTableDataType(upperTblName)
				} else {
					columnTypesMap[upperTblName] = append(columnTypesMap[upperTblName], ColumnTypeMap{
						SourceColumnType: tblName.SourceColumnType,
						TargetColumnType: tblName.AdjustTableDataType(upperTblName),
					})
				}
			}
			customTableSlice = append(customTableSlice, upperTblName)
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
