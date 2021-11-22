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
	"regexp"
	"strconv"
	"strings"

	"github.com/wentaojin/transferdb/service"
)

// 表数据类型转换
func ReverseOracleTableColumnMapRule(
	sourceSchema, sourceTableName, columnName, dataType, dataNullable, comments, dataDefault string,
	dataScaleValue, dataPrecisionValue, dataLengthValue string, engine *service.Engine) (string, error) {
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

	// 获取自定义映射规则
	columnDataTypeMapSlice, err := engine.GetColumnDataTypeMap(sourceSchema, sourceTableName)
	if err != nil {
		return columnMeta, err
	}
	tableDataTypeMapSlice, err := engine.GetTableDataTypeMap(sourceSchema, sourceTableName)
	if err != nil {
		return columnMeta, err
	}
	schemaDataTypeMapSlice, err := engine.GetSchemaDataTypeMap(sourceSchema)
	if err != nil {
		return columnMeta, err
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
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
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
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
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
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	default:
		if strings.Contains(dataType, "INTERVAL") {
			originColumnType = dataType
			buildInColumnType = "VARCHAR(30)"
		} else if strings.Contains(dataType, "TIMESTAMP") {
			originColumnType = dataType
			if strings.Contains(dataType, "WITH TIME ZONE") || strings.Contains(dataType, "WITH LOCAL TIME ZONE") {
				if dataScale <= 6 {
					buildInColumnType = fmt.Sprintf("DATETIME(%d)", dataScale)
				} else {
					buildInColumnType = fmt.Sprintf("DATETIME(%d)", 6)
				}
			} else {
				if dataScale <= 6 {
					buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", dataScale)
				} else {
					buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", 6)
				}
			}
		} else {
			originColumnType = dataType
			buildInColumnType = "TEXT"
		}
		modifyColumnType = changeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	}
	return columnMeta, nil
}

func changeOracleTableName(sourceTableName string, targetTableName string) string {
	if targetTableName == "" {
		return sourceTableName
	}
	if targetTableName != "" {
		return targetTableName
	}
	return sourceTableName
}

// 数据库查询获取自定义表结构转换规则
// 加载数据类型转换规则【处理字段级别、表级别、库级别数据类型映射规则】
// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
func changeOracleTableColumnType(columnName string, originColumnType string, buildInColumnType string,
	columnDataTypeMapSlice []service.ColumnDataTypeMap, tableDataTypeMapSlice []service.TableDataTypeMap,
	schemaDataTypeMapSlice []service.SchemaDataTypeMap) string {

	if len(columnDataTypeMapSlice) == 0 {
		return loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)
	}

	columnTypeFromColumn := loadDataTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice)
	columnTypeFromOther := loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	switch {
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther == buildInColumnType:
		return columnTypeFromColumn
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther != buildInColumnType:
		return columnTypeFromColumn
	case columnTypeFromColumn == buildInColumnType && columnTypeFromOther != buildInColumnType:
		return columnTypeFromOther
	default:
		return buildInColumnType
	}
}

func generateOracleTableColumnMetaByType(columnName, columnType, dataNullable, comments, dataDefault string) string {
	var (
		nullable string
		colMeta  string
	)

	columnName = strings.ToLower(columnName)
	columnType = strings.ToLower(columnType)

	if dataNullable == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	var comment string
	if comments != "" {
		if strings.Contains(comments, "\"") {
			comments = strings.Replace(comments, "\"", "'", -1)
		}
		match, _ := regexp.MatchString("'(.*)'", comments)
		if match {
			comment = fmt.Sprintf("\"%s\"", comments)
		} else {
			comment = fmt.Sprintf("'%s'", comments)
		}
	}

	if nullable == "NULL" {
		switch {
		case comment != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", columnName, columnType, dataDefault, comment)
		case comment != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COMMENT %s", columnName, columnType, comment)
		case comment == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault)
		case comment == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s", columnName, columnType)
		}
	} else {
		switch {
		case comment != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", columnName, columnType, nullable, dataDefault, comment)
			return colMeta
		case comment != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s %s COMMENT %s", columnName, columnType, nullable, comment)
		case comment == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault)
			return colMeta
		case comment == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable)
		}
	}
	return colMeta
}

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableDataTypeMap,
	schemaDataTypeMapSlice []service.SchemaDataTypeMap) string {
	switch {
	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) == 0:
		return loadDataTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleUsingTableAndSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) == 0:
		return buildInColumnType
	default:
		panic(fmt.Errorf("oracle data type mapping failed, tableDataTypeMapSlice [%v],schemaDataTypeMapSlice [%v]", len(tableDataTypeMapSlice), len(schemaDataTypeMapSlice)))
	}
}

func loadDataTypeRuleOnlyUsingTable(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableDataTypeMap) string {
	if len(tableDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, tbl := range tableDataTypeMapSlice {
		if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
			return tbl.TargetColumnType
		}
	}
	return buildInColumnType
}

func loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType string, schemaDataTypeMapSlice []service.SchemaDataTypeMap) string {
	if len(schemaDataTypeMapSlice) == 0 {
		return buildInColumnType
	}

	for _, tbl := range schemaDataTypeMapSlice {
		if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
			return tbl.TargetColumnType
		}
	}
	return buildInColumnType
}

func loadDataTypeRuleUsingTableAndSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableDataTypeMap, schemaDataTypeMapSlice []service.SchemaDataTypeMap) string {
	// 规则判断
	customTableDataType := loadDataTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	customSchemaDataType := loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

	switch {
	case customTableDataType == buildInColumnType && customSchemaDataType != buildInColumnType:
		return customSchemaDataType
	case customTableDataType != buildInColumnType && customSchemaDataType == buildInColumnType:
		return customTableDataType
	case customTableDataType != buildInColumnType && customSchemaDataType != buildInColumnType:
		return customTableDataType
	default:
		return buildInColumnType
	}
}

func loadDataTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []service.ColumnDataTypeMap) string {
	if len(columnDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, col := range columnDataTypeMapSlice {
		if strings.ToUpper(col.SourceColumnName) == strings.ToUpper(columnName) &&
			strings.ToUpper(col.SourceColumnType) == strings.ToUpper(originColumnType) &&
			col.TargetColumnType != "" {
			return col.TargetColumnType
		}
	}
	return buildInColumnType
}
