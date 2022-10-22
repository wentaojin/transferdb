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
package o2m

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/transferdb/pkg/reverse"
	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
)

/*
Oracle 表字段映射转换 -> Reverse 阶段
*/
func ReverseOracleTableColumnMapRule(
	sourceSchema, sourceTableName, columnName string,
	dataType, dataNullable, comments, dataDefault string,
	dataScaleValue, dataPrecisionValue, dataLengthValue, colCollation string, engine *service.Engine) (string, error) {
	var (
		// 字段元数据
		columnMeta string
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
		// 转换字段类型
		modifyColumnType string
		// 字段排序规则
		columnCollation string
	)
	// 字段排序规则检查
	if collationMapVal, ok := utils.OracleCollationMap[strings.ToUpper(colCollation)]; ok {
		columnCollation = collationMapVal
	} else {
		if !strings.EqualFold(strings.ToUpper(colCollation), "") {
			return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`, columnName, dataType, colCollation, comments, dataDefault, dataNullable)
		}
	}

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
	columnDataTypeMapSlice, err := engine.GetColumnRuleMap(sourceSchema, sourceTableName, utils.ReverseModeO2M)
	if err != nil {
		return columnMeta, err
	}
	tableDataTypeMapSlice, err := engine.GetTableRuleMap(sourceSchema, sourceTableName, utils.ReverseModeO2M)
	if err != nil {
		return columnMeta, err
	}
	schemaDataTypeMapSlice, err := engine.GetSchemaRuleMap(sourceSchema, utils.ReverseModeO2M)
	if err != nil {
		return columnMeta, err
	}
	defaultValueMapSlice, err := engine.GetDefaultValueMap(utils.ReverseModeO2M)
	if err != nil {
		return columnMeta, err
	}

	switch strings.ToUpper(dataType) {
	case "NUMBER":
		switch {
		case dataScale > 0:
			switch {
			// oracle 真实数据类型 number(*) -> number(38,127)
			// number  -> number(38,127)
			// number(*,x) ->  number(38,x)
			// decimal(x,y) -> y max 30
			case dataPrecision == 38 && dataScale > 30:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, 30)
			case dataPrecision == 38 && dataScale <= 30:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
			default:
				if dataScale <= 30 {
					originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
				} else {
					originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, 30)
				}
			}
		case dataScale == 0:
			switch {
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
				buildInColumnType = fmt.Sprintf("DECIMAL(%d)", 65)
			}
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DECIMAL":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DEC":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if dataLength < 256 {
		//	buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		//} else {
		//	buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		//}
		buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
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
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeO2M, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	}
	return columnMeta, nil
}
