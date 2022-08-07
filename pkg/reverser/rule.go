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

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"
)

/*
	Oracle 表字段映射转换 -> Reverse 阶段
*/
func ReverseOracleTableColumnMapRule(
	sourceSchema, sourceTableName, columnName string,
	dataType, dataNullable, comments, dataDefault string,
	dataScaleValue, dataPrecisionValue, dataLengthValue, columnCollation string, engine *service.Engine) (string, error) {
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
	columnDataTypeMapSlice, err := engine.GetColumnRuleMap(sourceSchema, sourceTableName)
	if err != nil {
		return columnMeta, err
	}
	tableDataTypeMapSlice, err := engine.GetTableRuleMap(sourceSchema, sourceTableName)
	if err != nil {
		return columnMeta, err
	}
	schemaDataTypeMapSlice, err := engine.GetSchemaRuleMap(sourceSchema)
	if err != nil {
		return columnMeta, err
	}
	defaultValueMapSlice, err := engine.GetDefaultValueMap()
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
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DECIMAL":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DEC":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if dataLength < 256 {
		//	buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		//} else {
		//	buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		//}
		buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
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
		modifyColumnType = ChangeOracleTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = genOracleTableColumnMeta(columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
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
func ChangeOracleTableColumnType(columnName string,
	originColumnType string,
	buildInColumnType string,
	columnDataTypeMapSlice []service.ColumnRuleMap,
	tableDataTypeMapSlice []service.TableRuleMap,
	schemaDataTypeMapSlice []service.SchemaRuleMap) string {

	if len(columnDataTypeMapSlice) == 0 {
		return loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)
	}

	columnTypeFromColumn := loadDataTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice)
	columnTypeFromOther := loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	switch {
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther == buildInColumnType:
		return strings.ToUpper(columnTypeFromColumn)
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther != buildInColumnType:
		return strings.ToUpper(columnTypeFromColumn)
	case columnTypeFromColumn == buildInColumnType && columnTypeFromOther != buildInColumnType:
		return strings.ToUpper(columnTypeFromOther)
	default:
		return strings.ToUpper(buildInColumnType)
	}
}

func genOracleTableColumnMeta(columnName, columnType, dataNullable, comments, defaultValue, columnCollation string,
	defaultValueMapSlice []service.DefaultValueMap) string {
	var (
		nullable    string
		colMeta     string
		dataDefault string
		comment     string
		collation   string
	)

	if dataNullable == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

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

	if columnCollation != "" {
		if val, ok := utils.OracleCollationMap[strings.ToUpper(columnCollation)]; ok {
			collation = val
		} else {
			panic(fmt.Errorf(`oracle table column meta generate failed, 
column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
				columnName, columnType, columnCollation, comment, defaultValue, nullable))
		}
	}

	if defaultValue != "" {
		dataDefault = LoadDataDefaultValueRule(defaultValue, defaultValueMapSlice)
	} else {
		dataDefault = defaultValue
	}

	if nullable == "NULL" {
		switch {
		case collation != "" && comment != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT %s", columnName, columnType, collation, dataDefault, comment)
		case collation != "" && comment == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", columnName, columnType, collation, dataDefault)
		case collation != "" && comment == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s", columnName, columnType, collation)
		case collation != "" && comment != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s COMMENT %s", columnName, columnType, collation, comment)
		case collation == "" && comment != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", columnName, columnType, dataDefault, comment)
		case collation == "" && comment == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault)
		case collation == "" && comment == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s", columnName, columnType)
		case collation == "" && comment != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COMMENT %s", columnName, columnType, comment)
		default:
			panic(fmt.Errorf(`oracle table column meta generate failed, 
column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
				columnName, columnType, collation, comment, defaultValue, nullable))
		}
	} else {
		switch {
		case collation != "" && comment != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT %s",
				columnName, columnType, collation, nullable, dataDefault, comment)
		case collation != "" && comment != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT %s", columnName, columnType, collation, nullable, comment)
		case collation != "" && comment == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", columnName, columnType, collation, nullable, dataDefault)
		case collation != "" && comment == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s", columnName, columnType, collation, nullable)
		case collation == "" && comment != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", columnName, columnType, nullable, dataDefault, comment)
		case collation == "" && comment != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s %s COMMENT %s", columnName, columnType, nullable, comment)
		case collation == "" && comment == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault)
		case collation == "" && comment == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable)
		default:
			panic(fmt.Errorf(`oracle table column meta generate failed, 
column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
				columnName, columnType, collation, comment, defaultValue, nullable))
		}
	}
	return colMeta
}

func LoadDataDefaultValueRule(defaultValue string, defaultValueMapSlice []service.DefaultValueMap) string {
	if len(defaultValueMapSlice) == 0 {
		return defaultValue
	}

	for _, dv := range defaultValueMapSlice {
		if strings.ToUpper(dv.SourceDefaultValue) == strings.ToUpper(defaultValue) && dv.TargetDefaultValue != "" {
			return dv.TargetDefaultValue
		}
	}
	return defaultValue
}

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableRuleMap,
	schemaDataTypeMapSlice []service.SchemaRuleMap) string {
	switch {
	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) == 0:
		return loadDataTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleUsingTableAndSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) == 0:
		return strings.ToUpper(buildInColumnType)
	default:
		panic(fmt.Errorf("oracle data type mapping failed, tableDataTypeMapSlice [%v],schemaDataTypeMapSlice [%v]", len(tableDataTypeMapSlice), len(schemaDataTypeMapSlice)))
	}
}

func loadDataTypeRuleUsingTableAndSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableRuleMap, schemaDataTypeMapSlice []service.SchemaRuleMap) string {
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
		return strings.ToUpper(buildInColumnType)
	}
}

// 表级别自定义映射规则
func loadDataTypeRuleOnlyUsingTable(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableRuleMap) string {
	if len(tableDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, tbl := range tableDataTypeMapSlice {
		/*
			number 类型处理：函数匹配 ->  GetOracleTableColumn
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number(38,127)
			- number -> number(38,127)
			- number(5) -> number(5)
			- number(8,9) -> number(8,9)
		*/
		if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "NUMBER") {
			switch {
			case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
				if strings.ToUpper(strings.Replace(tbl.SourceColumnType, "*", "38", -1)) == strings.ToUpper(originColumnType) &&
					tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
				if "NUMBER(38,127)" == strings.ToUpper(originColumnType) &&
					tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			case !strings.Contains(strings.ToUpper(tbl.SourceColumnType), "(") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ")"):
				if "NUMBER(38,127)" == strings.ToUpper(originColumnType) &&
					tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			default:
				if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			}
		} else {
			if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
		}
	}
	return strings.ToUpper(buildInColumnType)
}

// 库级别自定义映射规则
func loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType string, schemaDataTypeMapSlice []service.SchemaRuleMap) string {
	if len(schemaDataTypeMapSlice) == 0 {
		return buildInColumnType
	}

	for _, tbl := range schemaDataTypeMapSlice {
		/*
			number 类型处理：函数匹配 ->  GetOracleTableColumn
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number(38,127)
			- number -> number(38,127)
			- number(5) -> number(5)
			- number(8,9) -> number(8,9)
		*/
		if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "NUMBER") {
			switch {
			case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
				if strings.ToUpper(strings.Replace(tbl.SourceColumnType, "*", "38", -1)) == strings.ToUpper(originColumnType) &&
					tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
				if "NUMBER(38,127)" == strings.ToUpper(originColumnType) &&
					tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			case !strings.Contains(strings.ToUpper(tbl.SourceColumnType), "(") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ")"):
				if "NUMBER(38,127)" == strings.ToUpper(originColumnType) &&
					tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			default:
				if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			}
		} else {
			if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
		}
	}
	return strings.ToUpper(buildInColumnType)
}

// 字段级别自定义映射规则
func loadDataTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []service.ColumnRuleMap) string {
	if len(columnDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, tbl := range columnDataTypeMapSlice {
		if strings.ToUpper(tbl.SourceColumnName) == strings.ToUpper(columnName) {
			/*
				number 类型处理：函数匹配 ->  GetOracleTableColumn
				- number(*,10) -> number(38,10)
				- number(*,0) -> number(38,0)
				- number(*) -> number(38,127)
				- number -> number(38,127)
				- number(5) -> number(5)
				- number(8,9) -> number(8,9)
			*/
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "NUMBER") {
				switch {
				case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
					if strings.ToUpper(strings.Replace(tbl.SourceColumnType, "*", "38", -1)) == strings.ToUpper(originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
					if "NUMBER(38,127)" == strings.ToUpper(originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				case !strings.Contains(strings.ToUpper(tbl.SourceColumnType), "(") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ")"):
					if "NUMBER(38,127)" == strings.ToUpper(originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				default:
					if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				}
			} else {
				if strings.ToUpper(tbl.SourceColumnType) == strings.ToUpper(originColumnType) && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			}
		}
	}
	return strings.ToUpper(buildInColumnType)
}
