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
package m2o

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/wentaojin/transferdb/pkg/reverse"
	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
)

/*
	MySQL 表字段映射转换 -> Reverse 阶段
*/
func ReverseMySQLTableColumnMapRule(
	sourceSchema, sourceTableName, columnName string,
	dataType, dataNullable, comments, dataDefault string,
	dataScaleValue, dataPrecisionValue, dataLengthValue, datePrecisionValue, columnCollation string, engine *service.Engine) (string, error) {

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
		return columnMeta, fmt.Errorf("mysql schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, sourceTableName, err)
	}

	// https://stackoverflow.com/questions/5634104/what-is-the-size-of-column-of-int11-in-mysql-in-bytes
	// MySQL Numeric Data Type Only in term of display, t
	// INT(x) will make difference only in term of display, that is to show the number in x digits, and not restricted to 11.
	// You pair it using ZEROFILL, which will prepend the zeros until it matches your length.
	// So, for any number of x in INT(x) if the stored value has less digits than x, ZEROFILL will prepend zeros.
	// 忽略数值类型的长度

	dataPrecision, err := strconv.Atoi(dataPrecisionValue)
	if err != nil {
		return columnMeta, fmt.Errorf("mysql schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, sourceTableName, err)
	}
	dataScale, err := strconv.Atoi(dataScaleValue)
	if err != nil {
		return columnMeta, fmt.Errorf("mysql schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, sourceTableName, err)
	}
	datePrecision, err := strconv.Atoi(datePrecisionValue)
	if err != nil {
		return columnMeta, fmt.Errorf("mysql schema [%s] table [%s] reverser column date_precision string to int failed: %v", sourceSchema, sourceTableName, err)
	}

	// 获取自定义映射规则
	columnDataTypeMapSlice, err := engine.GetColumnRuleMap(sourceSchema, sourceTableName, utils.ReverseModeM2O)
	if err != nil {
		return columnMeta, err
	}
	tableDataTypeMapSlice, err := engine.GetTableRuleMap(sourceSchema, sourceTableName, utils.ReverseModeM2O)
	if err != nil {
		return columnMeta, err
	}
	schemaDataTypeMapSlice, err := engine.GetSchemaRuleMap(sourceSchema, utils.ReverseModeM2O)
	if err != nil {
		return columnMeta, err
	}
	defaultValueMapSlice, err := engine.GetDefaultValueMap(utils.ReverseModeM2O)
	if err != nil {
		return columnMeta, err
	}

	// mysql 默认值未区分，字符数据、数值数据，用于匹配 mysql 字符串默认值，判断是否需单引号
	// 默认值 uuid() 匹配到 xxx() 括号结尾，不需要单引号
	// 默认值 CURRENT_TIMESTAMP 不需要括号，内置转换成 ORACLE SYSDATE
	// 默认值 skp 或者 1 需要单引号
	var regDataDefault string
	reg, err := regexp.Compile(`^.+\(\)$`)
	if err != nil {
		return columnMeta, err
	}

	// mysql -> oracle 数据类型映射参考
	// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
	switch strings.ToUpper(dataType) {
	case "TINYINT":
		originColumnType = fmt.Sprintf("TINYINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(3,0)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "SMALLINT":
		originColumnType = fmt.Sprintf("SMALLINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(5,0)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "MEDIUMINT":
		originColumnType = fmt.Sprintf("MEDIUMINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(7,0)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "INT":
		originColumnType = fmt.Sprintf("INT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(10,0)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "BIGINT":
		originColumnType = fmt.Sprintf("BIGINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(19,0)"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "FLOAT":
		originColumnType = fmt.Sprintf("FLOAT(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = "BINARY_FLOAT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DOUBLE":
		originColumnType = fmt.Sprintf("DOUBLE(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = "BINARY_DOUBLE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "DECIMAL":
		originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = "NUMBER"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "REAL":
		originColumnType = "REAL"
		buildInColumnType = "BINARY_FLOAT"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
	case "YEAR":
		originColumnType = "YEAR"
		buildInColumnType = "NUMBER"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)

	case "TIME":
		originColumnType = fmt.Sprintf("TIME(%d)", datePrecision)
		buildInColumnType = "DATE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "DATETIME":
		originColumnType = fmt.Sprintf("DATETIME(%d)", datePrecision)
		buildInColumnType = "DATE"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "TIMESTAMP":
		originColumnType = fmt.Sprintf("TIMESTAMP(%d)", datePrecision)
		buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", datePrecision)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)

	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("CHAR(%d CHAR)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "TINYTEXT":
		originColumnType = "TINYTEXT"
		buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "TEXT":
		originColumnType = "TEXT"
		buildInColumnType = "CLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}

		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		columnCollation = ""
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "MEDIUMTEXT":
		originColumnType = "MEDIUMTEXT"
		buildInColumnType = "CLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}

		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		columnCollation = ""
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "LONGTEXT":
		originColumnType = "LONGTEXT"
		buildInColumnType = "CLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		columnCollation = ""
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)

	case "ENUM":
		// originColumnType = "ENUM"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
		return columnMeta, fmt.Errorf("oracle isn't support data type ENUM, please manual check")

	case "SET":
		// originColumnType = "SET"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, dataDefault, columnCollation, defaultValueMapSlice)
		return columnMeta, fmt.Errorf("oracle isn't support data type SET, please manual check")

	case "BIT":
		originColumnType = fmt.Sprintf("BIT(%d)", dataPrecision)
		buildInColumnType = fmt.Sprintf("RAW(%d)", dataPrecision)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)

	case "BINARY":
		originColumnType = fmt.Sprintf("BINARY(%d)", dataPrecision)
		buildInColumnType = fmt.Sprintf("RAW(%d)", dataPrecision)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "VARBINARY":
		originColumnType = fmt.Sprintf("VARBINARY(%d)", dataPrecision)
		buildInColumnType = fmt.Sprintf("RAW(%d)", dataPrecision)
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "TINYBLOB":
		originColumnType = "TINYBLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "MEDIUMBLOB":
		originColumnType = "MEDIUMBLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	case "LONGBLOB":
		originColumnType = "LONGBLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = reverse.ChangeTableColumnType(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice, tableDataTypeMapSlice, schemaDataTypeMapSlice)

		if reg.MatchString(dataDefault) || strings.EqualFold(dataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(dataDefault, "") {
			regDataDefault = dataDefault
		} else {
			regDataDefault = utils.StringsBuilder(`'`, dataDefault, `'`)
		}
		columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, regDataDefault, columnCollation, defaultValueMapSlice)
	default:
		return columnMeta, fmt.Errorf("mysql schema [%s] table [%s] reverser column meta info [%s] failed", sourceSchema, sourceTableName, strings.ToUpper(dataType))
	}
	return columnMeta, nil
}
