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
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/module/check/o2m"
	"strconv"
	"strings"
)

/*
MySQL 表字段映射转换 -> Reverse 阶段
*/
func MySQLTableColumnMapRule(sourceSchema, sourceTable string, column o2m.Column) (string, string, error) {

	var (

		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
	)

	dataLength, err := strconv.Atoi(column.DataLength)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("mysql schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, sourceTable, err)
	}

	// https://stackoverflow.com/questions/5634104/what-is-the-size-of-column-of-int11-in-mysql-in-bytes
	// MySQL Numeric Data Type Only in term of display, t
	// INT(x) will make difference only in term of display, that is to show the number in x digits, and not restricted to 11.
	// You pair it using ZEROFILL, which will prepend the zeros until it matches your length.
	// So, for any number of x in INT(x) if the stored value has less digits than x, ZEROFILL will prepend zeros.
	// 忽略数值类型的长度

	dataPrecision, err := strconv.Atoi(column.DataPrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("mysql schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, sourceTable, err)
	}
	dataScale, err := strconv.Atoi(column.DataScale)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("mysql schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, sourceTable, err)
	}
	datePrecision, err := strconv.Atoi(column.DatetimePrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("mysql schema [%s] table [%s] reverser column date_precision string to int failed: %v", sourceSchema, sourceTable, err)
	}

	// mysql -> oracle 数据类型映射参考
	// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
	switch strings.ToUpper(column.DataType) {
	case "TINYINT":
		originColumnType = fmt.Sprintf("TINYINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(3,0)"
		return originColumnType, buildInColumnType, nil

	case "SMALLINT":
		originColumnType = fmt.Sprintf("SMALLINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(5,0)"
		return originColumnType, buildInColumnType, nil

	case "MEDIUMINT":
		originColumnType = fmt.Sprintf("MEDIUMINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(7,0)"
		return originColumnType, buildInColumnType, nil

	case "INT":
		originColumnType = fmt.Sprintf("INT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(10,0)"
		return originColumnType, buildInColumnType, nil

	case "BIGINT":
		originColumnType = fmt.Sprintf("BIGINT(%d)", dataPrecision)
		buildInColumnType = "NUMBER(19,0)"
		return originColumnType, buildInColumnType, nil

	case "FLOAT":
		originColumnType = fmt.Sprintf("FLOAT(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = "BINARY_FLOAT"
		return originColumnType, buildInColumnType, nil

	case "DOUBLE":
		originColumnType = fmt.Sprintf("DOUBLE(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = "BINARY_DOUBLE"
		return originColumnType, buildInColumnType, nil

	case "DECIMAL":
		originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = "NUMBER"
		return originColumnType, buildInColumnType, nil

	case "REAL":
		originColumnType = "REAL"
		buildInColumnType = "BINARY_FLOAT"
		return originColumnType, buildInColumnType, nil

	case "YEAR":
		originColumnType = "YEAR"
		buildInColumnType = "NUMBER"
		return originColumnType, buildInColumnType, nil

	case "TIME":
		originColumnType = fmt.Sprintf("TIME(%d)", datePrecision)
		buildInColumnType = "DATE"

		return originColumnType, buildInColumnType, nil

	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATE"

		return originColumnType, buildInColumnType, nil

	case "DATETIME":
		originColumnType = fmt.Sprintf("DATETIME(%d)", datePrecision)
		buildInColumnType = "DATE"

		return originColumnType, buildInColumnType, nil

	case "TIMESTAMP":
		originColumnType = fmt.Sprintf("TIMESTAMP(%d)", datePrecision)
		buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", datePrecision)

		return originColumnType, buildInColumnType, nil

	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("CHAR(%d CHAR)", dataLength)

		return originColumnType, buildInColumnType, nil

	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)

		return originColumnType, buildInColumnType, nil

	case "TINYTEXT":
		originColumnType = "TINYTEXT"
		buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)

		return originColumnType, buildInColumnType, nil

	case "TEXT":
		originColumnType = "TEXT"
		buildInColumnType = "CLOB"

		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""
		return originColumnType, buildInColumnType, nil

	case "MEDIUMTEXT":
		originColumnType = "MEDIUMTEXT"
		buildInColumnType = "CLOB"

		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""
		return originColumnType, buildInColumnType, nil

	case "LONGTEXT":
		originColumnType = "LONGTEXT"
		buildInColumnType = "CLOB"

		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""
		return originColumnType, buildInColumnType, nil

	case "ENUM":
		// originColumnType = "ENUM"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, rule.DataDefault, columnCollation, defaultValueMapSlice)
		return originColumnType, buildInColumnType, fmt.Errorf("oracle isn't support data type ENUM, please manual check")

	case "SET":
		// originColumnType = "SET"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, rule.DataDefault, columnCollation, defaultValueMapSlice)
		return originColumnType, buildInColumnType, fmt.Errorf("oracle isn't support data type SET, please manual check")

	case "BIT":
		originColumnType = fmt.Sprintf("BIT(%d)", dataPrecision)
		buildInColumnType = fmt.Sprintf("RAW(%d)", dataPrecision)

		return originColumnType, buildInColumnType, nil

	case "BINARY":
		originColumnType = fmt.Sprintf("BINARY(%d)", dataPrecision)
		buildInColumnType = fmt.Sprintf("RAW(%d)", dataPrecision)

		return originColumnType, buildInColumnType, nil

	case "VARBINARY":
		originColumnType = fmt.Sprintf("VARBINARY(%d)", dataPrecision)
		buildInColumnType = fmt.Sprintf("RAW(%d)", dataPrecision)

		return originColumnType, buildInColumnType, nil

	case "TINYBLOB":
		originColumnType = "TINYBLOB"
		buildInColumnType = "BLOB"

		return originColumnType, buildInColumnType, nil

	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"

		return originColumnType, buildInColumnType, nil

	case "MEDIUMBLOB":
		originColumnType = "MEDIUMBLOB"
		buildInColumnType = "BLOB"

		return originColumnType, buildInColumnType, nil

	case "LONGBLOB":
		originColumnType = "LONGBLOB"
		buildInColumnType = "BLOB"

		return originColumnType, buildInColumnType, nil

	default:
		return originColumnType, buildInColumnType, fmt.Errorf("mysql schema [%s] table [%s] reverser column meta info [%s] failed", sourceSchema, sourceTable, common.StringUPPER(column.DataType))
	}
}
