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
	"github.com/wentaojin/transferdb/module/check/o2m"
	"strconv"
	"strings"
)

func OracleTableColumnMapRule(sourceSchema, sourceTable string, column o2m.Column) (string, string, error) {
	var (
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
	)

	dataLength, err := strconv.Atoi(column.DataLength)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, sourceTable, err)
	}
	dataPrecision, err := strconv.Atoi(column.DataPrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, sourceTable, err)
	}
	dataScale, err := strconv.Atoi(column.DataScale)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, sourceTable, err)
	}

	switch strings.ToUpper(column.DataType) {
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
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = "TINYINT"
			case dataPrecision >= 3 && dataPrecision < 5:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = "SMALLINT"
			case dataPrecision >= 5 && dataPrecision < 9:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = "INT"
			case dataPrecision >= 9 && dataPrecision < 19:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = "BIGINT"
			case dataPrecision >= 19 && dataPrecision <= 38:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d)", dataPrecision)
			default:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
			}
		}
		return originColumnType, buildInColumnType, nil
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"

		return originColumnType, buildInColumnType, nil
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}

		return originColumnType, buildInColumnType, nil
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}

		return originColumnType, buildInColumnType, nil
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"

		return originColumnType, buildInColumnType, nil
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"

		return originColumnType, buildInColumnType, nil
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"

		return originColumnType, buildInColumnType, nil
	case "DECIMAL":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}

		return originColumnType, buildInColumnType, nil
	case "DEC":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}

		return originColumnType, buildInColumnType, nil
	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"

		return originColumnType, buildInColumnType, nil
	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}

		return originColumnType, buildInColumnType, nil
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"

		return originColumnType, buildInColumnType, nil
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"

		return originColumnType, buildInColumnType, nil
	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"

		return originColumnType, buildInColumnType, nil
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"

		return originColumnType, buildInColumnType, nil
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"

		return originColumnType, buildInColumnType, nil
	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"

		return originColumnType, buildInColumnType, nil
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}

		return originColumnType, buildInColumnType, nil
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"

		return originColumnType, buildInColumnType, nil
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)

		return originColumnType, buildInColumnType, nil
	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if dataLength < 256 {
		//	buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		//} else {
		//	buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		//}
		buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"

		return originColumnType, buildInColumnType, nil
	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"

		return originColumnType, buildInColumnType, nil
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"

		return originColumnType, buildInColumnType, nil
	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"

		return originColumnType, buildInColumnType, nil
	default:
		if strings.Contains(column.DataType, "INTERVAL") {
			originColumnType = column.DataType
			buildInColumnType = "VARCHAR(30)"
		} else if strings.Contains(column.DataType, "TIMESTAMP") {
			originColumnType = column.DataType
			if strings.Contains(column.DataType, "WITH TIME ZONE") || strings.Contains(column.DataType, "WITH LOCAL TIME ZONE") {
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
			originColumnType = column.DataType
			buildInColumnType = "TEXT"
		}
		return originColumnType, buildInColumnType, nil
	}
}
