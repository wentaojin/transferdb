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
package public

import (
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"strconv"
)

type Column struct {
	DataType                string
	CharLength              string
	CharUsed                string
	CharacterSet            string
	Collation               string
	OracleOriginDataDefault string
	MySQLOriginDataDefault  string
	ColumnInfo
}

type ColumnInfo struct {
	DataLength        string
	DataPrecision     string
	DataScale         string
	DatetimePrecision string
	NULLABLE          string
	DataDefault       string
	Comment           string
}

func MySQLTableColumnMapOracleRule(sourceSchema, sourceTable string, column Column, buildinDatatypes []meta.BuildinDatatypeRule) (string, string, error) {

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

	buildinDatatypeMap := make(map[string]string)

	for _, b := range buildinDatatypes {
		buildinDatatypeMap[common.StringUPPER(b.DatatypeNameS)] = b.DatatypeNameT
	}

	// mysql -> oracle 数据类型映射参考
	// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
	switch common.StringUPPER(column.DataType) {
	case common.BuildInMySQLDatatypeTinyint:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeTinyint, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeTinyint]; ok {
			buildInColumnType = fmt.Sprintf("%s(3,0)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeTinyint)
		}

	case common.BuildInMySQLDatatypeSmallint:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeSmallint, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeSmallint]; ok {
			buildInColumnType = fmt.Sprintf("%s(5,0)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeSmallint)
		}
	case common.BuildInMySQLDatatypeMediumint:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeMediumint, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeMediumint]; ok {
			buildInColumnType = fmt.Sprintf("%s(7,0)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeMediumint)
		}
	case common.BuildInMySQLDatatypeInt:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeInt, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeInt]; ok {
			buildInColumnType = fmt.Sprintf("%s(10,0)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeInt)
		}
	case common.BuildInMySQLDatatypeBigint:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeBigint, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeBigint]; ok {
			buildInColumnType = fmt.Sprintf("%s(19,0)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeBigint)
		}
	case common.BuildInMySQLDatatypeFloat:
		originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInMySQLDatatypeFloat, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeFloat]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeFloat)
		}
	case common.BuildInMySQLDatatypeDouble:
		originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInMySQLDatatypeDouble, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeDouble]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeDouble)
		}
	case common.BuildInMySQLDatatypeDecimal:
		originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInMySQLDatatypeDecimal, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeDecimal]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeDecimal)
		}

	case common.BuildInMySQLDatatypeReal:
		originColumnType = common.BuildInMySQLDatatypeReal
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeReal]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeReal)
		}
	case common.BuildInMySQLDatatypeYear:
		originColumnType = common.BuildInMySQLDatatypeYear
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeYear]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeYear)
		}

	case common.BuildInMySQLDatatypeTime:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeTime, datePrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeTime]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeTime)
		}
	case common.BuildInMySQLDatatypeDate:
		originColumnType = common.BuildInMySQLDatatypeDate
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeDate]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeDate)
		}
	case common.BuildInMySQLDatatypeDatetime:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeDatetime, datePrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeDatetime]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeDatetime)
		}
	case common.BuildInMySQLDatatypeTimestamp:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeTimestamp, datePrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeTimestamp]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), datePrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeTimestamp)
		}
	case common.BuildInMySQLDatatypeChar:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeChar, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeChar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d CHAR)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeChar)
		}
	case common.BuildInMySQLDatatypeVarchar:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeVarchar, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeVarchar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d CHAR)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeVarchar)
		}
	case common.BuildInMySQLDatatypeTinyText:
		originColumnType = common.BuildInMySQLDatatypeTinyText
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeTinyText]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d CHAR)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeTinyText)
		}
	case common.BuildInMySQLDatatypeText:
		originColumnType = common.BuildInMySQLDatatypeText
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeText]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeText)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""

	case common.BuildInMySQLDatatypeMediumText:
		originColumnType = common.BuildInMySQLDatatypeMediumText
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeMediumText]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeMediumText)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""

	case common.BuildInMySQLDatatypeLongText:
		originColumnType = common.BuildInMySQLDatatypeLongText
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeLongText]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeLongText)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""

	case common.BuildInMySQLDatatypeBit:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeBit, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeBit]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeBit)
		}
	case common.BuildInMySQLDatatypeBinary:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeBinary, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeBinary]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeBinary)
		}
	case common.BuildInMySQLDatatypeVarbinary:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInMySQLDatatypeVarbinary, dataPrecision)
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeVarbinary]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeVarbinary)
		}
	case common.BuildInMySQLDatatypeTinyBlob:
		originColumnType = common.BuildInMySQLDatatypeTinyBlob
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeTinyBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeTinyBlob)
		}
	case common.BuildInMySQLDatatypeBlob:
		originColumnType = common.BuildInMySQLDatatypeBlob
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeBlob)
		}
	case common.BuildInMySQLDatatypeMediumBlob:
		originColumnType = common.BuildInMySQLDatatypeMediumBlob
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeMediumBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeMediumBlob)
		}
	case common.BuildInMySQLDatatypeLongBlob:
		originColumnType = common.BuildInMySQLDatatypeLongBlob
		if val, ok := buildinDatatypeMap[common.BuildInMySQLDatatypeLongBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql table column type [%s] map oracle column type rule isn't exist, please checkin", common.BuildInMySQLDatatypeLongBlob)
		}
	case common.BuildInMySQLDatatypeEnum:
		// originColumnType = "ENUM"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, rule.DataDefault, columnCollation, defaultValueMapSlice)
		return originColumnType, buildInColumnType, fmt.Errorf("oracle isn't support data type ENUM, please manual check")

	case common.BuildInMySQLDatatypeSet:
		// originColumnType = "SET"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, rule.DataDefault, columnCollation, defaultValueMapSlice)
		return originColumnType, buildInColumnType, fmt.Errorf("oracle isn't support data type SET, please manual check")

	default:
		return originColumnType, buildInColumnType, fmt.Errorf("mysql schema [%s] table [%s] reverser column meta info [%s] failed", sourceSchema, sourceTable, common.StringUPPER(column.DataType))
	}
}
