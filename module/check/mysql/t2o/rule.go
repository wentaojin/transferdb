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
package t2o

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/module/check/mysql/public"
	"strconv"
	"strings"
)

/*
	MySQL 表规则映射检查
*/

func MySQLTableColumnMapRuleCheck(
	sourceSchema, targetSchema, tableName, columnName string,
	oracleColInfo, mysqlColInfo public.Column) (string, table.Row, error) {
	var tableRows table.Row

	// 字段精度类型转换
	oracleDataLength, err := strconv.Atoi(oracleColInfo.DataLength)
	if err != nil {
		return "", nil, fmt.Errorf("oracle schema [%s] table [%s] column data_length string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataPrecision, err := strconv.Atoi(oracleColInfo.DataPrecision)
	if err != nil {
		return "", nil, fmt.Errorf("oracle schema [%s] table [%s] column data_precision string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataScale, err := strconv.Atoi(oracleColInfo.DataScale)
	if err != nil {
		return "", nil, fmt.Errorf("oracle schema [%s] table [%s] column data_scale string to int failed: %v", sourceSchema, tableName, err)
	}

	mysqlDataLength, err := strconv.Atoi(mysqlColInfo.DataLength)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] column data_length string to int failed: %v",
			targetSchema, tableName, err)
	}
	mysqlDataPrecision, err := strconv.Atoi(mysqlColInfo.DataPrecision)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] reverser column data_precision string to int failed: %v", targetSchema, tableName, err)
	}
	mysqlDataScale, err := strconv.Atoi(mysqlColInfo.DataScale)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] reverser column data_scale string to int failed: %v", targetSchema, tableName, err)
	}
	mysqlDatetimePrecision, err := strconv.Atoi(mysqlColInfo.DatetimePrecision)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] reverser column datetime_precision string to int failed: %v", targetSchema, tableName, err)
	}
	// 字段默认值、注释判断
	mysqlDataType := strings.ToUpper(mysqlColInfo.DataType)
	oracleDataType := strings.ToUpper(oracleColInfo.DataType)
	var (
		fixedMsg             string
		oracleColumnCharUsed string
	)

	// Oracle 字符型数据类型 bytes/char
	if oracleColInfo.CharUsed == "C" {
		oracleColumnCharUsed = "char"
	} else if oracleColInfo.CharUsed == "B" {
		oracleColumnCharUsed = "bytes"
	} else {
		oracleColumnCharUsed = "unknown"
	}

	// 字符集以及排序规则处理
	var (
		oracleCharacterSet string
		oracleCollation    string
	)
	oracleCharacterSet = strings.ToLower(common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][mysqlColInfo.CharacterSet])

	oracleCollation = strings.Split(strings.ToLower(common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][mysqlColInfo.Collation][common.StringUPPER(oracleCharacterSet)]), "/")[0]

	// 非加载 ORACLE 自定义规则，用于非设置自定义规则的表结构对比
	oracleColumnComment := common.SpecialLettersUsingMySQL([]byte(oracleColInfo.Comment))
	mysqlColumnComment := common.SpecialLettersUsingMySQL([]byte(mysqlColInfo.Comment))

	// 用于上下游对比 column meta (oracle 字段按规则转换之后的数据)
	oracleDiffColMeta := genColumnNullCommentDefaultMeta(oracleColInfo.NULLABLE, oracleColumnComment, oracleColInfo.DataDefault)
	mysqlDiffColMeta := genColumnNullCommentDefaultMeta(mysqlColInfo.NULLABLE, mysqlColumnComment, mysqlColInfo.DataDefault)

	// 用于上下游对比 column meta (最原始的上下游字段数据)
	oracleColMeta := genColumnNullCommentDefaultMeta(oracleColInfo.NULLABLE, oracleColumnComment, oracleColInfo.OracleOriginDataDefault)
	mysqlColMeta := genColumnNullCommentDefaultMeta(mysqlColInfo.NULLABLE, mysqlColInfo.Comment, mysqlColInfo.MySQLOriginDataDefault)

	// mysql -> oracle 数据类型映射参考
	// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
	switch common.StringUPPER(mysqlDataType) {
	case common.BuildInMySQLDatatypeTinyint:
		// 非自定义规则
		if oracleDataType == "NUMBER" && (oracleDataPrecision >= 1 && oracleDataPrecision < 3) && oracleDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("TINYINT %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("NUMBER(3,0) %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER(3,0)",
			mysqlColMeta,
		)

	case common.BuildInMySQLDatatypeSmallint:
		if oracleDataType == "NUMBER" && (oracleDataPrecision >= 3 && oracleDataPrecision < 5) && oracleDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("SMALLINT %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("NUMBER(5,0) %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER(5,0)",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeMediumint:
		if oracleDataType == "NUMBER" && (oracleDataPrecision >= 5 && oracleDataPrecision < 7) && oracleDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("MEDIUMINT %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("NUMBER(7,0) %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER(7,0)",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeInt:
		if oracleDataType == "NUMBER" && (oracleDataPrecision >= 5 && oracleDataPrecision < 9) && oracleDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("INT %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("NUMBER(10,0) %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER(10,0)",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeBigint:
		if oracleDataType == "NUMBER" && (oracleDataPrecision >= 9 && oracleDataPrecision < 20) && oracleDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("INT %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("NUMBER(20,0) %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER(20,0)",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeFloat:
		if oracleDataType == "BINARY_FLOAT" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("FLOAT %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("BINARY_FLOAT %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BINARY_FLOAT",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeDouble:
		if oracleDataType == "BINARY_DOUBLE" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("DOUBLE %s", mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("BINARY_DOUBLE %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BINARY_DOUBLE",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeDecimal:
		if oracleDataType == "NUMBER" && (oracleDataPrecision >= 19 && oracleDataPrecision <= 38) && oracleDataScale == mysqlDataScale && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("%s(%d,%d) %s", oracleDataType, oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("NUMBER %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeReal:
		if oracleDataType == "BINARY_FLOAT" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("%s(%d,%d) %s", oracleDataType, oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("BINARY_FLOAT %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BINARY_FLOAT",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeYear:
		if oracleDataType == "NUMBER" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("%s(%d,%d) %s", oracleDataType, oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("NUMBER %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"NUMBER",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeTime:
		if oracleDataType == "DATE" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("%s(%d,%d) %s", oracleDataType, oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("DATE %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DATE",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeDate:
		if oracleDataType == "DATE" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("%s(%d,%d) %s", oracleDataType, oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("DATE %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DATE",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeDatetime:
		if oracleDataType == "DATE" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("%s(%d,%d) %s", oracleDataType, oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("DATE %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DATE",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeTimestamp:
		if oracleDataType == "TIMESTAMP" && oracleDataScale == mysqlDatetimePrecision && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}

		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataScale, oracleColMeta),
			fmt.Sprintf("TIMESTAMP(%d) %s", mysqlDatetimePrecision, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("TIMESTAMP(%d)", mysqlDatetimePrecision),
			mysqlColMeta,
		)

		// oracle 字符类型 bytes/char 判断 B/C
		// CHAR、NCHAR、VARCHAR2、NVARCHAR2( oracle 字符类型 B/C)
		// mysql 同等长度（data_length） char 字符类型 > oracle bytes 字节类型
	case common.BuildInMySQLDatatypeChar:
		if oracleDataType == "VARCHAR2" && oracleDataLength == mysqlDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataLength, oracleColMeta),
			fmt.Sprintf("VARCHAR2(%d) %s", mysqlDataLength, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR2(%d)", mysqlDataLength),
			oracleCharacterSet,
			oracleCollation,
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeVarchar:
		if oracleDataType == "VARCHAR2" && oracleDataLength == mysqlDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataLength, oracleColMeta),
			fmt.Sprintf("VARCHAR2(%d) %s", mysqlDataLength, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR2(%d)", mysqlDataLength),
			oracleCharacterSet,
			oracleCollation,
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeTinyText:
		if oracleDataType == "VARCHAR2" && oracleDataLength == mysqlDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataLength, oracleColMeta),
			fmt.Sprintf("VARCHAR2(%d) %s", mysqlDataLength, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR2(%d)", mysqlDataLength),
			oracleCharacterSet,
			oracleCollation,
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeText:
		if oracleDataType == "CLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataLength, oracleColMeta),
			fmt.Sprintf("CLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"CLOB",
			oracleCharacterSet,
			mysqlColMeta,
		)

	case common.BuildInMySQLDatatypeMediumText:
		if oracleDataType == "CLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataLength, oracleColMeta),
			fmt.Sprintf("CLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"CLOB",
			oracleCharacterSet,
			mysqlColMeta,
		)

	case common.BuildInMySQLDatatypeLongText:
		if oracleDataType == "CLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataLength, oracleColMeta),
			fmt.Sprintf("CLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"CLOB",
			oracleCharacterSet,
			mysqlColMeta,
		)

	case common.BuildInMySQLDatatypeBit:
		if oracleDataType == "RAW" && oracleDataPrecision == mysqlDataPrecision && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("RAW(%d) %s", mysqlDataPrecision, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("RAW(%d)", mysqlDataPrecision),
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeBinary:
		if oracleDataType == "RAW" && oracleDataPrecision == mysqlDataPrecision && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("RAW(%d) %s", mysqlDataPrecision, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("RAW(%d)", mysqlDataPrecision),
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeVarbinary:
		if oracleDataType == "RAW" && oracleDataPrecision == mysqlDataPrecision && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("RAW(%d) %s", mysqlDataPrecision, mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("RAW(%d)", mysqlDataPrecision),
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeTinyBlob:
		if oracleDataType == "BLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("BLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeBlob:
		if oracleDataType == "BLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("BLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeMediumBlob:
		if oracleDataType == "BLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("BLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeLongBlob:
		if oracleDataType == "BLOB" && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
			fmt.Sprintf("%s(%d) %s", oracleDataType, oracleDataPrecision, oracleColMeta),
			fmt.Sprintf("BLOB %s", mysqlColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			mysqlColMeta,
		)
	case common.BuildInMySQLDatatypeEnum:
		// originColumnType = "ENUM"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, rule.DataDefault, columnCollation, defaultValueMapSlice)

		// oracle isn't support data type ENUM, please manual check, skip

		return fixedMsg, nil, nil

	case common.BuildInMySQLDatatypeSet:
		// originColumnType = "SET"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		// columnMeta = reverse.GenTableColumnMeta(utils.ReverseModeM2O, columnName, modifyColumnType, dataNullable, comments, rule.DataDefault, columnCollation, defaultValueMapSlice)

		// oracle isn't support data type SET, please manual check, skip

		return fixedMsg, nil, nil

	default:
		return fixedMsg, nil, fmt.Errorf("mysql schema [%s] table [%s] reverser column meta info [%s] failed", sourceSchema, tableName, common.StringUPPER(mysqlColInfo.DataType))
	}

	return fixedMsg, tableRows, nil
}

func genColumnNullCommentDefaultMeta(dataNullable, comments, dataDefault string) string {
	var (
		colMeta string
	)

	if dataNullable == "NULL" {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("DEFAULT %s COMMENT '%s'", dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("DEFAULT NULL COMMENT '%s'", comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("DEFAULT %s", dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = "DEFAULT NULL"
		}
	} else {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s COMMENT '%s'", dataNullable, dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("%s COMMENT '%s'", dataNullable, comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s", dataNullable, dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = fmt.Sprintf("%s", dataNullable)
		}
	}
	return colMeta
}

func genTableColumnCollation(nlsComp string, oraCollation bool, schemaCollation, tableCollation, columnCollation string) (string, error) {
	var collation string
	if oraCollation {
		if columnCollation != "" {
			collation = strings.ToUpper(columnCollation)
			return collation, nil
		}
		if columnCollation == "" && tableCollation != "" {
			collation = strings.ToUpper(tableCollation)
			return collation, nil
		}
		if columnCollation == "" && tableCollation == "" && schemaCollation != "" {
			collation = strings.ToUpper(schemaCollation)
			return collation, nil
		}
		return collation,
			fmt.Errorf("mysql schema collation [%v] table collation [%v] column collation [%v] isn't support by getColumnCollation", schemaCollation, tableCollation, columnCollation)
	} else {
		collation = strings.ToUpper(nlsComp)
		return collation, nil
	}
}

func genTableCollation(nlsComp string, oracleCollation bool, schemaCollation, tableCollation string) (string, error) {
	var collation string
	if oracleCollation {
		if tableCollation != "" {
			collation = strings.ToUpper(tableCollation)
			return collation, nil
		}
		if tableCollation == "" && schemaCollation != "" {
			collation = strings.ToUpper(schemaCollation)
			return collation, nil
		}
		return collation,
			fmt.Errorf("mysql schema collation [%v] table collation [%v] isn't support by getColumnCollation", schemaCollation, tableCollation)
	} else {
		collation = strings.ToUpper(nlsComp)
		return collation, nil
	}
}
