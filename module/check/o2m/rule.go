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
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"strconv"
	"strings"
)

/*
	Oracle 表规则映射检查
*/

func OracleTableColumnMapRuleCheck(
	sourceSchema, targetSchema, tableName, columnName string,
	oracleColInfo, mysqlColInfo Column) (string, table.Row, error) {
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

	// GBK 处理，统一 UTF8MB4 处理
	var (
		mysqlCharacterSet string
		mysqlCollation    string
	)
	if strings.ToUpper(common.OracleDBCharacterSetMap[oracleColInfo.CharacterSet]) == "GBK" {
		mysqlCharacterSet = strings.ToLower("UTF8MB4")
	} else {
		mysqlCharacterSet = strings.ToLower(common.OracleDBCharacterSetMap[oracleColInfo.CharacterSet])
	}
	mysqlCollation = strings.ToLower(common.OracleCollationMap[oracleColInfo.Collation])

	// 非加载 ORACLE 自定义规则，用于非设置自定义规则的表结构对比
	oracleColumnComment := common.SpecialLettersUsingMySQL([]byte(oracleColInfo.Comment))
	mysqlColumnComment := common.SpecialLettersUsingMySQL([]byte(mysqlColInfo.Comment))

	// 用于上下游对比 column meta (oracle 字段按规则转换之后的数据)
	oracleDiffColMeta := genColumnNullCommentDefaultMeta(oracleColInfo.NULLABLE, oracleColumnComment, oracleColInfo.DataDefault)
	mysqlDiffColMeta := genColumnNullCommentDefaultMeta(mysqlColInfo.NULLABLE, mysqlColumnComment, mysqlColInfo.DataDefault)

	// 用于上下游对比 column meta (最原始的上下游字段数据)
	oracleColMeta := genColumnNullCommentDefaultMeta(oracleColInfo.NULLABLE, oracleColumnComment, oracleColInfo.OracleOriginDataDefault)
	mysqlColMeta := genColumnNullCommentDefaultMeta(mysqlColInfo.NULLABLE, mysqlColInfo.Comment, mysqlColInfo.MySQLOriginDataDefault)

	// 字段类型判断
	// CHARACTER SET %s COLLATE %s（Only 作用于字符类型）
	switch oracleDataType {
	// 数字
	case "NUMBER":
		switch {
		case oracleDataScale > 0:
			// oracle 真实数据类型 number(*) -> number(38,127)
			// number  -> number(38,127)
			// number(*,x) ->  number(38,x)
			// decimal(x,y) -> y max 30
			switch {
			case oracleDataPrecision == 38 && oracleDataScale > 30:
				// 非自定义规则
				if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 65 && mysqlDataScale == 30 && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}

				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
					fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
					fmt.Sprintf("DECIMAL(65,30) %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"DECIMAL(65,30)",
					oracleColMeta,
				)
			case oracleDataPrecision == 38 && oracleDataScale <= 30:
				if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 65 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}

				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
					fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
					fmt.Sprintf("DECIMAL(65,%d) %s", oracleDataScale, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(65,%d)", oracleDataScale),
					oracleColMeta,
				)
			default:
				if oracleDataScale <= 30 {
					if mysqlDataType == "DECIMAL" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale && oracleDiffColMeta == mysqlDiffColMeta {
						return "", nil, nil
					}

					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
						fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
						fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
						oracleColMeta,
					)
				} else {
					if mysqlDataType == "DECIMAL" && oracleDataPrecision == mysqlDataPrecision && mysqlDataScale == 30 && oracleDiffColMeta == mysqlDiffColMeta {
						return "", nil, nil
					}

					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
						fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
						fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, 30, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, 30),
						oracleColMeta,
					)
				}
			}
		case oracleDataScale == 0:
			switch {
			case oracleDataPrecision >= 1 && oracleDataPrecision < 3:
				if mysqlDataType == "TINYINT" && mysqlDataPrecision >= 3 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}

				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("TINYINT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"TINYINT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 3 && oracleDataPrecision < 5:
				if mysqlDataType == "SMALLINT" && mysqlDataPrecision >= 5 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("SMALLINT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"SMALLINT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 5 && oracleDataPrecision < 9:
				if mysqlDataType == "INT" && mysqlDataPrecision >= 9 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("INT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"INT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 9 && oracleDataPrecision < 19:
				if mysqlDataType == "BIGINT" && mysqlDataPrecision >= 19 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("BIGINT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"BIGINT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 19 && oracleDataPrecision <= 38:
				if mysqlDataType == "DECIMAL" && mysqlDataPrecision >= 19 && mysqlDataPrecision <= 38 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("DECIMAL(%d) %s", oracleDataPrecision, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(%d)", oracleDataPrecision),
					oracleColMeta,
				)
			default:
				if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 65 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
					fmt.Sprintf("DECIMAL(%d) %s", 65, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"DECIMAL(65)",
					oracleColMeta,
				)
			}
		}
		return fixedMsg, tableRows, nil
	case "DECIMAL":
		switch {
		case oracleDataScale == 0 && oracleDataPrecision == 0:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 10 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL %s", oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL",
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		default:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == oracleDataPrecision && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
	case "DEC":
		switch {
		case oracleDataScale == 0 && oracleDataPrecision == 0:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 10 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL %s", oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL",
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		default:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == oracleDataPrecision && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
	case "DOUBLE PRECISION":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("DOUBLE PRECISION %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE PRECISION %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE PRECISION",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "FLOAT":
		if oracleDataPrecision == 0 {
			if mysqlDataType == "FLOAT" && mysqlDataPrecision == 12 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("FLOAT %s", oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("FLOAT %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"FLOAT",
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("FLOAT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "INTEGER":
		if mysqlDataType == "INT" && mysqlDataPrecision >= 10 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("INTEGER %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("INT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"INT",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "INT":
		if mysqlDataType == "INT" && mysqlDataPrecision >= 10 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("INT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("INT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"INT",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "REAL":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("REAL %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NUMERIC":
		if mysqlDataType == "DECIMAL" && mysqlDataPrecision == oracleDataPrecision && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NUMERIC(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "BINARY_FLOAT":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BINARY_FLOAT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "BINARY_DOUBLE":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BINARY_DOUBLE %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "SMALLINT":
		if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 38 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("SMALLINT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DECIMAL(38) %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DECIMAL(38)",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

		// 字符
	case "BFILE":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == 255 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BFILE %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(255) %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"VARCHAR(255)",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

	// 二进制
	case "LONG":
		if mysqlDataType == "LONGTEXT" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("LONG %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGTEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "LONG RAW":
		if mysqlDataType == "LONGBLOB" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("LONG RAW %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGBLOB %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGBLOB",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NCHAR VARYING":
		if mysqlDataType == "NCHAR VARYING" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NCHAR VARYING %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("NCHAR VARYING(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NCHAR VARYING(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NCLOB":
		if mysqlDataType == "TEXT" && mysqlDataLength == 65535 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NCLOB %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("TEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"TEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "RAW":
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if oracleDataLength < 256 {
		//	if mysqlDataType == "BINARY" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
		//		return "", nil, nil
		//	}
		//	tableRows = table.Row{tableName, columnName,
		//		fmt.Sprintf("RAW(%d) %s", oracleDataLength, oracleColMeta),
		//		fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
		//		fmt.Sprintf("BINARY(%d) %s", oracleDataLength, oracleColMeta)}
		//
		//	fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
		//		targetSchema,
		//		tableName,
		//		columnName,
		//		fmt.Sprintf("BINARY(%d)", oracleDataLength),
		//		mysqlCharacterSet,
		//		mysqlCollation,
		//		oracleColMeta,
		//	)
		//	return fixedMsg, tableRows, nil
		//}

		if mysqlDataType == "VARBINARY" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("RAW(%d) %s", oracleDataLength, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARBINARY(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARBINARY(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "XMLTYPE":
		if mysqlDataType == "LONGTEXT" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("XMLTYPE %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGTEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

		// 二进制
	case "CLOB":
		if mysqlDataType == "LONGTEXT" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("CLOB %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGTEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "BLOB":
		if mysqlDataType == "BLOB" && mysqlDataLength == 65535 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BLOB %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("BLOB %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

		// 时间
	case "DATE":
		if mysqlDataType == "DATETIME" && mysqlDataLength == 0 && mysqlDataPrecision == 0 && mysqlDataScale == 0 && mysqlDatetimePrecision == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("DATE %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DATETIME %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DATETIME",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

		// oracle 字符类型 bytes/char 判断 B/C
		// CHAR、NCHAR、VARCHAR2、NVARCHAR2( oracle 字符类型 B/C)
		// mysql 同等长度（data_length） char 字符类型 > oracle bytes 字节类型

	// 字符
	case "CHARACTER":
		if oracleDataLength < 256 {
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("CHARACTER(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("CHAR(%d) %s", oracleDataLength, oracleColMeta)}

			// 忽略 bytes -> char 语句修复输出
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
				return fixedMsg, tableRows, nil
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("CHAR(%d)", oracleDataLength),
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("CHARACTER(%d) %s", oracleDataLength, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "ROWID":
		if mysqlDataType == "CHAR" && mysqlDataLength == 10 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("ROWID %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("CHAR(10) %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"CHAR(10)",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "UROWID":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("UROWID %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "VARCHAR":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "CHAR":
		if oracleDataLength < 256 {
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && strings.EqualFold(oracleColumnCharUsed, "char") {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("CHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("CHAR(%d) %s", oracleDataLength, oracleColMeta)}

			// 忽略 bytes -> char 语句修复输出
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
				return fixedMsg, tableRows, nil
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("CHAR(%d)", oracleDataLength),
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("CHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}
		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NCHAR":
		if oracleDataLength < 256 {
			if mysqlDataType == "NCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("NCHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("NCHAR(%d) %s", oracleDataLength, oracleColMeta)}

			// 忽略 bytes -> char 语句修复输出
			if mysqlDataType == "NCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
				return fixedMsg, tableRows, nil
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("NCHAR(%d)", oracleDataLength),
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NVARCHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("NVARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NVARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "VARCHAR2":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("VARCHAR2(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NVARCHAR2":
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NVARCHAR2(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("NVARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NVARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

		// 默认其他类型
	default:
		if strings.Contains(oracleDataType, "INTERVAL") {
			if mysqlDataType == "VARCHAR" && mysqlDataLength == 30 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("VARCHAR(30) %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"VARCHAR(30)",
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		} else if strings.Contains(oracleDataType, "TIMESTAMP") {
			if strings.Contains(oracleDataType, "WITH TIME ZONE") || strings.Contains(oracleDataType, "WITH LOCAL TIME ZONE") {
				if oracleDataScale <= 6 {
					if mysqlDataType == "DATETIME" && mysqlDatetimePrecision == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
						return "", nil, nil
					}
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("DATETIME(%d) %s", oracleDataScale, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DATETIME(%d)", oracleDataScale),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				} else {
					// mysql/tidb 只支持精度 6，oracle 精度最大是 9，会检查出来但是保持原样
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("DATETIME(%d) %s", 6, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DATETIME(%d)", 6),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				}
			} else {
				if oracleDataScale <= 6 {
					if mysqlDataType == "TIMESTAMP" && mysqlDatetimePrecision == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
						return "", nil, nil
					}
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("TIMESTAMP(%d) %s", oracleDataScale, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("TIMESTAMP(%d)", oracleDataScale),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				} else {
					// mysql/tidb 只支持精度 6，oracle 精度最大是 9，会检查出来但是保持原样
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("TIMESTAMP(%d) %s", 6, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("TIMESTAMP(%d)", 6),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				}
			}
		} else {
			if mysqlDataType == "TEXT" && mysqlDataLength == 65535 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("TEXT %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"TEXT",
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
	}
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
			fmt.Errorf("oracle schema collation [%v] table collation [%v] column collation [%v] isn't support by getColumnCollation", schemaCollation, tableCollation, columnCollation)
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
			fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support by getColumnCollation", schemaCollation, tableCollation)
	} else {
		collation = strings.ToUpper(nlsComp)
		return collation, nil
	}
}
