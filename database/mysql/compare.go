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
package mysql

import (
	"database/sql"
	"fmt"
	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/common"
	"hash/crc32"
	"strconv"
	"strings"
	"sync/atomic"
)

func (m *MySQL) GetMySQLTableName(schemaName, tableName string) ([]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where UPPER(TABLE_SCHEMA) = '%s' AND UPPER(TABLE_NAME) IN (%s)`, strings.ToUpper(schemaName), strings.ToUpper(tableName)))
	if err != nil {
		return []string{}, err
	}
	if len(res) == 0 {
		return []string{}, nil
	}

	var tbls []string
	for _, r := range res {
		tbls = append(tbls, r["TABLE_NAME"])
	}

	return tbls, nil
}

func (m *MySQL) GetMySQLTableActualRows(mysqlQuery string) (int64, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, mysqlQuery)
	if err != nil {
		return 0, err
	}
	rowsCount, err := strconv.ParseInt(res[0]["COUNT(1)"], 10, 64)
	if err != nil {
		return rowsCount, fmt.Errorf("error on FUNC GetMySQLTableActualRows failed: %v", err)
	}
	return rowsCount, nil
}

func (m *MySQL) GetMySQLDataRowStrings(querySQL string) ([]string, *strset.Set, uint32, error) {
	var (
		cols     []string
		rowsTMP  []string
		rows     *sql.Rows
		err      error
		crc32SUM uint32
	)
	var crc32Value uint32 = 0

	stringSet := set.NewStringSet()

	rows, err = m.MySQLDB.Query(querySQL)
	if err != nil {
		return cols, stringSet, crc32Value, fmt.Errorf("general sql [%v] query failed: [%v]", querySQL, err.Error())
	}

	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, stringSet, crc32Value, fmt.Errorf("general sql [%v] query rows.Columns failed: [%v]", querySQL, err.Error())
	}

	// 用于判断字段值是数字还是字符
	var columnTypes []string
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return cols, stringSet, crc32Value, err
	}

	for _, ct := range colTypes {
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, stringSet, crc32Value, fmt.Errorf("general sql [%v] query rows.Columns failed: [%v]", querySQL, err.Error())
	}

	rawResult := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range rawResult {
		scans[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, stringSet, crc32Value, fmt.Errorf("general sql [%v] query rows.Scan failed: [%v]", querySQL, err.Error())
		}

		for i, raw := range rawResult {
			// ORACLE/MySQL 空字符串以及 NULL 统一NULL处理，忽略 MySQL 空字符串与 NULL 区别
			if raw == nil {
				rowsTMP = append(rowsTMP, fmt.Sprintf("%v", `NULL`))
			} else if string(raw) == "" {
				rowsTMP = append(rowsTMP, fmt.Sprintf("%v", `NULL`))
			} else {
				switch columnTypes[i] {
				case "int8":
					r, err := common.StrconvIntBitSize(string(raw), 8)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "int16":
					r, err := common.StrconvIntBitSize(string(raw), 16)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "int32", "sql.NullInt32":
					r, err := common.StrconvIntBitSize(string(raw), 32)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "int64", "sql.NullInt64":
					r, err := common.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "uint8":
					r, err := common.StrconvUintBitSize(string(raw), 8)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "uint16":
					r, err := common.StrconvUintBitSize(string(raw), 16)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "uint32":
					r, err := common.StrconvUintBitSize(string(raw), 32)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "uint64":
					r, err := common.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "float32":
					r, err := common.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "float64", "sql.NullFloat64":
					r, err := common.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "rune":
					r, err := common.StrconvRune(string(raw))
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				default:
					// 特殊字符
					rowsTMP = append(rowsTMP, fmt.Sprintf("'%v'", common.SpecialLettersUsingMySQL(raw)))
				}
			}
		}

		rowS := exstrings.Join(rowsTMP, ",")

		// 计算 CRC32
		crc32SUM = atomic.AddUint32(&crc32Value, crc32.ChecksumIEEE([]byte(rowS)))
		stringSet.Add(rowS)

		// 数组清空
		rowsTMP = rowsTMP[0:0]
	}

	if err = rows.Err(); err != nil {
		return cols, stringSet, crc32Value, fmt.Errorf("general sql [%v] query rows.Next failed: [%v]", querySQL, err.Error())
	}

	return cols, stringSet, crc32SUM, err
}
