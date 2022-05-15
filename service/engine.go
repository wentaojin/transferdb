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
package service

import (
	"database/sql"
	"fmt"
	"github.com/scylladb/go-set/strset"
	"hash/crc32"
	"strings"
	"sync/atomic"

	"github.com/scylladb/go-set"
	"github.com/thinkeridea/go-extend/exstrings"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/wentaojin/transferdb/utils"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

var (
	// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
	// 查询字段值 NULL
	// 如果字段值 = NULLABLE 则表示值是 NULL
	// 如果字段值 = "" 则表示值是空字符串
	// 如果字段值 = 'NULL' 则表示值是 NULL 字符串
	// 如果字段值 = 'null' 则表示值是 null 字符串
	IsNull = "NULLABLE"
)

// 定义数据库引擎
type Engine struct {
	OracleDB *sql.DB
	MysqlDB  *sql.DB
	GormDB   *gorm.DB
}

// 查询返回表字段列和对应的字段行数据
func Query(db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.Query(querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query failed: [%v]", querySQL, err.Error())
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Columns failed: [%v]", querySQL, err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("general sql [%v] query rows.Scan failed: [%v]", querySQL, err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			// 数据库类型 MySQL NULL 是 NULL，空字符串是空字符串
			// 数据库类型 Oracle NULL、空字符串归于一类 NULL
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if v == nil { // 处理 NULL 情况，当数据库类型 MySQL 等于 nil
				row[key] = IsNull
			} else {
				// 处理空字符串以及其他值情况
				// 数据统一 string 格式显示
				row[key] = string(v)
			}
		}
		res = append(res, row)
	}

	if err = rows.Err(); err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Next failed: [%v]", querySQL, err.Error())

	}
	return cols, res, nil
}

// 初始化同步表结构
func (e *Engine) InitMysqlEngineDB() error {
	if err := e.GormDB.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci").AutoMigrate(
		&ColumnRuleMap{},
		&TableRuleMap{},
		&SchemaRuleMap{},
		&DefaultValueMap{},
		&WaitSyncMeta{},
		&FullSyncMeta{},
		&IncrementSyncMeta{},
		&TableErrorDetail{},
		&DataDiffMeta{},
	); err != nil {
		return fmt.Errorf("init mysql engine db data failed: %v", err)
	}
	return nil
}

func (e *Engine) IsExistOracleSchema(schemaName string) error {
	schemas, err := e.getOracleSchema()
	if err != nil {
		return err
	}
	if !utils.IsContainString(schemas, strings.ToUpper(schemaName)) {
		return fmt.Errorf("oracle schema [%s] isn't exist in the database", schemaName)
	}
	return nil
}

// Preapre 批量 Batch
func (e *Engine) BatchWriteMySQLTableData(targetSchemaName, targetTableName, sqlPrefix string, valuesBatchArgs []string, applyThreads int) error {
	if len(valuesBatchArgs) > 0 {
		wp := workpool.New(applyThreads)
		for _, args := range valuesBatchArgs {
			valArgs := args
			wp.Do(func() error {
				insertSql := utils.StringsBuilder(sqlPrefix, valArgs)
				_, err := e.MysqlDB.Exec(insertSql)
				if err != nil {
					return fmt.Errorf("single full table [%s.%s] sql [%s] data bulk insert mysql falied: %v",
						targetSchemaName, targetTableName, insertSql, err)
				}
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			return fmt.Errorf("single full table [%s.%s] data concurrency bulk insert mysql falied: %v", targetSchemaName, targetTableName, err)
		}
	}
	return nil
}

// 获取表字段名以及行数据 -> 用于 FULL/ALL
func (e *Engine) GetOracleTableRowsData(querySQL string, insertBatchSize int) ([]string, []string, error) {
	var (
		err          error
		rowsResult   []string
		rowsTMP      []string
		batchResults []string
	)
	rows, err := e.OracleDB.Query(querySQL)
	if err != nil {
		return []string{}, batchResults, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return cols, batchResults, err
	}

	// 用于判断字段值是数字还是字符
	var (
		columnTypes   []string
		databaseTypes []string
	)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return cols, batchResults, err
	}

	for _, ct := range colTypes {
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())
	}

	// 数据 Scan
	columns := len(cols)
	rawResult := make([][]byte, columns)
	dest := make([]interface{}, columns)
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	// 表行数读取
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return cols, batchResults, err
		}

		for i, raw := range rawResult {
			// 注意 Oracle/Mysql NULL VS 空字符串区别
			// Oracle 空字符串与 NULL 归于一类，统一 NULL 处理 （is null 可以查询 NULL 以及空字符串值，空字符串查询无法查询到空字符串值）
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			// 按照 Oracle 特性来，转换同步统一转换成 NULL 即可，但需要注意业务逻辑中空字符串得写入，需要变更
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if raw == nil {
				rowsResult = append(rowsResult, fmt.Sprintf("%v", `NULL`))
			} else if string(raw) == "" {
				rowsResult = append(rowsResult, fmt.Sprintf("%v", `NULL`))
			} else {
				switch columnTypes[i] {
				case "int64":
					r, err := utils.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return cols, batchResults, err
					}
					rowsResult = append(rowsResult, fmt.Sprintf("%v", r))
				case "uint64":
					r, err := utils.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return cols, batchResults, err
					}
					rowsResult = append(rowsResult, fmt.Sprintf("%v", r))
				case "float32":
					r, err := utils.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return cols, batchResults, err
					}
					rowsResult = append(rowsResult, fmt.Sprintf("%v", r))
				case "float64":
					r, err := utils.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return cols, batchResults, err
					}
					rowsResult = append(rowsResult, fmt.Sprintf("%v", r))
				case "rune":
					r, err := utils.StrconvRune(string(raw))
					if err != nil {
						return cols, batchResults, err
					}
					rowsResult = append(rowsResult, fmt.Sprintf("%v", r))
				case "godror.Number":
					r, err := decimal.NewFromString(string(raw))
					if err != nil {
						return cols, rowsResult, err
					}
					if r.IsInteger() {
						si, err := utils.StrconvIntBitSize(string(raw), 64)
						if err != nil {
							return cols, rowsResult, err
						}
						rowsResult = append(rowsResult, fmt.Sprintf("%v", si))
					} else {
						rf, err := utils.StrconvFloatBitSize(string(raw), 64)
						if err != nil {
							return cols, rowsResult, err
						}
						rowsResult = append(rowsResult, fmt.Sprintf("%v", rf))
					}
				default:
					// 特殊字符单引号替换
					rowsResult = append(rowsResult, fmt.Sprintf("'%v'", strings.Replace(string(raw), "'", "\\"+"'", -1)))
				}
			}
		}

		rowsTMP = append(rowsTMP, utils.StringsBuilder("(", exstrings.Join(rowsResult, ","), ")"))

		// 数组清空
		rowsResult = rowsResult[0:0]

		// batch 批次
		if len(rowsTMP) == insertBatchSize {
			batchResults = append(batchResults, exstrings.Join(rowsTMP, ","))
			// 数组清空
			rowsTMP = rowsTMP[0:0]
		}
	}

	if err = rows.Err(); err != nil {
		return cols, batchResults, err
	}

	// 非 batch 批次
	if len(rowsTMP) > 0 {
		batchResults = append(batchResults, exstrings.Join(rowsTMP, ","))
	}

	return cols, batchResults, nil
}

// 获取数据行 -> 用于 DIFF
func (e *Engine) GetOracleDataRowStrings(querySQL string) ([]string, *strset.Set, uint32, error) {
	var (
		cols     []string
		rowsTMP  []string
		rows     *sql.Rows
		err      error
		crc32SUM uint32
	)

	var crc32Value uint32 = 0

	stringSet := set.NewStringSet()

	rows, err = e.OracleDB.Query(querySQL)
	if err != nil {
		return cols, stringSet, crc32Value, fmt.Errorf("general sql [%v] query failed: [%v]", querySQL, err.Error())
	}

	defer rows.Close()

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
				case "int64":
					r, err := utils.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "uint64":
					r, err := utils.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "float32":
					r, err := utils.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "float64":
					r, err := utils.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "rune":
					r, err := utils.StrconvRune(string(raw))
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				default:
					rowsTMP = append(rowsTMP, fmt.Sprintf("'%v'", string(raw)))
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

func (e *Engine) GetMySQLDataRowStrings(querySQL string) ([]string, *strset.Set, uint32, error) {
	var (
		cols     []string
		rowsTMP  []string
		rows     *sql.Rows
		err      error
		crc32SUM uint32
	)
	var crc32Value uint32 = 0

	stringSet := set.NewStringSet()

	rows, err = e.MysqlDB.Query(querySQL)
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
				case "int64":
					r, err := utils.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "uint64":
					r, err := utils.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "float32":
					r, err := utils.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "float64":
					r, err := utils.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				case "rune":
					r, err := utils.StrconvRune(string(raw))
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					rowsTMP = append(rowsTMP, fmt.Sprintf("%v", r))
				default:
					rowsTMP = append(rowsTMP, fmt.Sprintf("'%v'", string(raw)))
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
