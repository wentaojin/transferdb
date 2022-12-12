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
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
	"github.com/shopspring/decimal"
	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/common"
	"hash/crc32"
	"strconv"
	"strings"
	"sync/atomic"
)

func (o *Oracle) IsNumberColumnTYPE(schemaName, tableName, indexFiledName string) (bool, error) {
	querySQL := fmt.Sprintf(`select t.COLUMN_NAME,t.DATA_TYPE
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	and t.column_name = c.column_name
	and t.owner = c.owner
	and upper(t.owner) = upper('%s')
	and upper(t.table_name) = upper('%s')
	and upper(t.column_name) = upper('%s')`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		strings.ToUpper(indexFiledName))

	_, queryRes, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return false, err
	}
	if len(queryRes) == 0 || len(queryRes) > 1 {
		return false, fmt.Errorf("oracle table [%s.%s] column [%s] isn't exist or query result is multiple, please check again", schemaName, tableName, indexFiledName)
	}
	for _, q := range queryRes {
		if strings.ToUpper(q["COLUMN_NAME"]) == strings.ToUpper(indexFiledName) && strings.ToUpper(q["DATA_TYPE"]) == "NUMBER" {
			return true, nil
		}
	}
	return false, nil
}

func (o *Oracle) GetOracleTableColumnDistinctValue(schemaName, tableName string, columnList []string) ([]string, error) {
	var (
		colList  []string
		orderCol []string
	)
	for _, col := range columnList {
		colList = append(colList, common.StringsBuilder("'", strings.ToUpper(col), "'"))
	}
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM DBA_TAB_COLS WHERE OWNER = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME IN (%s) ORDER BY NUM_DISTINCT DESC",
		strings.ToUpper(schemaName), strings.ToUpper(tableName), strings.Join(colList, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, query)
	if err != nil {
		return orderCol, err
	}

	for _, r := range res {
		orderCol = append(orderCol, r["COLUMN_NAME"])
	}

	return orderCol, nil
}

func (o *Oracle) GetOracleTableRowsByStatistics(schemaName, tableName string) (int, error) {
	querySQL := fmt.Sprintf(`select NVL(NUM_ROWS,0) AS NUM_ROWS
  from dba_tables
 where upper(OWNER) = upper('%s')
   and upper(table_name) = upper('%s')`, schemaName, tableName)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return 0, err
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("get oracle schema table [%v] rows by statistics falied, results: [%v]",
			fmt.Sprintf("%s.%s", schemaName, tableName), res)
	}
	numRows, err := strconv.Atoi(res[0]["NUM_ROWS"])
	if err != nil {
		return 0, fmt.Errorf("get oracle schema table [%v] rows [%s] by statistics strconv.Atoi falied: %v",
			fmt.Sprintf("%s.%s", schemaName, tableName), res[0]["NUM_ROWS"], err)
	}
	return numRows, nil
}

func (o *Oracle) StartOracleCreateChunkByNUMBER(taskName, schemaName, tableName, numberColName string, chunkSize string) error {
	ctx, _ := context.WithCancel(o.Ctx)

	chunkSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_NUMBER_COL (task_name   => '`, taskName, `',
                                               table_owner => '`, schemaName, `',
                                               table_name  => '`, tableName, `',
                                               table_column => '`, numberColName, `',
                                               chunk_size  => `, chunkSize, `);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, chunkSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed: %v, sql: %v", err, chunkSQL)
	}
	return nil
}

func (o *Oracle) GetOracleTableChunksByNUMBER(taskName, numberColName string) ([]map[string]string, error) {
	querySQL := common.StringsBuilder(`SELECT '`, numberColName, ` BETWEEN ' || start_id || ' AND ' || end_id || '' CMD
FROM user_parallel_execute_chunks WHERE  task_name = '`, taskName, `' ORDER BY chunk_id`)

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}

	return res, nil
}

func (o *Oracle) GetOracleTableActualRows(oraQuery string) (int64, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, oraQuery)
	if err != nil {
		return 0, err
	}
	rowsCount, err := strconv.ParseInt(res[0]["COUNT(1)"], 10, 64)
	if err != nil {
		return rowsCount, fmt.Errorf("error on FUNC GetOracleTableActualRows failed: %v", err)
	}
	return rowsCount, nil
}

func (o *Oracle) GetOracleDataRowStrings(querySQL string) ([]string, *strset.Set, uint32, error) {
	var (
		cols     []string
		rowsTMP  []string
		rows     *sql.Rows
		err      error
		crc32SUM uint32
	)

	var crc32Value uint32 = 0

	stringSet := set.NewStringSet()

	rows, err = o.OracleDB.Query(querySQL)
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
					r, err := common.StrconvIntBitSize(string(raw), 64)
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
				case "float64":
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
				case "godror.Number":
					r, err := decimal.NewFromString(string(raw))
					if err != nil {
						return cols, stringSet, crc32Value, err
					}
					if r.IsInteger() {
						si, err := common.StrconvIntBitSize(string(raw), 64)
						if err != nil {
							return cols, stringSet, crc32Value, err
						}
						rowsTMP = append(rowsTMP, fmt.Sprintf("%v", si))
					} else {
						rf, err := common.StrconvFloatBitSize(string(raw), 64)
						if err != nil {
							return cols, stringSet, crc32Value, err
						}
						rowsTMP = append(rowsTMP, fmt.Sprintf("%v", rf))
					}
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
