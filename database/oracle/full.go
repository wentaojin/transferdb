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
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"strconv"
	"time"
)

func (o *Oracle) GetOracleCurrentSnapshotSCN() (uint64, error) {
	// 获取当前 SCN 号
	_, res, err := Query(o.Ctx, o.OracleDB, "select min(current_scn) CURRENT_SCN from gv$database")
	var globalSCN uint64
	if err != nil {
		return globalSCN, err
	}
	globalSCN, err = common.StrconvUintBitSize(res[0]["CURRENT_SCN"], 64)
	if err != nil {
		return globalSCN, fmt.Errorf("get oracle current snapshot scn %s utils.StrconvUintBitSize failed: %v", res[0]["CURRENT_SCN"], err)
	}
	return globalSCN, nil
}

func (o *Oracle) StartOracleChunkCreateTask(taskName string) error {
	querySQL := common.StringsBuilder(`SELECT COUNT(1) COUNT FROM dba_parallel_execute_chunks WHERE TASK_NAME='`, taskName, `'`)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return err
	}
	if res[0]["COUNT"] != "0" {
		if err = o.CloseOracleChunkTask(taskName); err != nil {
			return err
		}
	}

	ctx, _ := context.WithCancel(o.Ctx)
	createSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_TASK (task_name => '`, taskName, `');
END;`)
	_, err = o.OracleDB.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create task failed: %v, sql: %v", err, createSQL)
	}
	return nil
}

func (o *Oracle) StartOracleCreateChunkByRowID(taskName, schemaName, tableName string, chunkSize string, callTimeout int) error {

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)
	ctx, cancel := context.WithDeadline(o.Ctx, deadline)
	defer cancel()

	chunkSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (task_name   => '`, taskName, `',
                                               table_owner => '`, schemaName, `',
                                               table_name  => '`, tableName, `',
                                               by_row      => TRUE,
                                               chunk_size  => `, chunkSize, `);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, chunkSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed: %v, sql: %v", err, chunkSQL)
	}
	return nil
}

func (o *Oracle) GetOracleTableChunksByRowID(taskName string) ([]map[string]string, error) {
	querySQL := common.StringsBuilder(`SELECT 'ROWID BETWEEN ''' || start_rowid || ''' AND ''' || end_rowid || '''' CMD FROM dba_parallel_execute_chunks WHERE  task_name = '`, taskName, `' ORDER BY chunk_id`)

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) CloseOracleChunkTask(taskName string) error {
	ctx, _ := context.WithCancel(context.Background())

	clearSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.DROP_TASK ('`, taskName, `');
END;`)

	_, err := o.OracleDB.ExecContext(ctx, clearSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE drop task failed: %v, sql: %v", err, clearSQL)
	}

	return nil
}

// 获取表字段以及行数据 -> 用于 CSV
func (o *Oracle) GetOracleTableRowsColumnCSV(querySQL string, sourceDBCharset, targetDBCharset string) ([]string, error) {

	rows, err := o.OracleDB.QueryContext(o.Ctx, querySQL)
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return []string{}, err
	}

	var columns []string
	for _, col := range cols {
		convertUtf8Raw, err := common.CharsetConvert([]byte(col), sourceDBCharset, common.CharsetUTF8MB4)
		if err != nil {
			return columns, fmt.Errorf("column [%s] charset convert failed, %v", col, err)
		}

		convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
		if err != nil {
			return columns, fmt.Errorf("column [%s] charset convert failed, %v", col, err)
		}
		columns = append(columns, string(convertTargetRaw))
	}

	return columns, nil
}

func (o *Oracle) GetOracleTableRowsDataCSV(querySQL, sourceDBCharset, targetDBCharset string, cfg *config.Config, dataChan chan [][]string, tableColumnNames []string) error {

	var (
		err         error
		columnNames []string
		columnTypes []string
	)
	// 临时数据存放
	rowsTMP := make([][]string, 0, cfg.AppConfig.InsertBatchSize)
	rowData := make([]string, len(tableColumnNames))
	tableColumnNameIndex := make(map[string]int)
	for i, v := range tableColumnNames {
		tableColumnNameIndex[v] = i
	}

	rows, err := o.OracleDB.QueryContext(o.Ctx, querySQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to csv get rows columnTypes: %v", err)
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := common.CharsetConvert([]byte(ct.Name()), sourceDBCharset, common.CharsetUTF8MB4)
		if err != nil {
			return fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}

		convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
		if err != nil {
			return fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, common.BytesToString(convertTargetRaw))
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	// 数据 SCAN
	columnNums := len(columnNames)
	rawResult := make([][]byte, columnNums)
	dest := make([]interface{}, columnNums)
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	// 表行数读取
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		for i, raw := range rawResult {
			// 注意 Oracle/Mysql NULL VS 空字符串区别
			// Oracle 空字符串与 NULL 归于一类，统一 NULL 处理 （is null 可以查询 NULL 以及空字符串值，空字符串查询无法查询到空字符串值）
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			// 按照 Oracle 特性来，转换同步统一转换成 NULL 即可，但需要注意业务逻辑中空字符串得写入，需要变更
			if raw == nil {
				if cfg.CSVConfig.NullValue != "" {
					rowData[tableColumnNameIndex[columnNames[i]]] = cfg.CSVConfig.NullValue
				} else {
					rowData[tableColumnNameIndex[columnNames[i]]] = `NULL`
				}
			} else if common.BytesToString(raw) == "" {
				if cfg.CSVConfig.NullValue != "" {
					rowData[tableColumnNameIndex[columnNames[i]]] = cfg.CSVConfig.NullValue
				} else {
					rowData[tableColumnNameIndex[columnNames[i]]] = `NULL`
				}
			} else {
				switch columnTypes[i] {
				case "int64":
					r, err := common.StrconvIntBitSize(common.BytesToString(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[tableColumnNameIndex[columnNames[i]]] = strconv.FormatInt(r, 10)
				case "uint64":
					r, err := common.StrconvUintBitSize(common.BytesToString(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[tableColumnNameIndex[columnNames[i]]] = strconv.FormatUint(r, 10)
				case "float32":
					r, err := common.StrconvFloatBitSize(common.BytesToString(raw), 32)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[tableColumnNameIndex[columnNames[i]]] = strconv.FormatFloat(r, 'f', -1, 32)
				case "float64":
					r, err := common.StrconvFloatBitSize(common.BytesToString(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[tableColumnNameIndex[columnNames[i]]] = strconv.FormatFloat(r, 'f', -1, 64)
				case "rune":
					r, err := common.StrconvRune(common.BytesToString(raw))
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[tableColumnNameIndex[columnNames[i]]] = string(r)
				case "godror.Number":
					r, err := decimal.NewFromString(common.BytesToString(raw))
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[tableColumnNameIndex[columnNames[i]]] = r.String()
				case "[]uint8":
					// binary data -> raw、long raw、blob
					rowData[tableColumnNameIndex[columnNames[i]]] = common.EscapeBinaryCSV(raw, cfg.CSVConfig.EscapeBackslash, cfg.CSVConfig.Delimiter, cfg.CSVConfig.Separator)
				default:
					var convertTargetRaw []byte

					convertUtf8Raw, err := common.CharsetConvert(raw, sourceDBCharset, common.CharsetUTF8MB4)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					// 处理字符集、特殊字符转义、字符串引用定界符
					if cfg.CSVConfig.EscapeBackslash {
						convertTargetRaw, err = common.CharsetConvert([]byte(common.SpecialLettersUsingMySQL(convertUtf8Raw)), common.CharsetUTF8MB4, targetDBCharset)
						if err != nil {
							return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
						}
					} else {
						convertTargetRaw, err = common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
						if err != nil {
							return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
						}
					}

					if cfg.CSVConfig.Delimiter == "" {
						rowData[tableColumnNameIndex[columnNames[i]]] = common.BytesToString(convertTargetRaw)
					} else {
						rowData[tableColumnNameIndex[columnNames[i]]] = common.StringsBuilder(cfg.CSVConfig.Delimiter, common.BytesToString(convertTargetRaw), cfg.CSVConfig.Delimiter)
					}
				}
			}
		}

		// 临时数组
		rowsTMP = append(rowsTMP, rowData)

		// MAP 清空
		rowData = make([]string, len(tableColumnNames))

		// batch 批次
		if len(rowsTMP) == cfg.AppConfig.InsertBatchSize {

			dataChan <- rowsTMP

			// 数组清空
			rowsTMP = make([][]string, 0, cfg.AppConfig.InsertBatchSize)
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// 非 batch 批次
	if len(rowsTMP) > 0 {
		dataChan <- rowsTMP
	}

	return nil
}

// 获取表字段名以及行数据 -> 用于 FULL/ALL
func (o *Oracle) GetOracleTableRowsColumn(querySQL string, sourceDBCharset, targetDBCharset string) ([]string, error) {
	var (
		err     error
		columns []string
	)

	rows, err := o.OracleDB.QueryContext(o.Ctx, querySQL)
	if err != nil {
		return columns, err
	}
	defer rows.Close()

	tmpCols, err := rows.Columns()
	if err != nil {
		return columns, err
	}

	// 字段名关键字反引号处理
	for _, col := range tmpCols {
		convertUtf8Raw, err := common.CharsetConvert([]byte(col), sourceDBCharset, common.CharsetUTF8MB4)
		if err != nil {
			return columns, fmt.Errorf("column [%s] charset convert failed, %v", col, err)
		}

		convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
		if err != nil {
			return columns, fmt.Errorf("column [%s] charset convert failed, %v", col, err)
		}
		columns = append(columns, common.StringsBuilder("`", string(convertTargetRaw), "`"))
	}
	return columns, nil
}

func (o *Oracle) GetOracleTableRowsData(querySQL string, insertBatchSize, callTimeout int, sourceDBCharset, targetDBCharset string, dataChan chan []map[string]interface{}) error {
	var (
		err  error
		cols []string
	)

	// 临时数据存放
	var rowsTMP []map[string]interface{}
	rowsMap := make(map[string]interface{})

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(o.Ctx, deadline)
	defer cancel()

	rows, err := o.OracleDB.QueryContext(ctx, querySQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	tmpCols, err := rows.Columns()
	if err != nil {
		return err
	}

	// 字段名关键字反引号处理
	for _, col := range tmpCols {
		convertUtf8Raw, err := common.CharsetConvert([]byte(col), sourceDBCharset, common.CharsetUTF8MB4)
		if err != nil {
			return fmt.Errorf("column [%s] charset convert failed, %v", col, err)
		}

		convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
		if err != nil {
			return fmt.Errorf("column [%s] charset convert failed, %v", col, err)
		}
		cols = append(cols, common.StringsBuilder("`", string(convertTargetRaw), "`"))
	}

	// 用于判断字段值是数字还是字符
	var (
		columnNames []string
		columnTypes []string
	)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		columnNames = append(columnNames, ct.Name())
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
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
			return err
		}

		for i, raw := range rawResult {
			if raw == nil {
				//rowsMap[cols[i]] = `NULL` -> sql
				rowsMap[cols[i]] = nil
			} else if string(raw) == "" {
				//rowsMap[cols[i]] = `NULL` -> sql
				rowsMap[cols[i]] = nil
			} else {
				switch columnTypes[i] {
				case "int64":
					r, err := common.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "uint64":
					r, err := common.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "float32":
					r, err := common.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "float64":
					r, err := common.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "rune":
					r, err := common.StrconvRune(string(raw))
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "godror.Number":
					r, err := decimal.NewFromString(string(raw))
					if err != nil {
						return fmt.Errorf("column [%s] NewFromString strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "[]uint8":
					// binary data -> raw、long raw、blob
					rowsMap[cols[i]] = raw
				default:
					// 特殊字符
					convertUtf8Raw, err := common.CharsetConvert(raw, sourceDBCharset, common.CharsetUTF8MB4)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", string(convertTargetRaw))
				}
			}
		}

		// 临时数组
		rowsTMP = append(rowsTMP, rowsMap)
		// MAP 清空
		rowsMap = make(map[string]interface{})

		// batch 批次
		if len(rowsTMP) == insertBatchSize {
			dataChan <- rowsTMP

			// 数组清空
			rowsTMP = make([]map[string]interface{}, 0)
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// 非 batch 批次
	if len(rowsTMP) > 0 {
		dataChan <- rowsTMP
	}

	return nil
}
