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
package main

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/oracle"
)

func main() {
	ctx := context.Background()

	engine, err := oracle.NewOracleDBEngine(ctx, config.OracleConfig{
		Username:    "findpt",
		Password:    "findpt",
		Host:        "10.2.103.33",
		Port:        1521,
		ServiceName: "utf8",
		Charset:     "al32utf8",
	}, "findpt")
	if err != nil {
		panic(err)
	}

	sqlStr := `SELECT * FROM findpt.ora`
	//_, res, err := oracle.Query(ctx, engine.OracleDB, `SELECT * FROM findpt.ora`)
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Println(res)

	err = GetOracleTableRowsData(engine, sqlStr, "UTF8MB4", "UTF8MB4")
	if err != nil {
		panic(err)
	}
}

func GetOracleTableRowsData(o *oracle.Oracle, querySQL string, sourceDBCharset, targetDBCharset string) error {
	var (
		err  error
		cols []string
	)

	var rowsT []map[string]string
	rowsMap := make(map[string]string)

	rows, err := o.OracleDB.QueryContext(o.Ctx, querySQL)
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
		cols = append(cols, common.StringsBuilder("`", col, "`"))
	}

	// 用于判断字段值是数字还是字符
	var (
		columnNames   []string
		columnTypes   []string
		databaseTypes []string
	)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		columnNames = append(columnNames, ct.Name())
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
			return err
		}

		for i, raw := range rawResult {
			// 注意 Oracle/Mysql NULL VS 空字符串区别
			// Oracle 空字符串与 NULL 归于一类，统一 NULL 处理 （is null 可以查询 NULL 以及空字符串值，空字符串查询无法查询到空字符串值）
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			// 按照 Oracle 特性来，转换同步统一转换成 NULL 即可，但需要注意业务逻辑中空字符串得写入，需要变更
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if raw == nil {
				rowsMap[cols[i]] = fmt.Sprintf("%v", `NULL`)
			} else if string(raw) == "" {
				rowsMap[cols[i]] = fmt.Sprintf("%v", `NULL`)
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
				default:
					// 特殊字符
					convertUtf8Raw, err := common.CharsetConvert(raw, sourceDBCharset, common.CharsetUTF8MB4)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					convertTargetRaw, err := common.CharsetConvert([]byte(common.SpecialLettersUsingMySQL(convertUtf8Raw)), common.CharsetUTF8MB4, targetDBCharset)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					rowsMap[cols[i]] = fmt.Sprintf("'%v'", string(convertTargetRaw))
				}
			}
		}
		rowsT = append(rowsT, rowsMap)

		rowsMap = make(map[string]string)

	}

	if err = rows.Err(); err != nil {
		return err
	}

	fmt.Println(rowsT)

	return nil
}
