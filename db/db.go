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
package db

import (
	"database/sql"
	"fmt"

	"gorm.io/gorm"
)

// 事务 batch 数
var InsertBatchSize = 500

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
		return cols, res, fmt.Errorf("[%v] error on general query SQL [%v] failed", err.Error(), querySQL)
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("[%v] error on general query rows.Columns failed", err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("[%v] error on general query rows.Scan failed", err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			row[key] = string(v)
		}
		res = append(res, row)
	}
	return cols, res, nil
}

// 查询按行返回对应字段以及行数据
func QueryRows(db *sql.DB, querySQL string) ([]string, [][]string, error) {
	var (
		cols       []string
		actualRows [][]string
		err        error
	)
	rows, err := db.Query(querySQL)
	if err == nil {
		defer rows.Close()
	}

	if err != nil {
		return cols, actualRows, err
	}

	cols, err = rows.Columns()
	if err != nil {
		return cols, actualRows, err
	}
	// Read all rows.
	for rows.Next() {
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err = rows.Scan(dest...)
		if err != nil {
			return cols, actualRows, err
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)
				result[i] = val
			}
		}

		actualRows = append(actualRows, result)
	}
	if err = rows.Err(); err != nil {
		return cols, actualRows, err
	}
	return cols, actualRows, nil
}
