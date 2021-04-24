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
	"strings"
	"time"

	"github.com/godror/godror"

	"github.com/wentaojin/transferdb/pkg/config"

	"github.com/godror/godror/dsn"

	"github.com/wentaojin/transferdb/util"

	_ "github.com/godror/godror"
)

// 创建 oracle 数据库引擎
func NewOracleDBEngine(oraCfg config.SourceConfig) (*sql.DB, error) {
	// https://pkg.go.dev/github.com/godror/godror
	// https://github.com/godror/godror/blob/db9cd12d89cdc1c60758aa3f36ece36cf5a61814/doc/connection.md
	// 时区以及配置设置
	loc, err := time.LoadLocation(oraCfg.Timezone)
	if err != nil {
		return nil, err
	}
	oraDsn := godror.ConnectionParams{
		CommonParams: godror.CommonParams{
			Username:      oraCfg.Username,
			ConnectString: oraCfg.ConnectString,
			Password:      godror.NewPassword(oraCfg.Password),
			OnInitStmts:   oraCfg.SessionParams,
			Timezone:      loc,
		},
		PoolParams: godror.PoolParams{
			MinSessions:    dsn.DefaultPoolMinSessions,
			MaxSessions:    dsn.DefaultPoolMaxSessions,
			WaitTimeout:    dsn.DefaultWaitTimeout,
			MaxLifeTime:    dsn.DefaultMaxLifeTime,
			SessionTimeout: dsn.DefaultSessionTimeout,
		},
	}
	sqlDB := sql.OpenDB(godror.NewConnector(oraDsn))

	err = sqlDB.Ping()
	if err != nil {
		return sqlDB, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return sqlDB, nil
}

func (e *Engine) IsExistOracleSchema(schemaName string) error {
	schemas, err := e.getOracleSchema()
	if err != nil {
		return err
	}
	if !util.IsContainString(schemas, strings.ToUpper(schemaName)) {
		return fmt.Errorf("oracle schema [%s] isn't exist in the database", schemaName)
	}
	return nil
}

func (e *Engine) IsExistOracleTable(schemaName string, includeTables []string) error {
	tables, err := e.GetOracleTable(schemaName)
	if err != nil {
		return err
	}
	ok, noExistTables := util.IsSubsetString(tables, includeTables)
	if !ok {
		return fmt.Errorf("oracle include-tables values [%v] isn't exist in the db schema [%v]", noExistTables, schemaName)
	}
	return nil
}

// 查询 Oracle 数据并按行返回对应字段以及行数据 -> 按字段类型返回行数据
// 用于拼接 batch
func (e *Engine) QueryFormatOracleRows(querySQL string) ([]string, []string, error) {
	var (
		cols       []string
		err        error
		rowsResult []string
	)
	rows, err := e.OracleDB.Query(querySQL)
	if err != nil {
		return cols, rowsResult, err
	}
	defer rows.Close()

	cols, err = rows.Columns()
	if err != nil {
		return cols, rowsResult, err
	}

	// 用于判断字段值是数字还是字符
	var columnTypes []string
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return cols, rowsResult, err
	}

	for _, ct := range colTypes {
		// 数据库字段类型 DatabaseTypeName() 映射 go 类型 ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	// Read all rows
	var actualRows [][]string
	for rows.Next() {
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err = rows.Scan(dest...)
		if err != nil {
			return cols, rowsResult, err
		}

		for i, raw := range rawResult {
			// 注意 Oracle/Mysql NULL VS 空字符串区别
			// Oracle 空字符串与 NULL 归于一类，统一 NULL 处理 （is null 可以查询 NULL 以及空字符串值，空字符串查询无法查询到空字符串值）
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			// 按照 Oracle 特性来，转换同步统一转换成 NULL 即可，但需要注意业务逻辑中空字符串得写入，需要变更
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if raw == nil {
				result[i] = "NULL"
			} else if string(raw) == "" {
				result[i] = "NULL"
			} else {
				ok := util.IsNum(string(raw))
				switch {
				case ok && columnTypes[i] != "string":
					result[i] = string(raw)
				default:
					result[i] = util.StringsBuilder("'", string(raw), "'")
				}

			}
		}
		actualRows = append(actualRows, result)
	}

	if err = rows.Err(); err != nil {
		return cols, rowsResult, err
	}

	for _, row := range actualRows {
		//数据按行返回，格式如下：(1,2) (2,3) ,用于数据拼接 batch
		rowsResult = append(rowsResult, util.StringsBuilder("(", strings.Join(row, ","), ")"))
	}

	return cols, rowsResult, nil
}
