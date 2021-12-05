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
	"strings"

	"github.com/thinkeridea/go-extend/exstrings"

	"github.com/wentaojin/transferdb/utils"

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
			// 数据库类型 MySQL NULL 是 NULL，空字符串是空字符串
			// 数据库类型 Oracle NULL、空字符串归于一类 NULL
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if v == nil { // 处理 NULL 情况，当数据库类型 MySQL 等于 nil
				row[key] = IsNull
			} else {
				// 处理空字符串以及其他值情况
				row[key] = string(v)
			}
		}
		res = append(res, row)
	}
	return cols, res, nil
}

// 初始化同步表结构
func (e *Engine) InitMysqlEngineDB() error {
	if err := e.GormDB.AutoMigrate(
		&ColumnRuleMap{},
		&TableRuleMap{},
		&SchemaRuleMap{},
		&WaitSyncMeta{},
		&FullSyncMeta{},
		&IncrementSyncMeta{},
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

func (e *Engine) IsExistOracleTable(schemaName string, includeTables []string) error {
	tables, err := e.GetOracleTable(schemaName)
	if err != nil {
		return err
	}
	ok, noExistTables := utils.IsSubsetString(tables, includeTables)
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
				ok := utils.IsNum(string(raw))
				switch {
				case ok && columnTypes[i] != "string":
					result[i] = string(raw)
				default:
					// 数据特殊字符处理
					if strings.Contains(string(raw), "'") && !strings.Contains(string(raw), "\"") {
						result[i] = utils.StringsBuilder("\"", string(raw), "\"")
					} else if strings.Contains(string(raw), "'") && strings.Contains(string(raw), "\"") {
						result[i] = utils.StringsBuilder("'", strings.Replace(string(raw), "'", "\\'", -1), "'")
					} else {
						result[i] = utils.StringsBuilder("'", string(raw), "'")
					}
				}

			}
		}
		//数据按行返回，格式如下：(1,2) (2,3) ,用于数据拼接 batch
		rowsResult = append(rowsResult, utils.StringsBuilder("(", exstrings.Join(result, ","), ")"))
	}

	if err = rows.Err(); err != nil {
		return cols, rowsResult, err
	}

	return cols, rowsResult, nil
}
