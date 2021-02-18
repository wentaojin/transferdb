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

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"gorm.io/gorm"
)

var (
	// 事务 batch 数
	InsertBatchSize = 500
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
	zlog.Logger.Info("exec sql",
		zap.String("sql", fmt.Sprintf("%v", querySQL)))
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
