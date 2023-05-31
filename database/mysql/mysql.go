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
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"strings"
)

type MySQL struct {
	Ctx     context.Context
	MySQLDB *sql.DB
}

func NewMySQLDBEngine(ctx context.Context, mysqlCfg config.MySQLConfig) (*MySQL, error) {
	if !strings.EqualFold(mysqlCfg.Charset, "") {
		mysqlCfg.ConnectParams = fmt.Sprintf("charset=%s&%s", strings.ToLower(mysqlCfg.Charset), mysqlCfg.ConnectParams)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port, mysqlCfg.ConnectParams)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error on open mysql database connection: %v", err)
	}

	mysqlDB.SetMaxIdleConns(common.MySQLMaxIdleConn)
	mysqlDB.SetMaxOpenConns(common.MySQLMaxConn)
	mysqlDB.SetConnMaxLifetime(common.MySQLConnMaxLifeTime)
	mysqlDB.SetConnMaxIdleTime(common.MySQLConnMaxIdleTime)

	if err = mysqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("error on ping mysql database connection: %v", err)
	}

	return &MySQL{
		Ctx:     ctx,
		MySQLDB: mysqlDB,
	}, nil
}

func Query(ctx context.Context, db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.QueryContext(ctx, querySQL)
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
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			// 查询字段值 NULL
			// 如果字段值 = NULLABLE 则表示值是 NULL
			// 如果字段值 = "" 则表示值是空字符串
			// 如果字段值 = 'NULL' 则表示值是 NULL 字符串
			// 如果字段值 = 'null' 则表示值是 null 字符串
			if v == nil {
				row[cols[k]] = "NULLABLE"
			} else {
				// 处理空字符串以及其他值情况
				// 数据统一 string 格式显示
				row[cols[k]] = string(v)
			}
		}
		res = append(res, row)
	}

	if err = rows.Err(); err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Next failed: [%v]", querySQL, err.Error())
	}
	return cols, res, nil
}
