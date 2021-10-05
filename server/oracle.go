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
package server

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/wentaojin/transferdb/service"

	"github.com/godror/godror"

	"github.com/godror/godror/dsn"

	_ "github.com/godror/godror"
)

// 创建 oracle 数据库引擎
func NewOracleDBEngine(oraCfg service.SourceConfig) (*sql.DB, error) {
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
