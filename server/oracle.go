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
	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"os"
	"runtime"
	"strconv"

	"github.com/godror/godror"

	_ "github.com/godror/godror"
)

// 创建 oracle 数据库引擎
func NewOracleDBEngine(oraCfg service.SourceConfig) (*sql.DB, error) {
	// https://pkg.go.dev/github.com/godror/godror
	// https://github.com/godror/godror/blob/db9cd12d89cdc1c60758aa3f36ece36cf5a61814/doc/connection.md
	// https://godror.github.io/godror/doc/connection.html
	// 启用异构池 heterogeneousPool 即程序连接用户与访问 oracle schema 用户名不一致
	connString := fmt.Sprintf("oracle://@%s/%s?connectionClass=POOL_CONNECTION_CLASS&heterogeneousPool=1&%s",
		utils.StringsBuilder(oraCfg.Host, ":", strconv.Itoa(oraCfg.Port)),
		oraCfg.ServiceName, oraCfg.ConnectParams)

	oraDSN, err := godror.ParseDSN(connString)
	if err != nil {
		return nil, err
	}

	// https://blogs.oracle.com/opal/post/external-and-proxy-connection-syntax-examples-for-node-oracledb
	// Using 12.2 or later client libraries
	// 异构连接池
	oraDSN.Username, oraDSN.Password = utils.StringsBuilder(oraCfg.Username, "[", oraCfg.SchemaName, "]"), godror.NewPassword(oraCfg.Password)
	oraDSN.OnInitStmts = oraCfg.SessionParams

	// libDir won't have any effect on Linux for linking reasons to do with Oracle's libnnz library that are proving to be intractable.
	// You must set LD_LIBRARY_PATH or run ldconfig before your process starts.
	// This is documented in various places for other drivers that use ODPI-C. The parameter works on macOS and Windows.
	switch runtime.GOOS {
	case "linux":
		if err = os.Setenv("LD_LIBRARY_PATH", oraCfg.LibDir); err != nil {
			return nil, fmt.Errorf("set LD_LIBRARY_PATH env failed: %v", err)
		}
	case "windows", "darwin":
		oraDSN.LibDir = oraCfg.LibDir
	}

	// godror logger 日志输出
	// godror.SetLogger(zapr.NewLogger(zap.L()))

	sqlDB := sql.OpenDB(godror.NewConnector(oraDSN))
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxOpenConns(0)
	sqlDB.SetConnMaxLifetime(0)

	err = sqlDB.Ping()
	if err != nil {
		return sqlDB, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return sqlDB, nil
}
