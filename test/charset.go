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
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/godror/godror"
	"github.com/godror/godror/dsn"
	"github.com/shopspring/decimal"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/oracle"
	"runtime"
	"strconv"
	"strings"
)

var (
	userName    = flag.String("user", "user", "specify oracle db username")
	password    = flag.String("pass", "pass", "specify oracle db username password")
	host        = flag.String("host", "host", "specify oracle db host")
	port        = flag.Int("port", 1521, "specify oracle db host port")
	serviceName = flag.String("service", "service", "specify oracle db servicename")
	charset     = flag.String("charset", "", "specify oracle db charset")
	schemaName  = flag.String("schema", "marvin", "specify query schema name")
	tableName   = flag.String("table", "marvin", "specify query table name")
	chunk       = flag.String("chunk", "", "specify query table chunk")
	querySQL    = flag.String("sql", "", "specify query sql")
)

func main() {
	flag.Parse()

	oraCfg := config.OracleConfig{
		Username:      *userName,
		Password:      *password,
		Host:          *host,
		Port:          *port,
		ServiceName:   *serviceName,
		ConnectParams: "poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5m&poolIncrement=1&timezone=Local",
		SchemaName:    *schemaName,
		IncludeTable:  []string{*tableName},
	}

	ctx := context.Background()
	oraConn, err := newOracleDBEngine(ctx, oraCfg, *charset)
	if err != nil {
		panic(err)
	}
	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oracleDBVersion, err := oraConn.GetOracleDBVersion()
	if err != nil {
		panic(err)
	}
	if common.VersionOrdinal(oracleDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		panic(fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oracleDBVersion))
	}
	oracleCollation := false
	if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oracleCollation = true
	}
	column, err := adjustTableSelectColumn(oraConn, common.StringUPPER(*schemaName), common.StringsBuilder(*tableName), oracleCollation)
	if err != nil {
		panic(err)
	}

	var queryS string

	switch {
	case !strings.EqualFold(*querySQL, ""):
		queryS = *querySQL
	case !strings.EqualFold(*chunk, ""):
		queryS = common.StringsBuilder(`SELECT `, column, ` FROM `, common.StringUPPER(*schemaName), `.`, common.StringUPPER(*tableName), ` WHERE `, *chunk)
	default:
		queryS = common.StringsBuilder(`SELECT `, column, ` FROM `, common.StringUPPER(*schemaName), `.`, common.StringUPPER(*tableName))
	}

	res, err := getOracleTableRowsData(ctx, oraConn, queryS)
	if err != nil {
		panic(err)
	}

	fmt.Println(res)
	fmt.Println("success")
}

func newOracleDBEngine(ctx context.Context, oraCfg config.OracleConfig, charset string) (*oracle.Oracle, error) {
	// https://pkg.go.dev/github.com/godror/godror
	// https://github.com/godror/godror/blob/db9cd12d89cdc1c60758aa3f36ece36cf5a61814/doc/connection.md
	// https://godror.github.io/godror/doc/connection.html
	// You can specify connection timeout seconds with "?connect_timeout=15" - Ping uses this timeout, NOT the Deadline in Context!
	// For more connection options, see [Godor Connection Handling](https://godror.github.io/godror/doc/connection.html).
	var (
		connString string
		oraDSN     dsn.ConnectionParams
		err        error
	)

	// https://github.com/godror/godror/pull/65
	connString = fmt.Sprintf("oracle://@%s/%s?connectionClass=POOL_CONNECTION_CLASS00&%s",
		common.StringsBuilder(oraCfg.Host, ":", strconv.Itoa(oraCfg.Port)),
		oraCfg.ServiceName, oraCfg.ConnectParams)
	oraDSN, err = godror.ParseDSN(connString)
	if err != nil {
		return nil, err
	}

	oraDSN.Username, oraDSN.Password = oraCfg.Username, godror.NewPassword(oraCfg.Password)

	if !strings.EqualFold(oraCfg.PDBName, "") {
		oraCfg.SessionParams = append(oraCfg.SessionParams, fmt.Sprintf(`ALTER SESSION SET CONTAINER = %s`, oraCfg.PDBName))
	}

	if !strings.EqualFold(oraCfg.Username, oraCfg.SchemaName) && !strings.EqualFold(oraCfg.SchemaName, "") {
		oraCfg.SessionParams = append(oraCfg.SessionParams, fmt.Sprintf(`ALTER SESSION SET CURRENT_SCHEMA = %s`, oraCfg.SchemaName))
	}

	// 关闭外部认证
	oraDSN.ExternalAuth = false
	oraDSN.OnInitStmts = oraCfg.SessionParams

	// libDir won't have any effect on Linux for linking reasons to do with Oracle's libnnz library that are proving to be intractable.
	// You must set LD_LIBRARY_PATH or run ldconfig before your process starts.
	// This is documented in various places for other drivers that use ODPI-C. The parameter works on macOS and Windows.
	if !strings.EqualFold(oraCfg.LibDir, "") {
		switch runtime.GOOS {
		case "windows", "darwin":
			oraDSN.LibDir = oraCfg.LibDir
		}
	}

	// charset 字符集
	if !strings.EqualFold(charset, "") {
		oraDSN.Charset = charset
	}

	// godror logger 日志输出
	// godror.SetLogger(zapr.NewLogger(zap.L()))

	sqlDB := sql.OpenDB(godror.NewConnector(oraDSN))
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxOpenConns(0)
	sqlDB.SetConnMaxLifetime(0)

	fmt.Printf("oracle db connect string: %v\n", oraDSN.StringWithPassword())

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return &oracle.Oracle{
		Ctx:      ctx,
		OracleDB: sqlDB,
	}, nil
}

func adjustTableSelectColumn(oraConn *oracle.Oracle, sourceSchema, sourceTable string, oracleCollation bool) (string, error) {
	// Date/Timestamp 字段类型格式化
	// Interval Year/Day 数据字符 TO_CHAR 格式化
	columnsINFO, err := oraConn.GetOracleSchemaTableColumn(sourceSchema, sourceTable, oracleCollation)
	if err != nil {
		return "", err
	}

	var columnNames []string

	for _, rowCol := range columnsINFO {
		switch strings.ToUpper(rowCol["DATA_TYPE"]) {
		// 数字
		case "NUMBER":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
			// 字符
		case "BFILE", "CHARACTER", "LONG", "NCHAR VARYING", "ROWID", "UROWID", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2", "NCLOB", "CLOB":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
			// XMLTYPE
		case "XMLTYPE":
			columnNames = append(columnNames, fmt.Sprintf(" XMLSERIALIZE(CONTENT %s AS CLOB) AS %s", rowCol["COLUMN_NAME"], rowCol["COLUMN_NAME"]))
			// 二进制
		case "BLOB", "LONG RAW", "RAW":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
			// 时间
		case "DATE":
			columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-MM-dd HH24:mi:ss') AS ", rowCol["COLUMN_NAME"]))
			// 默认其他类型
		default:
			if strings.Contains(rowCol["DATA_TYPE"], "INTERVAL") {
				columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ") AS ", rowCol["COLUMN_NAME"]))
			} else if strings.Contains(rowCol["DATA_TYPE"], "TIMESTAMP") {
				dataScale, err := strconv.Atoi(rowCol["DATA_SCALE"])
				if err != nil {
					return "", fmt.Errorf("aujust oracle timestamp datatype scale [%s] strconv.Atoi failed: %v", rowCol["DATA_SCALE"], err)
				}
				if dataScale == 0 {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-mm-dd hh24:mi:ss') AS ", rowCol["COLUMN_NAME"]))
				} else if dataScale < 0 && dataScale <= 6 {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"],
						",'yyyy-mm-dd hh24:mi:ss.ff", rowCol["DATA_SCALE"], "') AS ", rowCol["COLUMN_NAME"]))
				} else {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-mm-dd hh24:mi:ss.ff6') AS ", rowCol["COLUMN_NAME"]))
				}

			} else {
				columnNames = append(columnNames, rowCol["COLUMN_NAME"])
			}
		}

	}

	return strings.Join(columnNames, ","), nil
}

func getOracleTableRowsData(ctx context.Context, oraConn *oracle.Oracle, queryS string) ([]map[string]string, error) {
	var (
		err  error
		cols []string
	)

	// 临时数据存放
	var rowsTMP []map[string]string
	rowsMap := make(map[string]string)

	rows, err := oraConn.OracleDB.QueryContext(ctx, queryS)
	if err != nil {
		return rowsTMP, err
	}

	cols, err = rows.Columns()
	if err != nil {
		return rowsTMP, err
	}

	// 用于判断字段值是数字还是字符
	var (
		columnNames   []string
		columnTypes   []string
		databaseTypes []string
	)
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return rowsTMP, err
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
			return rowsTMP, fmt.Errorf("data row %v meet error: %v", string(bytes.Join(rawResult, []byte(" "))), err)
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
						return rowsTMP, fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "uint64":
					r, err := common.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return rowsTMP, fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "float32":
					r, err := common.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return rowsTMP, fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "float64":
					r, err := common.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return rowsTMP, fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "rune":
					r, err := common.StrconvRune(string(raw))
					if err != nil {
						return rowsTMP, fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				case "godror.Number":
					r, err := decimal.NewFromString(string(raw))
					if err != nil {
						return rowsTMP, fmt.Errorf("column [%s] NewFromString strconv failed, %v", columnNames[i], err)
					}
					rowsMap[cols[i]] = fmt.Sprintf("%v", r)
				default:
					// 特殊字符
					rowsMap[cols[i]] = fmt.Sprintf("'%v'", common.SpecialLettersUsingMySQL(raw))
				}
			}
		}

		rowsTMP = append(rowsTMP, rowsMap)
	}

	if err = rows.Err(); err != nil {
		return rowsTMP, err
	}

	return rowsTMP, nil
}
