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
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
)

func main() {
	oraCfg := config.OracleConfig{
		Username:      "findpt",
		Password:      "findpt",
		Host:          "10.2.103.33",
		Port:          1521,
		ServiceName:   "gbk",
		ConnectParams: "poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5m&poolIncrement=1&timezone=Local",
		SchemaName:    "findpt",
		Charset:       "ZHS16GBK",
		IncludeTable:  []string{},
	}
	myCfg := config.MySQLConfig{
		Username:      "root",
		Password:      "marvin",
		Host:          "10.2.103.30",
		Port:          3306,
		ConnectParams: "charset=utf8mb4&multiStatements=true&parseTime=True&loc=Local",
		SchemaName:    "marvin",
	}

	ctx := context.Background()
	oraConn, err := oracle.NewOracleDBEngine(ctx, oraCfg)
	if err != nil {
		panic(err)
	}

	myConn, err := mysql.NewMySQLDBEngine(ctx, myCfg)
	if err != nil {
		panic(err)
	}

	err = getOracleTableRowsDataCSV(ctx, oraConn, myConn, `SELECT * FROM gbk00`)
	if err != nil {
		panic(err)
	}
}

func getOracleTableRowsDataCSV(ctx context.Context, oraConn *oracle.Oracle, myConn *mysql.MySQL, querySQL string) error {
	var (
		err         error
		columnNames []string
		columnTypes []string
	)
	// 临时数据存放
	var rowsTMP []map[string]string
	rowsMap := make(map[string]string)

	rows, err := oraConn.OracleDB.QueryContext(ctx, querySQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to csv get rows columnTypes: %v", err)
	}

	for _, ct := range colTypes {
		columnNames = append(columnNames, ct.Name())
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
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if raw == nil {
				rowsMap[columnNames[i]] = fmt.Sprintf("%v", `NULL`)
			} else if string(raw) == "" {
				rowsMap[columnNames[i]] = fmt.Sprintf("%v", `NULL`)
			} else {
				switch columnTypes[i] {
				case "int64":
					r, err := common.StrconvIntBitSize(string(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[columnNames[i]] = fmt.Sprintf("%v", r)
				case "uint64":
					r, err := common.StrconvUintBitSize(string(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[columnNames[i]] = fmt.Sprintf("%v", r)
				case "float32":
					r, err := common.StrconvFloatBitSize(string(raw), 32)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[columnNames[i]] = fmt.Sprintf("%v", r)
				case "float64":
					r, err := common.StrconvFloatBitSize(string(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[columnNames[i]] = fmt.Sprintf("%v", r)
				case "rune":
					r, err := common.StrconvRune(string(raw))
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[columnNames[i]] = fmt.Sprintf("%v", r)
				case "godror.Number":
					r, err := decimal.NewFromString(string(raw))
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowsMap[columnNames[i]] = fmt.Sprintf("%v", r)
				default:
					// 处理字符集、特殊字符转义、字符串引用定界符
					rowsMap[columnNames[i]] = fmt.Sprintf("'%v'", string(raw))
				}
			}
		}

		rowsTMP = append(rowsTMP, rowsMap)

	}

	if err = rows.Err(); err != nil {
		return err
	}

	for _, s := range rowsTMP {
		_, err = myConn.MySQLDB.Exec(fmt.Sprintf(`INSERT INTO marvin.gbk02 VALUES(%v,%v,%v)`, s["ID"], s["N1"], s["N2"]))
		if err != nil {
			return err
		}
	}

	return nil
}

func isGBK(data []byte) bool {
	length := len(data)
	var i int = 0
	for i < length {
		if data[i] <= 0x7f {
			//编码0~127,只有一个字节的编码，兼容ASCII码
			i++
			continue
		} else {
			//大于127的使用双字节编码，落在gbk编码范围内的字符
			if data[i] >= 0x81 &&
				data[i] <= 0xfe &&
				data[i+1] >= 0x40 &&
				data[i+1] <= 0xfe &&
				data[i+1] != 0xf7 {
				i += 2
				continue
			} else {
				return false
			}
		}
	}
	return true
}

func preNUm(data byte) int {
	var mask byte = 0x80
	var num int = 0
	//8bit中首个0bit前有多少个1bits
	for i := 0; i < 8; i++ {
		if (data & mask) == mask {
			num++
			mask = mask >> 1
		} else {
			break
		}
	}
	return num
}
func isUtf8(data []byte) bool {
	i := 0
	for i < len(data) {
		if (data[i] & 0x80) == 0x00 {
			// 0XXX_XXXX
			i++
			continue
		} else if num := preNUm(data[i]); num > 2 {
			// 110X_XXXX 10XX_XXXX
			// 1110_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_0XXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_10XX 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_110X 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// preNUm() 返回首个字节的8个bits中首个0bit前面1bit的个数，该数量也是该字符所使用的字节数
			i++
			for j := 0; j < num-1; j++ {
				//判断后面的 num - 1 个字节是不是都是10开头
				if (data[i] & 0xc0) != 0x80 {
					return false
				}
				i++
			}
		} else {
			//其他情况说明不是utf-8
			return false
		}
	}
	return true
}
