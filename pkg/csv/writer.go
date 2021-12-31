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
package csv

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/service"

	"github.com/thinkeridea/go-extend/exstrings"

	"github.com/wentaojin/transferdb/utils"
)

type FileWriter struct {
	SourceSchema  string
	SourceCharset string
	SourceTable   string
	Columns       []string
	RowidSQL      string
	Rows          *sql.Rows
	OutDir        string
	FileName      string
	Engine        *service.Engine
	service.CSVConfig
}

func (f *FileWriter) WriteFile() error {
	if err := f.adjustCSVConfig(); err != nil {
		return err
	}

	// 文件目录判断
	if err := utils.PathExist(f.OutDir); err != nil {
		return err
	}

	fileW, err := os.OpenFile(f.FileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileW.Close()

	if err = f.write(fileW); err != nil {
		return err
	}
	return nil
}

func (f *FileWriter) adjustCSVConfig() error {
	if f.Separator == "" {
		f.Separator = ","
	}
	if f.Terminator == "" {
		f.Terminator = "\r\n"
	}
	if f.Charset == "" {
		f.Charset = f.SourceCharset
	}
	isSupport := false
	if f.Charset != "" {
		switch strings.ToUpper(f.Charset) {
		case "UTF8":
			isSupport = true
		case "GBK":
			isSupport = true
		default:
			isSupport = false
		}
	}
	if !isSupport {
		return fmt.Errorf("target db character is not support: [%s]", f.Charset)
	}
	return nil
}

func (f *FileWriter) write(w io.Writer) error {
	writer := bufio.NewWriter(w)
	if f.Header {
		if _, err := writer.WriteString(utils.StringsBuilder(exstrings.Join(f.Columns, f.Separator), f.Terminator)); err != nil {
			return fmt.Errorf("failed to write headers: %w", err)
		}
	}

	// Close Rows
	defer f.Rows.Close()

	// 如果不存在数据记录，直接返回
	if !f.Rows.Next() {
		service.Logger.Warn("oracle schema table rowid data return null rows, skip",
			zap.String("schema", f.SourceSchema),
			zap.String("table", f.SourceTable),
			zap.String("sql", f.RowidSQL))
		return nil
	}

	// 数据 SCAN
	columns := len(f.Columns)
	rawResult := make([][]byte, columns)
	dest := make([]interface{}, columns)
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	// 表行数读取
	for f.Rows.Next() {
		var results []string

		err := f.Rows.Scan(dest...)
		if err != nil {
			return err
		}

		for _, raw := range rawResult {
			// 注意 Oracle/Mysql NULL VS 空字符串区别
			// Oracle 空字符串与 NULL 归于一类，统一 NULL 处理 （is null 可以查询 NULL 以及空字符串值，空字符串查询无法查询到空字符串值）
			// Mysql 空字符串与 NULL 非一类，NULL 是 NULL，空字符串是空字符串（is null 只查询 NULL 值，空字符串查询只查询到空字符串值）
			// 按照 Oracle 特性来，转换同步统一转换成 NULL 即可，但需要注意业务逻辑中空字符串得写入，需要变更
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			if raw == nil {
				results = append(results, "NULL")
			} else if string(raw) == "" {
				results = append(results, "NULL")
			} else {
				ok := utils.IsNum(string(raw))
				if ok {
					results = append(results, string(raw))
				} else {
					var (
						by []byte
						bs string
					)
					// 处理字符集、特殊字符转义、字符串引用定界符
					if strings.ToUpper(f.Charset) == utils.OracleGBKCharacterSet {
						gbkBytes, err := utils.Utf8ToGbk(raw)
						if err != nil {
							return err
						}
						by = gbkBytes
					} else {
						by = raw
					}

					if f.EscapeBackslash {
						bs = utils.SpecialLetters(by)
					} else {
						bs = string(by)
					}

					if f.Delimiter == "" {
						results = append(results, bs)
					} else {
						results = append(results, utils.StringsBuilder(f.Delimiter, bs, f.Delimiter))
					}
				}
			}
		}

		// 写入文件
		if _, err = writer.WriteString(utils.StringsBuilder(exstrings.Join(results, f.Separator), f.Terminator)); err != nil {
			return fmt.Errorf("failed to write data row to csv %w", err)
		}
	}

	if err := f.Rows.Err(); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data row to csv %w", err)
	}

	return nil
}
