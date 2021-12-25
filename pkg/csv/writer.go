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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/thinkeridea/go-extend/exstrings"

	"github.com/wentaojin/transferdb/utils"
)

type FileWriter struct {
	SourceCharset string
	Header        bool
	Separator     string
	Terminator    string
	Charset       string
	Columns       []string
	Rows          [][]string
	OutDir        string
	FileName      string
}

func (f *FileWriter) WriteFile() error {
	if err := f.adjustCSVConfig(); err != nil {
		return err
	}

	// 文件目录判断
	if err := utils.PathExist(f.OutDir); err != nil {
		return err
	}

	fileW, err := os.OpenFile(filepath.Join(f.OutDir, f.FileName), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileW.Close()

	if err = f.write(fileW, f.Columns, f.Rows); err != nil {
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

func (f *FileWriter) write(w io.Writer, columns []string, rows [][]string) error {
	writer := bufio.NewWriter(w)
	if f.Header {
		if _, err := writer.WriteString(utils.StringsBuilder(exstrings.Join(columns, f.Separator), f.Terminator)); err != nil {
			return fmt.Errorf("failed to write headers: %w", err)
		}
	}
	// 写入文件
	for _, r := range rows {
		if _, err := writer.WriteString(utils.StringsBuilder(exstrings.Join(r, f.Separator), f.Terminator)); err != nil {
			return fmt.Errorf("failed to write data row to csv %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data row to csv %w", err)
	}
	return nil
}
