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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"
	"go.uber.org/zap"
)

func extractorTableFullRecord(engine *service.Engine, csvConfig service.CSVConfig, sourceSchemaName, sourceTableName, oracleQuery string) ([]string, [][]string, error) {
	startTime := time.Now()
	cols, rowsResult, err := engine.CSVRowProcessor(oracleQuery, csvConfig)
	if err != nil {
		return cols, rowsResult, fmt.Errorf("get oracle schema [%s] table [%s] record by rowid sql falied: %v", sourceSchemaName, sourceTableName, err)
	}

	endTime := time.Now()
	service.Logger.Info("single full table rowid data extractor finished",
		zap.String("schema", sourceSchemaName),
		zap.String("table", sourceTableName),
		zap.String("rowid sql", oracleQuery),
		zap.String("cost", endTime.Sub(startTime).String()))

	return cols, rowsResult, nil
}

func translatorTableFullRecord(
	targetSchemaName, targetTableName, sourceDBCharset string, fileIndex int, columns []string, rowsResult [][]string, csvConfig service.CSVConfig) *FileWriter {
	return &FileWriter{
		SourceCharset: sourceDBCharset,
		Header:        csvConfig.Header,
		Separator:     csvConfig.Separator,
		Terminator:    csvConfig.Terminator,
		Charset:       csvConfig.Charset,
		Columns:       columns,
		Rows:          rowsResult,
		OutDir: filepath.Join(
			csvConfig.OutputDir,
			strings.ToUpper(targetSchemaName),
			strings.ToUpper(targetTableName)),
		FileName: utils.StringsBuilder(
			strings.ToUpper(targetSchemaName),
			".",
			strings.ToUpper(targetTableName),
			".", strconv.Itoa(fileIndex), ".csv"),
	}
}

func applierTableFullRecord(targetSchemaName, targetTableName string, rowCounts int, rowidSQL string, fileWriter *FileWriter) error {
	startTime := time.Now()
	service.Logger.Info("single full table rowid data applier start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.Int("rows", rowCounts),
		zap.String("rowid sql", rowidSQL))
	if err := fileWriter.WriteFile(); err != nil {
		return err
	}
	endTime := time.Now()
	service.Logger.Info("single full table rowid data applier finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.Int("rows", rowCounts),
		zap.String("rowid sql", rowidSQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
