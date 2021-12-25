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
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xxjwxc/gowp/workpool"

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
	targetSchemaName, targetTableName,
	rowidSQL, sourceDBCharset string, dirIndex int, columns []string, rowsResult [][]string, csvConfig service.CSVConfig) []*FileWriter {
	startTime := time.Now()

	// 行数
	rowCounts := len(rowsResult)

	// 计算可切分数，向下取整
	splitNums := int(math.Floor(float64(rowCounts) / float64(csvConfig.Rows)))

	csvSplitRows := utils.SplitMultipleStringSlice(rowsResult, int64(splitNums))

	var fw []*FileWriter
	for i := 0; i < len(csvSplitRows); i++ {
		fw = append(fw, &FileWriter{
			SourceCharset: sourceDBCharset,
			Header:        csvConfig.Header,
			Separator:     csvConfig.Separator,
			Terminator:    csvConfig.Terminator,
			Charset:       csvConfig.Charset,
			Columns:       columns,
			Rows:          csvSplitRows[i],
			OutDir: filepath.Join(
				csvConfig.OutputDir,
				strings.ToUpper(targetSchemaName),
				strings.ToUpper(targetTableName),
				utils.StringsBuilder("TFB_", strconv.Itoa(dirIndex))),
			FileName: utils.StringsBuilder("CSV_", strconv.Itoa(i)),
		})
	}

	endTime := time.Now()
	service.Logger.Info("single full table rowid data translator",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("rowid sql", rowidSQL),
		zap.Int("rowid rows", rowCounts),
		zap.Int("csv rows", csvConfig.Rows),
		zap.Int("split sql nums", splitNums),
		zap.String("cost", endTime.Sub(startTime).String()))

	return fw
}

func applierTableFullRecord(targetSchemaName, targetTableName, rowidSQL string, applyThreads int, fileWriter []*FileWriter) error {
	startTime := time.Now()
	service.Logger.Info("single full table rowid data applier start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("rowid sql", rowidSQL))

	wp := workpool.New(applyThreads)
	for _, fw := range fileWriter {
		f := fw
		wp.Do(func() error {
			if err := f.WriteFile(); err != nil {
				return err
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return fmt.Errorf("single full table [%s.%s] data concurrency csv file write falied: %v", targetSchemaName, targetTableName, err)
	}

	endTime := time.Now()
	service.Logger.Info("single full table rowid data applier finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("rowid sql", rowidSQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
