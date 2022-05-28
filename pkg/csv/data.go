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
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/service"
	"go.uber.org/zap"
)

func extractorTableFullRecord(engine *service.Engine, sourceSchemaName, sourceTableName, oracleQuery string) ([]string, *sql.Rows, error) {
	startTime := time.Now()

	rows, err := engine.OracleDB.Query(oracleQuery)
	if err != nil {
		return []string{}, rows, fmt.Errorf("get oracle schema [%s] table [%s] record by rowid sql falied: %v", sourceSchemaName, sourceTableName, err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return cols, rows, fmt.Errorf("[%v] error on general query rows.Columns failed", err.Error())
	}

	endTime := time.Now()
	zap.L().Info("single full table rowid data extractor finished",
		zap.String("schema", sourceSchemaName),
		zap.String("table", sourceTableName),
		zap.String("rowid sql", oracleQuery),
		zap.String("cost", endTime.Sub(startTime).String()))

	return cols, rows, nil
}

func translatorTableFullRecord(
	targetSchemaName, targetTableName, sourceDBCharset string, columns []string,
	engine *service.Engine, sourceSchema, sourceTable, querySQL string,
	rowsResult *sql.Rows, csvConfig service.CSVConfig, csvFileName string) *FileWriter {
	return &FileWriter{
		SourceSchema:  sourceSchema,
		SourceTable:   sourceTable,
		SourceCharset: sourceDBCharset,
		QuerySQL:      querySQL,
		Engine:        engine,
		CSVConfig:     csvConfig,
		Columns:       columns,
		Rows:          rowsResult,
		OutDir: filepath.Join(
			csvConfig.OutputDir,
			strings.ToUpper(targetSchemaName),
			strings.ToUpper(targetTableName)),
		FileName: csvFileName,
	}
}

func applierTableFullRecord(targetSchemaName, targetTableName string, querySQL string, fileWriter *FileWriter) error {
	startTime := time.Now()
	zap.L().Info("single full table rowid data applier start",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("query sql", querySQL))
	if err := fileWriter.WriteFile(); err != nil {
		return err
	}
	endTime := time.Now()
	zap.L().Info("single full table rowid data applier finished",
		zap.String("schema", targetSchemaName),
		zap.String("table", targetTableName),
		zap.String("query sql", querySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
