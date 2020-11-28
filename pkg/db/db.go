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
package db

import (
	"database/sql"
	"fmt"
)

type Engine struct {
	DB *sql.DB
}

func (e *Engine) QuerySQL(querySQL string) (cols []string, res []map[string]string, err error) {
	return GeneralQuery(e.DB, querySQL)
}

type OperatorDB interface {
	QuerySQL(querySQL string) (cols []string, res []map[string]string, err error)
}

type ConvertDB interface {
	GetSchemaMeta() (schemaMeta []string)
	GetTableMeta(schemaName string) (tableMeta []map[string]string)
	GetViewMeta(schemaName, viewName string) (viewMeta []map[string]string)
	GetTableColumnMeta(schemaName string, tableName string) (colMeta []map[string]string)
	GetTablePrimaryKey(schemaName string, tableName string) (pkList []map[string]string)
	GetTableUniqueKey(schemaName string, tableName string) (ukList []map[string]string)
	GetTableForeignKey(schemaName string, tableName string) (fkList []map[string]string)
	GetTableIndexMeta(schemaName string, tableName string) (idxMeta []map[string]string)
}

// General query returns table field columns and corresponding field row data
func GeneralQuery(db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.Query(querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("error on general query SQL \n%v \nFailed: %v", querySQL, err.Error())
	}
	defer rows.Close()

	// indefinite field general query,get field names automatically
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("error on general query rows.Columns failed: %v", err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("error on general query rows.Scan failed: %v", err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			row[key] = string(v)
		}
		res = append(res, row)
	}
	return cols, res, nil
}
