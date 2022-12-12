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
package o2m

import (
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
)

type Task struct {
	SourceSchemaName      string `json:"source_schema_name"`
	TargetSchemaName      string `json:"target_schema_name"`
	TableName             string `json:"table_name"`
	TargetDBType          string `json:"target_db_type"`
	SourceDBCharacterSet  string `json:"source_db_character_set"`
	SourceDBNLSSort       string `json:"source_dbnls_sort"`
	SourceDBNLSComp       string `json:"source_dbnls_comp"`
	SourceDBCollation     bool   `json:"source_db_collation"`
	SourceTableCollation  string `json:"source_table_collation"`
	SourceSchemaCollation string `json:"source_schema_collation"`

	Oracle *oracle.Oracle
	MySQL  *mysql.MySQL
}

func GenCheckTaskTable(sourceSchemaName, targetSchemaName, sourceDBCharacterSet, nlsSort, nlsComp string,
	sourceTableCollation map[string]string, sourceSchemaCollation string,
	sourceDBCollation bool, targetDBType string, oracle *oracle.Oracle, mysql *mysql.MySQL, exporters []string) []*Task {
	var tasks []*Task
	for _, t := range exporters {
		tasks = append(tasks, &Task{
			SourceSchemaName:      sourceSchemaName,
			TargetSchemaName:      targetSchemaName,
			TableName:             t,
			SourceDBCharacterSet:  sourceDBCharacterSet,
			SourceDBNLSSort:       nlsSort,
			SourceDBNLSComp:       nlsComp,
			SourceDBCollation:     sourceDBCollation,
			SourceTableCollation:  sourceTableCollation[t],
			SourceSchemaCollation: sourceSchemaCollation,
			TargetDBType:          targetDBType,
			Oracle:                oracle,
			MySQL:                 mysql,
		})
	}
	return tasks
}

func (t *Task) GenOracleTable() (*Table, error) {
	info, err := NewOracleTableINFO(t.SourceSchemaName, t.TableName, t.Oracle, t.SourceDBCharacterSet, t.SourceDBNLSComp, t.SourceTableCollation, t.SourceSchemaCollation, t.SourceDBCollation)
	if err != nil {
		return info, err
	}
	return info, nil
}

func (t *Task) GenMySQLTable() (*Table, string, error) {
	info, version, err := NewMySQLTableINFO(t.TargetSchemaName, t.TableName, t.TargetDBType, t.MySQL)
	if err != nil {
		return info, version, err
	}
	return info, version, nil
}
