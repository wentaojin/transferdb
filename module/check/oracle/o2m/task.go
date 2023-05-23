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
	"encoding/json"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/check/oracle/public"
)

type Task struct {
	SourceSchemaName      string `json:"source_schema_name"`
	TargetSchemaName      string `json:"target_schema_name"`
	SourceTableName       string `json:"source_table_name"`
	TargetTableName       string `json:"target_table_name"`
	SourceDBCharacterSet  string `json:"source_db_character_set"`
	SourceDBNLSSort       string `json:"source_dbnls_sort"`
	SourceDBNLSComp       string `json:"source_dbnls_comp"`
	SourceDBCollation     bool   `json:"source_db_collation"`
	SourceTableCollation  string `json:"source_table_collation"`
	SourceSchemaCollation string `json:"source_schema_collation"`

	Oracle *oracle.Oracle `json:"-"`
	MySQL  *mysql.MySQL   `json:"-"`
}

func GenCheckTaskTable(sourceSchemaName, targetSchemaName, sourceDBCharacterSet, nlsSort, nlsComp string,
	sourceTableCollation map[string]string, sourceSchemaCollation string,
	sourceDBCollation bool, oracle *oracle.Oracle, mysql *mysql.MySQL, tableNameRule map[string]string, waitSyncMetas []meta.WaitSyncMeta) []*Task {
	var tasks []*Task
	for _, t := range waitSyncMetas {
		// 库名、表名规则
		var targetTableName string
		if val, ok := tableNameRule[common.StringUPPER(t.TableNameS)]; ok {
			targetTableName = val
		} else {
			targetTableName = common.StringUPPER(t.TableNameS)
		}
		tasks = append(tasks, &Task{
			SourceSchemaName:      sourceSchemaName,
			TargetSchemaName:      targetSchemaName,
			SourceTableName:       t.TableNameS,
			TargetTableName:       targetTableName,
			SourceDBCharacterSet:  sourceDBCharacterSet,
			SourceDBNLSSort:       nlsSort,
			SourceDBNLSComp:       nlsComp,
			SourceDBCollation:     sourceDBCollation,
			SourceTableCollation:  sourceTableCollation[t.TableNameS],
			SourceSchemaCollation: sourceSchemaCollation,
			Oracle:                oracle,
			MySQL:                 mysql,
		})
	}
	return tasks
}

func (t *Task) GenOracleTable() (*public.Table, error) {
	info, err := NewOracleTableINFO(t.SourceSchemaName, t.SourceTableName, t.Oracle, t.SourceDBCharacterSet, t.SourceDBNLSComp, t.SourceTableCollation, t.SourceSchemaCollation, t.SourceDBCollation)
	if err != nil {
		return info, err
	}
	return info, nil
}

func (t *Task) GenMySQLTable() (*public.Table, string, error) {
	info, version, err := NewMySQLTableINFO(t.TargetSchemaName, t.TargetTableName, common.DatabaseTypeMySQL, t.MySQL)
	if err != nil {
		return info, version, err
	}
	return info, version, nil
}

func (t *Task) String() string {
	marshal, _ := json.Marshal(t)
	return string(marshal)
}
