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
package m2o

import (
	"encoding/json"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
)

type Task struct {
	SourceDBType          string `json:"source_db_type"`
	SourceSchemaName      string `json:"source_schema_name"`
	TargetSchemaName      string `json:"target_schema_name"`
	SourceTableName       string `json:"source_table_name"`
	TargetTableName       string `json:"target_table_name"`
	TargetDBCharacterSet  string `json:"target_db_character_set"`
	TargetDBNLSSort       string `json:"target_dbnls_sort"`
	TargetDBNLSComp       string `json:"target_dbnls_comp"`
	TargetDBCollation     bool   `json:"target_db_collation"`
	TargetTableCollation  string `json:"target_table_collation"`
	TargetSchemaCollation string `json:"target_schema_collation"`

	Oracle *oracle.Oracle `json:"-"`
	MySQL  *mysql.MySQL   `json:"-"`
}

func GenCheckTaskTable(sourceDBType, sourceSchemaName, targetSchemaName, targetDBCharacterSet, nlsSort, nlsComp string,
	targetTableCollation map[string]string, targetSchemaCollation string,
	targetDBCollation bool, oracle *oracle.Oracle, mysql *mysql.MySQL, tableNameRule map[string]string, waitSyncMetas []meta.WaitSyncMeta) []*Task {
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
			SourceDBType:          sourceDBType,
			SourceSchemaName:      sourceSchemaName,
			TargetSchemaName:      targetSchemaName,
			SourceTableName:       t.TableNameS,
			TargetTableName:       targetTableName,
			TargetDBCharacterSet:  targetDBCharacterSet,
			TargetDBNLSSort:       nlsSort,
			TargetDBNLSComp:       nlsComp,
			TargetDBCollation:     targetDBCollation,
			TargetTableCollation:  targetTableCollation[t.TableNameS],
			TargetSchemaCollation: targetSchemaCollation,
			Oracle:                oracle,
			MySQL:                 mysql,
		})
	}
	return tasks
}

func (t *Task) GenOracleTable() (*Table, error) {
	info, err := NewOracleTableINFO(t.TargetSchemaName, t.TargetTableName, t.Oracle, t.TargetDBCharacterSet, t.TargetDBNLSComp, t.TargetTableCollation, t.TargetSchemaCollation, t.TargetDBCollation)
	if err != nil {
		return info, err
	}
	return info, nil
}

func (t *Task) GenMySQLTable() (*Table, string, error) {
	info, version, err := NewMySQLTableINFO(t.SourceSchemaName, t.SourceSchemaName, t.SourceDBType, t.MySQL)
	if err != nil {
		return info, version, err
	}
	return info, version, nil
}

func (t *Task) String() string {
	marshal, _ := json.Marshal(t)
	return string(marshal)
}
