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
package taskflow

import (
	"encoding/json"
	"fmt"

	"github.com/WentaoJin/transferdb/pkg/config"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"
)

// 全量同步任务
func SyncTableFullRecordToMySQL(cfg *config.CfgFile, engine *db.Engine, oracleSQL, targetTableName string, insertBatchSize int) error {
	// 捕获数据
	columns, rowsResult, err := extractorTableFullRecord(engine, oracleSQL)
	if err != nil {
		return err
	}
	// 转换数据
	sqlSlice := translatorTableFullRecord(
		cfg.TargetConfig.SchemaName,
		targetTableName,
		columns,
		rowsResult,
		insertBatchSize,
		cfg.AppConfig.SafeMode)
	// 应用数据
	for _, sql := range sqlSlice {
		if err := applierTableFullRecord(sql, p.Engine); err != nil {
			return err
		}
	}
	return nil
}

func ClearMySQLTableFullMetaRecord(engine *db.Engine, deleteSQL string) {

}

/*
	增量同步任务
*/
type Payload struct {
	Engine           *db.Engine
	TargetSchemaName string
	TargetTableName  string
	OracleSQL        string
	InsertBatchSize  int
	SafeMode         bool
}

// 任务同步
func (p *Payload) Do() error {
	return nil
}

// 序列化
func (p *Payload) Marshal() string {
	b, err := json.Marshal(&p)
	if err != nil {
		zlog.Logger.Error("MarshalTaskToString",
			zap.String("string", string(b)),
			zap.String("error", fmt.Sprintf("json marshal task failed: %v", err)))
	}
	return string(b)
}
