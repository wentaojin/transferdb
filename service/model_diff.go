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
package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"go.uber.org/zap"
)

// 数据校验元数据表
type DataDiffMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_range;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_range;comment:'源端表名'" json:"source_table_name"`
	SourceColumnInfo string `gorm:"comment:'源端查询字段信息'" json:"source_column_info"`
	TargetColumnInfo string `gorm:"comment:'目标端查询字段信息'" json:"target_column_info"`
	Range            string `gorm:"not null;index:idx_schema_table_range;comment:'查询 where 条件'" json:"range"`
	NumberColumn     string `gorm:"comment:'number 类型字段列'" json:"number_column"` // 值为空则是指定 WHERE 对比或者 WHERE 1=1 对比
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"`        // 同步转换统一转换成非分区表，此处只做标志
	BaseModel
}

func (df *DataDiffMeta) GetDataDiffMetaTableName(schemaName string, engine *Engine) ([]string, error) {
	var tables []string
	if err := engine.GormDB.Model(&DataDiffMeta{}).
		Where("source_schema_name = ?",
			strings.ToUpper(schemaName)).Distinct().Pluck("source_table_name", &tables).Error; err != nil {
		return tables, fmt.Errorf("meta schema table [data_diff_meta] query source_table_name record failed: %v", err)
	}
	return tables, nil
}

func (df *DataDiffMeta) String() string {
	dfBytes, _ := json.Marshal(df)
	return string(dfBytes)
}

func (e *Engine) GetDataDiffMeta(schemaName, tableName string) ([]DataDiffMeta, error) {
	var c []DataDiffMeta
	if err := e.GormDB.Where("source_schema_name = ? AND source_table_name = ?",
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName)).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom data diff meta failed: %v", err)
	}
	return c, nil
}

func (e *Engine) DeleteDataDiffMeta(schemaName, tableName, whereS string) error {
	errTotal, err := e.GetTableErrorDetailCountByTableMode(schemaName, tableName, utils.DiffMode)
	if err != nil {
		return err
	}
	// 若存在错误，skip 清理
	if errTotal >= 1 {
		return nil
	}
	if strings.Replace(whereS, " ", "", -1) == "1=1" {
		if err := e.GormDB.Model(DataDiffMeta{}).
			Where("1 = 1 AND source_schema_name = ? AND source_table_name = ?",
				strings.ToUpper(schemaName),
				strings.ToUpper(tableName)).
			Delete(&DataDiffMeta{}).Error; err != nil {
			return fmt.Errorf("get custom data diff meta failed: %v", err)
		}
	} else {
		if err := e.GormDB.Model(DataDiffMeta{}).
			Where("source_schema_name = ? AND source_table_name = ? AND `range` = ?",
				strings.ToUpper(schemaName),
				strings.ToUpper(tableName), whereS).
			Delete(&DataDiffMeta{}).Error; err != nil {
			return fmt.Errorf("get custom data diff meta failed: %v", err)
		}
	}

	Logger.Info("delete mysql [data_diff_meta] meta",
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("where", whereS),
		zap.String("status", "success"))
	return nil
}
