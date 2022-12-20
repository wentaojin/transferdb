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
package meta

import (
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/errors"
	"gorm.io/gorm/clause"
)

// 自定义字段默认值转换规则 - global 级别
type BuildinColumnDefaultval struct {
	ID                 uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceDefaultValue string `gorm:"not null;index:unique_source_default_mode,unique;comment:'源端默认值'" json:"source_default_value"`
	TargetDefaultValue string `gorm:"not null;index:idx_target_default;comment:'目标默认值'" json:"target_default_value"`
	ReverseMode        string `gorm:"not null;index:idx_reverse_mode;index:unique_source_default_mode,unique;comment:'表结构转换模式 ReverseO2M/ReverseM2O'" json:"reverse_mode"`
	*BaseModel
}

func NewBuildinColumnDefaultvalModel(m *Meta) *BuildinColumnDefaultval {
	return &BuildinColumnDefaultval{BaseModel: &BaseModel{
		Meta: m,
	}}
}

func (rw *BuildinColumnDefaultval) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(*BuildinColumnDefaultval)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *BuildinColumnDefaultval) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*BuildinColumnDefaultval)
	var defaultRuleMap []BuildinColumnDefaultval
	if err := rw.DB(ctx).Where("UPPER(reverse_mode) = ?", common.StringUPPER(ds.ReverseMode)).Find(&defaultRuleMap).Error; err != nil {
		return defaultRuleMap, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return defaultRuleMap, nil
}

func (rw *BuildinColumnDefaultval) RowsAffected(ctx context.Context, defaultVal *BuildinColumnDefaultval) int64 {
	u := &BuildinColumnDefaultval{}
	return rw.DB(ctx).Where(defaultVal).Find(&u).RowsAffected
}

func (rw *BuildinColumnDefaultval) InitO2MBuildinColumnDefaultValue(ctx context.Context) error {
	var buildinColumDefaultvals []*BuildinColumnDefaultval

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinColumnDefaultval{
		SourceDefaultValue: common.BuildInOracleColumnDefaultValueSysdate,
		TargetDefaultValue: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueSysdate],
		ReverseMode:        common.ReverseO2MMode,
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinColumnDefaultval{
		SourceDefaultValue: common.BuildInOracleColumnDefaultValueSYSGUID,
		TargetDefaultValue: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueSYSGUID],
		ReverseMode:        common.ReverseO2MMode,
	})

	return rw.DB(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "source_default_value"},
			{Name: "reverse_mode"},
		},
		DoNothing: true,
	}).CreateInBatches(buildinColumDefaultvals, 2).Error
}

func (rw *BuildinColumnDefaultval) InitM2OBuildinColumnDefaultValue(ctx context.Context) error {
	var buildinColumDefaultvals []*BuildinColumnDefaultval

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinColumnDefaultval{
		SourceDefaultValue: common.BuildInMySQLColumnDefaultValueCurrentTimestamp,
		TargetDefaultValue: common.BuildInMySQLM2OColumnDefaultValueMap[common.BuildInMySQLColumnDefaultValueCurrentTimestamp],
		ReverseMode:        common.ReverseM2OMode,
	})
	return rw.DB(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "source_default_value"},
			{Name: "reverse_mode"},
		},
		DoNothing: true,
	}).CreateInBatches(buildinColumDefaultvals, 1).Error
}
