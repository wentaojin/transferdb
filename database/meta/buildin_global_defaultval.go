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
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 自定义字段默认值转换规则 - global 级别
type BuildinGlobalDefaultval struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS       string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT       string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'目标数据库类型'" json:"db_type_t"`
	DefaultValueS string `gorm:"type:varchar(200);not null;index:idx_dbtype_st_map,unique;comment:'源端默认值'" json:"default_value_s"`
	DefaultValueT string `gorm:"type:varchar(200);not null;comment:'目标默认值'" json:"default_value_t"`
	*BaseModel
}

func NewBuildinGlobalDefaultvalModel(m *Meta) *BuildinGlobalDefaultval {
	return &BuildinGlobalDefaultval{BaseModel: &BaseModel{
		Meta: m,
	}}
}

func (rw *BuildinGlobalDefaultval) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [BuildinGlobalDefaultval] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *BuildinGlobalDefaultval) CreateGlobalDefaultVal(ctx context.Context, createS *BuildinGlobalDefaultval) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *BuildinGlobalDefaultval) DetailGlobalDefaultVal(ctx context.Context, detailS *BuildinGlobalDefaultval) ([]BuildinGlobalDefaultval, error) {
	var defaultRuleMap []BuildinGlobalDefaultval

	table, err := rw.ParseSchemaTable()
	if err != nil {
		return defaultRuleMap, err
	}
	if err := rw.DB(ctx).Where("UPPER(db_type_s) = ? AND UPPER(db_type_t) = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT)).Find(&defaultRuleMap).Error; err != nil {
		return defaultRuleMap, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}

	return defaultRuleMap, nil
}

func (rw *BuildinGlobalDefaultval) RowsAffected(ctx context.Context, defaultVal *BuildinGlobalDefaultval) int64 {
	u := &BuildinGlobalDefaultval{}
	return rw.DB(ctx).Where(defaultVal).Find(&u).RowsAffected
}

func (rw *BuildinGlobalDefaultval) InitO2MTBuildinGlobalDefaultValue(ctx context.Context) error {
	var buildinColumDefaultvals []*BuildinGlobalDefaultval

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeOracle,
		DBTypeT:       common.DatabaseTypeMySQL,
		DefaultValueS: common.BuildInOracleColumnDefaultValueSysdate,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueSysdate],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeOracle,
		DBTypeT:       common.DatabaseTypeMySQL,
		DefaultValueS: common.BuildInOracleColumnDefaultValueSYSGUID,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueSYSGUID],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeOracle,
		DBTypeT:       common.DatabaseTypeMySQL,
		DefaultValueS: common.BuildInOracleColumnDefaultValueNULL,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueNULL],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeOracle,
		DBTypeT:       common.DatabaseTypeTiDB,
		DefaultValueS: common.BuildInOracleColumnDefaultValueSysdate,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueSysdate],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeOracle,
		DBTypeT:       common.DatabaseTypeTiDB,
		DefaultValueS: common.BuildInOracleColumnDefaultValueSYSGUID,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueSYSGUID],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeOracle,
		DBTypeT:       common.DatabaseTypeTiDB,
		DefaultValueS: common.BuildInOracleColumnDefaultValueNULL,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInOracleColumnDefaultValueNULL],
	})

	return rw.DB(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "source_default_value"},
			{Name: "reverse_mode"},
		},
		DoNothing: true,
	}).Create(buildinColumDefaultvals).Error
}

func (rw *BuildinGlobalDefaultval) InitMT2OBuildinGlobalDefaultValue(ctx context.Context) error {
	var buildinColumDefaultvals []*BuildinGlobalDefaultval

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeMySQL,
		DBTypeT:       common.DatabaseTypeOracle,
		DefaultValueS: common.BuildInMySQLColumnDefaultValueCurrentTimestamp,
		DefaultValueT: common.BuildInMySQLM2OColumnDefaultValueMap[common.BuildInMySQLColumnDefaultValueCurrentTimestamp],
	})
	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeTiDB,
		DBTypeT:       common.DatabaseTypeOracle,
		DefaultValueS: common.BuildInMySQLColumnDefaultValueCurrentTimestamp,
		DefaultValueT: common.BuildInMySQLM2OColumnDefaultValueMap[common.BuildInMySQLColumnDefaultValueCurrentTimestamp],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeMySQL,
		DBTypeT:       common.DatabaseTypeOracle,
		DefaultValueS: common.BuildInMySQLColumnDefaultValueNULL,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInMySQLColumnDefaultValueNULL],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinGlobalDefaultval{
		DBTypeS:       common.DatabaseTypeTiDB,
		DBTypeT:       common.DatabaseTypeOracle,
		DefaultValueS: common.BuildInMySQLColumnDefaultValueNULL,
		DefaultValueT: common.BuildInOracleO2MColumnDefaultValueMap[common.BuildInMySQLColumnDefaultValueNULL],
	})

	return rw.DB(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(buildinColumDefaultvals).Error
}
