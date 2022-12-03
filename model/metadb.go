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
package model

import (
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/errors"
	"gorm.io/gorm"
	"time"
)

type MetaDB struct {
	Gorm *gorm.DB
}

func WrapGormDB(gormDB *gorm.DB) *MetaDB {
	return &MetaDB{Gorm: gormDB}
}

type ctxTransactionKeyStruct struct{}

var ctxTransactionKey = ctxTransactionKeyStruct{}

func (g *MetaDB) DB(ctx context.Context) *gorm.DB {
	iface := ctx.Value(ctxTransactionKey)
	if iface != nil {
		tx, ok := iface.(*gorm.DB)
		if !ok {
			return nil
		}
		return tx
	}
	return g.Gorm.WithContext(ctx)
}

func (g *MetaDB) MigrateTables() (err error) {
	return g.migrateStream(
		new(ColumnRuleMap),
		new(TableRuleMap),
		new(SchemaRuleMap),
		new(DefaultValueMap),
		new(DataCompareMeta),
		new(WaitSyncMeta),
		new(FullSyncMeta),
		new(IncrSyncMeta),
		new(TableErrorDetail),
	)
}

func (g *MetaDB) migrateStream(models ...interface{}) (err error) {
	for _, model := range models {
		err = g.Gorm.AutoMigrate(model)
		if err != nil {
			return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on migrate stream: %v", err))
		}
	}
	return nil
}

func (g *MetaDB) InitDefaultValue(ctx context.Context) error {
	defaultValEngine := NewReverseModel(g.Gorm).DefaultValueMap

	if defaultValEngine.RowsAffected(ctx, &DefaultValueMap{
		SourceDefaultValue: common.OracleSysdateDefaultValueMap,
		TargetDefaultValue: common.OracleDefaultValueMap[common.OracleSysdateDefaultValueMap],
		ReverseMode:        common.ReverseO2MMode,
	}) == 0 {
		if err := defaultValEngine.Create(
			ctx,
			&DefaultValueMap{
				SourceDefaultValue: common.OracleSysdateDefaultValueMap,
				TargetDefaultValue: common.OracleDefaultValueMap[common.OracleSysdateDefaultValueMap],
				ReverseMode:        common.ReverseO2MMode,
			}); err != nil {
			return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on init oracle default sysdate value: %v", err))
		}
	}

	if defaultValEngine.RowsAffected(ctx, &DefaultValueMap{
		SourceDefaultValue: common.OracleSYSGUIDDefaultValueMap,
		TargetDefaultValue: common.OracleDefaultValueMap[common.OracleSYSGUIDDefaultValueMap],
		ReverseMode:        common.ReverseO2MMode,
	}) == 0 {
		if err := defaultValEngine.Create(
			ctx,
			&DefaultValueMap{
				SourceDefaultValue: common.OracleSYSGUIDDefaultValueMap,
				TargetDefaultValue: common.OracleDefaultValueMap[common.OracleSYSGUIDDefaultValueMap],
				ReverseMode:        common.ReverseO2MMode,
			}); err != nil {
			return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on init oracle default sysdate value: %v", err))
		}
	}

	if defaultValEngine.RowsAffected(ctx, &DefaultValueMap{
		SourceDefaultValue: common.MySQLCurrentTimestampDefaultValueMAP,
		TargetDefaultValue: common.MySQLDefaultValueMap[common.MySQLCurrentTimestampDefaultValueMAP],
		ReverseMode:        common.ReverseM2OMode,
	}) == 0 {
		if err := defaultValEngine.Create(
			ctx,
			&DefaultValueMap{
				SourceDefaultValue: common.MySQLCurrentTimestampDefaultValueMAP,
				TargetDefaultValue: common.MySQLDefaultValueMap[common.MySQLCurrentTimestampDefaultValueMAP],
				ReverseMode:        common.ReverseM2OMode,
			}); err != nil {
			return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on init mysql default current_timestamp value: %v", err))
		}
	}
	return nil
}

// BaseModel returns model struct base fields
type BaseModel struct {
	Comment   string `gorm:"type:varchar(1000);comment:comment content" json:"comment"`
	CreatedAt string `gorm:"type:datetime(3) default current_timestamp(3);comment:'创建时间'" json:"createdAt"`
	UpdatedAt string `gorm:"type:datetime(3) default current_timestamp(3) on update current_timestamp(3);comment:'更新时间'" json:"updatedAt"`
	*MetaDB   `gorm:"-" json:"-"`
}

func (v *BaseModel) BeforeCreate(db *gorm.DB) (err error) {
	db.Statement.SetColumn("CreatedAt", getCurrentTime())
	db.Statement.SetColumn("UpdatedAt", getCurrentTime())
	return nil
}

func (v *BaseModel) BeforeUpdate(db *gorm.DB) (err error) {
	db.Statement.SetColumn("UpdatedAt", getCurrentTime())
	return nil
}

func getCurrentTime() string {
	return time.Date(time.Now().Year(),
		time.Now().Month(),
		time.Now().Day(),
		time.Now().Hour(),
		time.Now().Minute(),
		time.Now().Second(),
		time.Now().Nanosecond(),
		time.Local).String()
}
