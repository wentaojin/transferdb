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
	"database/sql"
	"fmt"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/errors"
	"github.com/wentaojin/transferdb/logger"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type Meta struct {
	GormDB *gorm.DB
}

func NewMetaDBEngine(ctx context.Context, mysqlCfg config.MySQLConfig, slowThreshold int) (*Meta, error) {
	// 创建元数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return &Meta{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on open general database connection [%v]: %v", mysqlCfg.MetaSchema, err))
	}

	createSchema := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, mysqlCfg.MetaSchema)
	_, err = mysqlDB.ExecContext(ctx, createSchema)
	if err != nil {
		return &Meta{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on exec meta database sql [%v]: %v", createSchema, err))
	}
	createSchema = fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, mysqlCfg.SchemaName)
	_, err = mysqlDB.ExecContext(ctx, createSchema)
	if err != nil {
		return &Meta{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on exec target database sql [%v]: %v", createSchema, err))
	}
	err = mysqlDB.Close()
	if err != nil {
		return &Meta{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on close general database sql [%v]: %v", createSchema, err))
	}

	// 初始化 MetaDB
	// 初始化 gorm 日志记录器
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port, mysqlCfg.MetaSchema)
	l := logger.NewGormLogger(zap.L(), slowThreshold)
	l.SetAsDefault()
	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		DriverName: "mysql",
		DSN:        dsn,
	}), &gorm.Config{
		// 禁用外键（指定外键时不会在 mysql 创建真实的外键约束）
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
		Logger:                                   l,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
	})

	if err != nil {
		return nil, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on open meta database connection: %v", err))
	}

	return &Meta{GormDB: gormDB}, nil
}

func WrapGormDB(gormDB *gorm.DB) *Meta {
	return &Meta{GormDB: gormDB}
}

type ctxTxnKeyStruct struct{}

var ctxTxnKey = ctxTxnKeyStruct{}

func (m *Meta) DB(ctx context.Context) *gorm.DB {
	iface := ctx.Value(ctxTxnKey)
	if iface != nil {
		tx, ok := iface.(*gorm.DB)
		if !ok {
			return nil
		}
		return tx
	}
	return m.GormDB.WithContext(ctx)
}

func (m *Meta) MigrateTables() (err error) {
	return m.migrateStream(
		new(ColumnDatatypeRule),
		new(TableDatatypeRule),
		new(SchemaDatatypeRule),
		new(DataCompareMeta),
		new(WaitSyncMeta),
		new(FullSyncMeta),
		new(IncrSyncMeta),
		new(ErrorLogDetail),
		new(BuildinColumnDefaultval),
		new(BuildinObjectCompatible),
		new(BuildinDatatypeRule),
		new(TableNameRule),
	)
}

func (m *Meta) InitDefaultValue(ctx context.Context) error {
	err := NewBuildinColumnDefaultvalModel(m).InitO2MBuildinColumnDefaultValue(ctx)
	if err != nil {
		return err
	}
	err = NewBuildinColumnDefaultvalModel(m).InitM2OBuildinColumnDefaultValue(ctx)
	if err != nil {
		return err
	}
	err = NewBuildinObjectCompatibleModel(m).InitO2MBuildinObjectCompatible(ctx)
	if err != nil {
		return err
	}
	err = NewBuildinDatatypeRuleModel(m).InitO2MBuildinDatatypeRule(ctx)
	if err != nil {
		return err
	}
	err = NewBuildinDatatypeRuleModel(m).InitM2OBuildinDatatypeRule(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (m *Meta) migrateStream(models ...interface{}) (err error) {
	for _, model := range models {
		err = m.GormDB.AutoMigrate(model)
		if err != nil {
			return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on migrate stream: %v", err))
		}
	}
	return nil
}
