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
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/errors"
	"github.com/wentaojin/transferdb/logger"
	"github.com/wentaojin/transferdb/model"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func NewMetaDBEngine(ctx context.Context, mysqlCfg config.MySQLConfig, slowThreshold int) (*model.MetaDB, error) {
	// 创建元数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return &model.MetaDB{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on open general database connection [%v]: %v", mysqlCfg.MetaSchema, err))
	}

	createSchema := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, mysqlCfg.MetaSchema)
	_, err = mysqlDB.ExecContext(ctx, createSchema)
	if err != nil {
		return &model.MetaDB{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on exec general database sql [%v]: %v", createSchema, err))
	}
	err = mysqlDB.Close()
	if err != nil {
		return &model.MetaDB{}, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on close general database sql [%v]: %v", createSchema, err))
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

	return &model.MetaDB{Gorm: gormDB}, nil
}

func NewMySQLDBEngine(mysqlCfg config.MySQLConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port, mysqlCfg.SchemaName, mysqlCfg.ConnectParams)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return mysqlDB, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on open mysql database connection [%v]: %v", mysqlCfg.SchemaName, err))
	}

	mysqlDB.SetMaxIdleConns(common.MysqlMaxIdleConn)
	mysqlDB.SetMaxOpenConns(common.MysqlMaxConn)
	mysqlDB.SetConnMaxLifetime(common.MysqlConnMaxLifeTime)
	mysqlDB.SetConnMaxIdleTime(common.MysqlConnMaxIdleTime)

	if err = mysqlDB.Ping(); err != nil {
		return mysqlDB, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("error on ping mysql database connection [%v]: %v", mysqlCfg.SchemaName, err))
	}

	return mysqlDB, nil
}
