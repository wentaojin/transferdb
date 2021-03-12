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
package db

import (
	"fmt"
	"time"

	gormLogger "gorm.io/gorm/logger"

	"github.com/WentaoJin/transferdb/zlog"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	// gorm 元数据库慢日志阈值
	slowQueryThreshold = 300
)

// 创建 mysql 数据库引擎
func NewMySQLEnginePrepareDB(username string, password string, host string, port int, schema string) (*Engine, error) {
	// 通用数据库链接池
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local", username, password, host, port)

	// 初始化 gorm 日志记录器
	gLogger := zlog.NewGormLogger(zlog.Logger, slowQueryThreshold)
	gLogger.LogMode(gormLogger.Warn)
	gLogger.SetAsDefault()
	gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gLogger,
	})
	if err != nil {
		return &Engine{}, fmt.Errorf("error on initializing mysql database connection [no-schema]: %v", err)
	}
	sqlDB, err := gormDB.DB()
	if err != nil {
		return &Engine{}, fmt.Errorf("error on ping mysql database connection [no-schema]:%v", err)
	}
	_, _, err = Query(sqlDB, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, schema))
	if err != nil {
		return &Engine{}, err
	}

	engine, err := NewMySQLEngineGeneralDB(username, password, host, port, schema)
	if err != nil {
		return engine, err
	}
	return engine, nil
}

func NewMySQLEngineGeneralDB(username string, password string, host string, port int, schema string) (*Engine, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true", username, password, host, port, schema)
	// 初始化 gorm 日志记录器
	gLogger := zlog.NewGormLogger(zlog.Logger, slowQueryThreshold)
	gLogger.LogMode(gormLogger.Warn)
	gLogger.SetAsDefault()
	gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		// 禁用外键（指定外键时不会在 mysql 创建真实的外键约束）
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
		Logger:                                   gLogger,
	})
	if err != nil {
		return &Engine{}, fmt.Errorf("error on initializing mysql database connection [meta-schema]: %v", err)
	}

	// 初始化数据库连接池
	sqlDB, err := gormDB.DB()
	if err != nil {
		return &Engine{}, fmt.Errorf("error on ping mysql database connection [meta-schema]: %v", err)
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return &Engine{
		MysqlDB: sqlDB,
		GormDB:  gormDB,
	}, nil
}

// 初始化同步表结构
func (e *Engine) InitMysqlEngineDB() error {
	if err := e.GormDB.AutoMigrate(
		// todo: 自定义表名适配删除 - 数据同步不支持表名不一致
		//&CustomTableNameMap{},
		&CustomTableColumnTypeMap{},
		&CustomSchemaColumnTypeMap{},
		&TableFullMeta{},
		&TableIncrementMeta{},
	); err != nil {
		return fmt.Errorf("init mysql engine db data failed: %v", err)
	}
	return nil
}
