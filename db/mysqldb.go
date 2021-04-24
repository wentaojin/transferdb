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

	"github.com/wentaojin/transferdb/pkg/config"

	gormLogger "gorm.io/gorm/logger"

	"github.com/wentaojin/transferdb/zlog"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 创建 mysql 数据库引擎
func NewMySQLEnginePrepareDB(mysqlCfg config.TargetConfig, slowQueryThreshold int) (*Engine, error) {
	// 通用数据库链接池
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port)

	// 初始化 gorm 日志记录器
	gLogger := zlog.NewGormLogger(zlog.Logger, time.Duration(slowQueryThreshold)*time.Millisecond)
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
	_, _, err = Query(sqlDB, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, mysqlCfg.MetaSchema))
	if err != nil {
		return &Engine{}, err
	}

	engine, err := NewMySQLEngineGeneralDB(mysqlCfg, slowQueryThreshold)
	if err != nil {
		return engine, err
	}
	return engine, nil
}

func NewMySQLEngineGeneralDB(mysqlCfg config.TargetConfig, slowQueryThreshold int) (*Engine, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		mysqlCfg.Username, mysqlCfg.Password, mysqlCfg.Host, mysqlCfg.Port, mysqlCfg.MetaSchema, mysqlCfg.ConnectParams)
	// 初始化 gorm 日志记录器
	var (
		gormDB *gorm.DB
		err    error
	)
	gLogger := zlog.NewGormLogger(zlog.Logger, time.Duration(slowQueryThreshold)*time.Millisecond)
	gLogger.LogMode(gormLogger.Warn)
	gLogger.SetAsDefault()
	gormDB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
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
		&TableMeta{},
		&CustomTableColumnTypeMap{},
		&CustomSchemaColumnTypeMap{},
		&TableFullMeta{},
		&TableIncrementMeta{},
	); err != nil {
		return fmt.Errorf("init mysql engine db data failed: %v", err)
	}
	return nil
}
