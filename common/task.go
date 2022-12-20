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
package common

import "time"

// MySQL 连接配置
const (
	MysqlMaxIdleConn     = 512
	MysqlMaxConn         = 1024
	MysqlConnMaxLifeTime = 300 * time.Second
	MysqlConnMaxIdleTime = 200 * time.Second
)

// 任务并发通道 CHANNEL Size
const BufferSize = 1024

// 任务模式
const (
	ReverseO2MMode = "ReverseO2M"
	ReverseM2OMode = "ReverseM2O"
	CheckO2MMode   = "CheckO2M"
	CompareO2MMode = "CompareO2M"
	CSVO2MMode     = "CsvO2M"
	FullO2MMode    = "FullO2M"
	AllO2MMode     = "AllO2M"
)

// 任务DB
const (
	TaskDBOracle = "ORACLE"
	TaskDBTiDB   = "TIDB"
	TaskDBMySQL  = "MYSQL"
)

// 任务类型
const (
	TaskTypeConfig        = "CONFIG"
	TaskTypeDatabase      = "DATABASE"
	TaskTypeObjectAssess  = "OBJECT_ASSESS"
	TaskTypeObjectReverse = "OBJECT_REVERSE"
	TaskTypeObjectCheck   = "OBJECT_CHECK"

	TaskTypeDataCompare     = "DATA_COMPARE"
	TaskTypeDataSQLMigrate  = "DATA_SQL_MIGRATE"
	TaskTypeDataIncrMigrate = "DATA_INCR_MIGRATE"
	TaskTypeDataCSVMigrate  = "DATA_CSV_MIGRATE"
)
