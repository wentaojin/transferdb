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
	MySQLMaxIdleConn     = 512
	MySQLMaxConn         = 1024
	MySQLConnMaxLifeTime = 300 * time.Second
	MySQLConnMaxIdleTime = 200 * time.Second
)

// 任务并发通道 Channle Size
const ChannelBufferSize = 1024

// 任务模式
const (
	TaskModePrepare = "PREPARE"
	TaskModeAssess  = "ASSESS"
	TaskModeReverse = "REVERSE"
	TaskModeCheck   = "CHECK"
	TaskModeCompare = "COMPARE"
	TaskModeCSV     = "CSV"
	TaskModeFull    = "FULL"
	TaskModeAll     = "ALL"
)

// 任务状态
const (
	TaskStatusWaiting = "WAITING"
	TaskStatusRunning = "RUNNING"
	TaskStatusSuccess = "SUCCESS"
	TaskStatusFailed  = "FAILED"
)

// 任务初始值
const (
	// 值 0 代表源端表未进行初始化 -> 适用于 full/csv/all 模式
	TaskTableDefaultSourceGlobalSCN = 0
	// 值 -1 代表源端表未进行 chunk 切分 -> 适用于 full/csv/all 模式
	TaskTableDefaultSplitChunkNums = -1
)

// 任务 DB 类型
const (
	DatabaseTypeOracle = "ORACLE"
	DatabaseTypeTiDB   = "TIDB"
	DatabaseTypeMySQL  = "MYSQL"
)

// 任务类型
const (
	TaskTypeOracle2MySQL = "ORACLE2MYSQL"
	TaskTypeOracle2TiDB  = "ORACLE2TIDB"
	TaskTypeMySQL2Oracle = "MYSQL2ORACLE"
	TaskTypeTiDB2Oracle  = "TIDB2ORACLE"
)
