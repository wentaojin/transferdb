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
package utils

// 同步操作类型
const (
	UpdateOperation        = "UPDATE"
	InsertOperation        = "INSERT"
	DeleteOperation        = "DELETE"
	TruncateOperation      = "TRUNCATE"
	DropOperation          = "DROP"
	DDLOperation           = "DDL"
	TruncateTableOperation = "TRUNCATE TABLE"
	DropTableOperation     = "DROP TABLE"
)

// 用于控制当程序消费追平到当前 CURRENT 重做日志，
// 当值 == 0 启用 filterOracleRedoGreaterOrEqualRecordByTable 大于或者等于
// 当值 == 1 启用 filterOracleRedoGreaterOrEqualRecordByTable 大于，避免已被消费得日志一直被重复消费
var CurrentResetFlag = 0

const (
	// MySQL 支持 check 约束版本 > 8.0.15
	MySQLCheckConsVersion = "8.0.15"
	// MySQL 版本分隔符号
	MySQLVersionDelimiter = "-"
	// MySQL 字符集
	MySQLCharacterSet = "UTF8MB4"

	// JSON 格式化某字段
	ColumnsJSON      = "COLUMN"
	IndexJSON        = "INDEX"
	PUConstraintJSON = "PUK"
	FKConstraintJSON = "FK"
	CKConstraintJSON = "CK"
	PartitionJSON    = "PARTITION"

	// 目标数据库类型
	TiDBTargetDBType  = "TIDB"
	MySQLTargetDBType = "MYSQL"

	// 数据全量/实时同步 Oracle 版本要求
	// 要求 oracle 11g 及以上
	OracleSYNCRequireDBVersion = "11"

	// 允许 Oracle 表、字段 Collation
	// 需要 oracle 12.2g 及以上
	OracleTableColumnCollationDBVersion = "12.2"

	// Oracle 用户、表、字段默认使用 DB 排序规则
	OracleUserTableColumnDefaultCollation = "USING_NLS_COMP"

	// CSV 字符集判断
	UTF8CharacterSetCSV = "UTF8"
	GBKCharacterSetCSV  = "GBK"

	// 模式
	ReverseMode = "Reverse"
	CheckMode   = "Check"
	DiffMode    = "Diff"

	// Channel
	BufferSize = 1024

	// TiDB 数据库
	TiDBClusteredIndexIntOnlyValue = "INT_ONLY"
	TiDBClusteredIndexONValue      = "ON"
	TiDBClusteredIndexOFFValue     = "OFF"

	// Reverse 模式
	ReverseModeO2M = "O2M"
	ReverseModeM2O = "M2O"
)

// alter-primary-key = fase 主键整型数据类型列表
var TiDBIntegerPrimaryKeyList = []string{"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"}

// MySQL 8.0
// utf8mb4_0900_as_cs 区分重音、区分大小写的排序规则
// utf8mb4_0900_ai_ci 不区分重音和不区分大小写的排序规则
// utf8mb4_0900_as_ci 区分重音、不区分大小写的排序规则
// Oracle 字段 Collation 映射
var OracleCollationMap = map[string]string{
	// ORACLE 12.2 及以上版本
	// 不区分大小写，但区分重音
	// MySQL 8.0 ONlY
	"BINARY_CI": "utf8mb4_0900_as_ci",
	// 不区分大小写和重音
	"BINARY_AI": "utf8mb4_general_ci",
	// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值
	"BINARY_CS": "utf8mb4_bin",
	// ORACLE 12.2 以下版本
	// 区分大小写和重音
	"BINARY": "utf8mb4_bin",
}

// ORACLE 字符集映射规则
var OracleDBCharacterSetMap = map[string]string{
	"AL32UTF8": "UTF8MB4",
	"UTF8":     "UTF8MB4",
	"ZHS16GBK": "GBK",
}

// ORACLE 字符集映射规则
var OracleDBCSVCharacterSetMap = map[string]string{
	"AL32UTF8": "UTF8",
	"UTF8":     "UTF8",
	"ZHS16GBK": "GBK",
}

// ORACLE 默认值规则映射规则
const (
	DefaultValueSysdate    = "SYSDATE"
	DefaultValueSysdateMap = "NOW()"
)

// MySQL 字符集映射规则
var MySQLDBCharacterSetMap = map[string]string{
	"UTF8MB4": "AL32UTF8",
	"UTF8":    "AL32UTF8",
	"GBK":     "AL32UTF8",
}

var MySQLDBCollationMap = map[string]string{
	// ORACLE 12.2 及以上版本
	// 不区分大小写，但区分重音
	// MySQL 8.0 ONlY
	"utf8mb4_0900_as_ci": "BINARY_CI",
	// 不区分大小写和重音
	"utf8mb4_general_ci": "BINARY_AI",
	// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值 BINARY_CS
	// ORACLE 12.2 以下版本：区分大小写和重音 BINARY
	"utf8mb4_bin": "BINARY/BINARY_CS",
	// MySQL 8.0 ONlY
	"utf8_0900_as_ci": "BINARY_CI",
	// 不区分大小写和重音
	"utf8_general_ci": "BINARY_AI",
	// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值 BINARY_CS
	// ORACLE 12.2 以下版本：区分大小写和重音 BINARY
	"utf8_bin": "BINARY/BINARY_CS",
}

// Oracle 不支持数据类型 -> M2O
var OracleIsNotSupportDataType = []string{"ENUM", "SET"}

// MySQL 默认值规则映射规则
const (
	DefaultValueNow    = "CURRENT_TIMESTAMP"
	DefaultValueNowMAP = "SYSDATE"
)
