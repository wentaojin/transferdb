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
	OracleGBKCharacterSet  = "GBK"
	OracleUTF8CharacterSet = "UTF8"
	// oracle collation 默认大小写敏感，a != A
	OracleCollationBin = "BIN"
	// MySQL 支持 check 约束版本 > 8.0.15
	MySQLCheckConsVersion = "8.0.15"
	// MySQL 版本分隔符号
	MySQLVersionDelimiter = "-"
	// MySQL 字符集/排序规则
	MySQLCharacterSet = "utf8mb4"
	MySQLCollation    = "utf8mb4_bin"

	// JSON 格式化某字段
	ColumnsJSON      = "column"
	IndexJSON        = "index"
	PUConstraintJSON = "puk"
	FKConstraintJSON = "fk"
	CKConstraintJSON = "ck"
	PartitionJSON    = "partition"

	// 默认值规则
	DefaultValueSysdate    = "SYSDATE"
	DefaultValueSysdateMap = "NOW()"
)
