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

/*
	Common Constant
*/

const (
	// MySQL 支持 check 约束版本 > 8.0.15
	MySQLCheckConsVersion = "8.0.15"
	// MySQL 表达式索引版本 > 8.0.0
	MySQLExpressionIndexVersion = "8.0.0"
	// MySQL 版本分隔符号
	MySQLVersionDelimiter = "-"

	// 允许 Oracle 表、字段 Collation
	// 需要 oracle 12.2g 及以上
	OracleTableColumnCollationDBVersion = "12.2"

	// Oracle 用户、表、字段默认使用 DB 排序规则
	OracleUserTableColumnDefaultCollation = "USING_NLS_COMP"

	// Struct JSON 格式化 -> Check 阶段
	JSONColumns      = "COLUMN"
	JSONIndex        = "INDEX"
	JSONPUConstraint = "PUK"
	JSONFKConstraint = "FK"
	JSONCKConstraint = "CK"
	JSONPartition    = "PARTITION"
)

const (
	// TiDB 数据库
	TiDBClusteredIndexIntOnlyValue = "INT_ONLY"
	TiDBClusteredIndexONValue      = "ON"
	TiDBClusteredIndexOFFValue     = "OFF"
)

// alter-primary-key = fase 主键整型数据类型列表
var TiDBIntegerPrimaryKeyList = []string{"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"}

// 表结构迁移以及表结构校验字符集、排序规则 MAP
var MigrateTableStructureDatabaseCharsetMap = map[string]map[string]string{
	TaskTypeOracle2MySQL: {
		"AL32UTF8":     "UTF8MB4",
		"ZHT16BIG5":    "BIG5",
		"ZHS16GBK":     "GBK",
		"ZHS32GB18030": "GB18030",
	},
	// TiDB 统一使用 UTF8MB4 字符集
	TaskTypeOracle2TiDB: {
		"AL32UTF8":     "UTF8MB4",
		"ZHT16BIG5":    "UTF8MB4",
		"ZHS16GBK":     "UTF8MB4",
		"ZHS32GB18030": "UTF8MB4",
	},
	TaskTypeMySQL2Oracle: {
		"UTF8MB4": "AL32UTF8",
		"UTF8":    "AL32UTF8",
		"BIG5":    "ZHT16BIG5",
		"GBK":     "ZHS16GBK",
		"GB18030": "ZHS32GB18030",
	},
	TaskTypeTiDB2Oracle: {
		"UTF8MB4": "AL32UTF8",
		"UTF8":    "AL32UTF8",
		"GBK":     "ZHS16GBK",
	},
}

// MySQL 8.0
// utf8mb4_0900_as_cs 区分重音、区分大小写的排序规则
// utf8mb4_0900_ai_ci 不区分重音和不区分大小写的排序规则
// utf8mb4_0900_as_ci 区分重音、不区分大小写的排序规则
// Oracle 字段 Collation 映射
var MigrateTableStructureDatabaseCollationMap = map[string]map[string]map[string]string{
	TaskTypeOracle2MySQL: {
		// ORACLE 12.2 及以上版本
		// 不区分大小写，但区分重音
		// MySQL 8.0 ONlY
		"BINARY_CI": {
			"UTF8MB4": "UTF8MB4_0900_AS_CI",
			"UTF8":    "UTF8_0900_AS_CI",
			"BIG5":    "BIG5_CHINESE_CI",    // 无此排序规则，用 BIG5_CHINESE_CI 代替
			"GBK":     "GBK_CHINESE_CI",     // 无此排序规则，用 GBK_CHINESE_CI 代替
			"GB18030": "GB18030_CHINESE_CI", // 无此排序规则，用 GB18030_CHINESE_CI 代替，gb18030_unicode_520_ci 区分大小写，但不区分重音
		},
		// 不区分大小写和重音
		"BINARY_AI": {
			"UTF8MB4": "UTF8MB4_GENERAL_CI",
			"UTF8":    "UTF8_GENERAL_CI",
			"BIG5":    "BIG5_CHINESE_CI",
			"GBK":     "GBK_CHINESE_CI",
			"GB18030": "GB18030_CHINESE_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值
		"BINARY_CS": {
			"UTF8MB4": "UTF8MB4_BIN",
			"UTF8":    "UTF8_BIN",
			"BIG5":    "BIG5_BIN",
			"GBK":     "GBK_BIN",
			"GB18030": "GB18030_BIN",
		},
		// ORACLE 12.2 以下版本
		// 区分大小写和重音
		"BINARY": {
			"UTF8MB4": "UTF8MB4_BIN",
			"UTF8":    "UTF8_BIN",
			"BIG5":    "BIG5_BIN",
			"GBK":     "GBK_BIN",
			"GB18030": "GB18030_BIN",
		},
	},
	// Charset 统一 UTF8MB4
	TaskTypeOracle2TiDB: {
		// ORACLE 12.2 及以上版本
		// 不区分大小写，但区分重音
		// MySQL 8.0 ONlY
		"BINARY_CI": {
			"UTF8MB4": "UTF8MB4_0900_AS_CI",
			"UTF8":    "UTF8_0900_AS_CI",
			"BIG5":    "BIG5_CHINESE_CI",    // 无此排序规则，用 BIG5_CHINESE_CI 代替
			"GBK":     "GBK_CHINESE_CI",     // 无此排序规则，用 GBK_CHINESE_CI 代替
			"GB18030": "GB18030_CHINESE_CI", // 无此排序规则，用 GB18030_CHINESE_CI 代替，gb18030_unicode_520_ci 区分大小写，但不区分重音
		},
		// 不区分大小写和重音
		"BINARY_AI": {
			"UTF8MB4": "UTF8MB4_GENERAL_CI",
			"UTF8":    "UTF8_GENERAL_CI",
			"BIG5":    "BIG5_CHINESE_CI",
			"GBK":     "GBK_CHINESE_CI",
			"GB18030": "GB18030_CHINESE_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值
		"BINARY_CS": {
			"UTF8MB4": "UTF8MB4_BIN",
			"UTF8":    "UTF8_BIN",
			"BIG5":    "BIG5_BIN",
			"GBK":     "GBK_BIN",
			"GB18030": "GB18030_BIN",
		},
		// ORACLE 12.2 以下版本
		// 区分大小写和重音
		"BINARY": {
			"UTF8MB4": "UTF8MB4_BIN",
			"UTF8":    "UTF8_BIN",
			"BIG5":    "BIG5_BIN",
			"GBK":     "GBK_BIN",
			"GB18030": "GB18030_BIN",
		},
	},
	TaskTypeMySQL2Oracle: {
		// ORACLE 12.2 及以上版本
		// 不区分大小写，但区分重音
		// MySQL 8.0 ONlY
		// 无此排序规则，用 BIG5_CHINESE_CI 代替
		// 无此排序规则，用 GBK_CHINESE_CI 代替
		// 无此排序规则，用 GB18030_CHINESE_CI 代替，gb18030_unicode_520_ci 区分大小写，但不区分重音
		"UTF8MB4_0900_AS_CI": {
			"AL32UTF8":     "BINARY_CI",
			"ZHT16BIG5":    "BINARY_CI",
			"ZHS16GBK":     "BINARY_CI",
			"ZHS32GB18030": "BINARY_CI",
		},
		// 不区分大小写和重音
		"UTF8MB4_GENERAL_CI": {
			"AL32UTF8":     "BINARY_AI",
			"ZHT16BIG5":    "BINARY_AI",
			"ZHS16GBK":     "BINARY_AI",
			"ZHS32GB18030": "BINARY_AI",
		},
		"UTF8_GENERAL_CI": {
			"AL32UTF8":     "BINARY_AI",
			"ZHT16BIG5":    "BINARY_AI",
			"ZHS16GBK":     "BINARY_AI",
			"ZHS32GB18030": "BINARY_AI",
		},
		"BIG5_CHINESE_CI": {
			"AL32UTF8":     "BINARY_AI/BINARY_CI",
			"ZHT16BIG5":    "BINARY_AI/BINARY_CI",
			"ZHS16GBK":     "BINARY_AI/BINARY_CI",
			"ZHS32GB18030": "BINARY_AI/BINARY_CI",
		},
		"GBK_CHINESE_CI": {
			"AL32UTF8":     "BINARY_AI/BINARY_CI",
			"ZHT16BIG5":    "BINARY_AI/BINARY_CI",
			"ZHS16GBK":     "BINARY_AI/BINARY_CI",
			"ZHS32GB18030": "BINARY_AI/BINARY_CI",
		},
		"GB18030_CHINESE_CI": {
			"AL32UTF8":     "BINARY_AI/BINARY_CI",
			"ZHT16BIG5":    "BINARY_AI/BINARY_CI",
			"ZHS16GBK":     "BINARY_AI/BINARY_CI",
			"ZHS32GB18030": "BINARY_AI/BINARY_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值 -> BINARY_CS
		// ORACLE 12.2 以下版本 -> BINARY
		// 区分大小写和重音
		"UTF8MB4_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
		"UTF8_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
		"BIG5_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
		"GBK_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
		"GB18030_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
	},
	TaskTypeTiDB2Oracle: {
		// ORACLE 12.2 及以上版本
		// 不区分大小写，但区分重音
		// MySQL 8.0 ONlY
		// 无此排序规则，用 BIG5_CHINESE_CI 代替
		// 无此排序规则，用 GBK_CHINESE_CI 代替
		// 无此排序规则，用 GB18030_CHINESE_CI 代替，gb18030_unicode_520_ci 区分大小写，但不区分重音
		"UTF8MB4_0900_AS_CI": {
			"AL32UTF8":     "BINARY_CI",
			"ZHT16BIG5":    "BINARY_CI",
			"ZHS16GBK":     "BINARY_CI",
			"ZHS32GB18030": "BINARY_CI",
		},
		// 不区分大小写和重音
		"UTF8MB4_GENERAL_CI": {
			"AL32UTF8":     "BINARY_AI",
			"ZHT16BIG5":    "BINARY_AI",
			"ZHS16GBK":     "BINARY_AI",
			"ZHS32GB18030": "BINARY_AI",
		},
		"UTF8_GENERAL_CI": {
			"AL32UTF8":     "BINARY_AI",
			"ZHT16BIG5":    "BINARY_AI",
			"ZHS16GBK":     "BINARY_AI",
			"ZHS32GB18030": "BINARY_AI",
		},
		"GBK_CHINESE_CI": {
			"AL32UTF8":     "BINARY_AI/BINARY_CI",
			"ZHT16BIG5":    "BINARY_AI/BINARY_CI",
			"ZHS16GBK":     "BINARY_AI/BINARY_CI",
			"ZHS32GB18030": "BINARY_AI/BINARY_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值 -> BINARY_CS
		// ORACLE 12.2 以下版本 -> BINARY
		// 区分大小写和重音
		"UTF8MB4_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
		"UTF8_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
		"GBK_BIN": {
			"AL32UTF8":     "BINARY/BINARY_CS",
			"ZHT16BIG5":    "BINARY/BINARY_CS",
			"ZHS16GBK":     "BINARY/BINARY_CS",
			"ZHS32GB18030": "BINARY/BINARY_CS",
		},
	},
}

// 数据迁移以及数据校验
// 字符类型数据映射规则 -> 用于程序连接源端数据库读取数据字符类型数据，以对应字符集写入下游数据库
var MigrateStringDataTypeDatabaseCharsetMap = map[string]map[string]string{
	TaskTypeOracle2MySQL: {
		"AL32UTF8":     "UTF8MB4",
		"ZHT16BIG5":    "BIG5",
		"ZHS16GBK":     "GBK",
		"ZHS32GB18030": "GB18030",
	},
	TaskTypeOracle2TiDB: {
		"AL32UTF8":     "UTF8MB4",
		"ZHT16BIG5":    "UTF8MB4",
		"ZHS16GBK":     "UTF8MB4",
		"ZHS32GB18030": "UTF8MB4",
	},
}

// transferdb 字符类型数据字符集转换支持列表
var (
	CharsetUTF8MB4 = "UTF8MB4"
	CharsetBIG5    = "BIG5"
	CharsetGBK     = "GBK"
	CharsetGB18030 = "GBK18030"
)
var TransferDBStringDataTypeCharsetTransformList = []string{CharsetUTF8MB4, CharsetGBK, CharsetGB18030, CharsetBIG5}

// Oracle 不支持数据类型 -> M2O
var OracleIsNotSupportDataType = []string{"ENUM", "SET"}

// MySQL Reverse M2O
// mysql 默认值未区分，字符数据、数值数据，用于匹配 mysql 字符串默认值，判断是否需单引号
// 默认值 uuid() 匹配到 xxx() 括号结尾，不需要单引号
// 默认值 CURRENT_TIMESTAMP 不需要括号，内置转换成 ORACLE SYSDATE
// 默认值 skp 或者 1 需要单引号
var SpecialMySQLDataDefaultsWithDataTYPE = []string{"TIME",
	"DATE",
	"DATETIME",
	"TIMESTAMP",
	"CHAR",
	"VARCHAR",
	"TINYTEXT",
	"TEXT", "MEDIUMTEX", "LONGTEXT", "BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"}

// MySQL Data Type reverse Oracle CLOB or NCLOB configure collation error, need configure columnCollation = ""
// ORA-43912: invalid collation specified for a CLOB or NCLOB value
// columnCollation = ""
var SpecialMySQLColumnCollationWithDataTYPE = []string{"TINYTEXT",
	"TEXT",
	"MEDIUMTEXT",
	"LONGTEXT"}
