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

// transferdb 字符类型数据字符集转换支持列表
var (
	MYSQLCharsetUTF8MB4       = "UTF8MB4"
	MYSQLCharsetUTF8          = "UTF8"
	MYSQLCharsetBIG5          = "BIG5"
	MYSQLCharsetGBK           = "GBK"
	MYSQLCharsetGB18030       = "GB18030"
	ORACLECharsetAL32UTF8     = "AL32UTF8"
	ORACLECharsetZHT16BIG5    = "ZHT16BIG5"
	ORACLECharsetZHS16GBK     = "ZHS16GBK"
	ORACLECharsetZHS32GB18030 = "ZHS32GB18030"
)

// Oracle 字符集转换映射程序字符集
const (
	CharsetUTF8MB4 = "UTF8MB4"
	CharsetGB18030 = "GB18030"
	CharsetBIG5    = "BIG5"
	CharsetGBK     = "GBK"
)

var OracleCharsetStringConvertMapping = map[string]string{
	ORACLECharsetAL32UTF8:     CharsetUTF8MB4,
	ORACLECharsetZHT16BIG5:    CharsetBIG5,
	ORACLECharsetZHS16GBK:     CharsetGBK,
	ORACLECharsetZHS32GB18030: CharsetGB18030,
}

// 表结构迁移以及表结构校验字符集、排序规则
// 用于表结构以及字段属性字符集映射规则
var MigrateTableStructureDatabaseCharsetMap = map[string]map[string]string{
	TaskTypeOracle2MySQL: {
		ORACLECharsetAL32UTF8:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHT16BIG5:    MYSQLCharsetBIG5,
		ORACLECharsetZHS16GBK:     MYSQLCharsetGBK,
		ORACLECharsetZHS32GB18030: MYSQLCharsetGB18030,
	},
	// TiDB 表结构以及字段属性统一使用 UTF8MB4 字符集，适用于 check、compare、reverse 模式下 o2t、t2o
	TaskTypeOracle2TiDB: {
		ORACLECharsetAL32UTF8:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHT16BIG5:    MYSQLCharsetUTF8MB4,
		ORACLECharsetZHS16GBK:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHS32GB18030: MYSQLCharsetUTF8MB4,
	},
	TaskTypeMySQL2Oracle: {
		MYSQLCharsetUTF8MB4: ORACLECharsetAL32UTF8,
		MYSQLCharsetUTF8:    ORACLECharsetAL32UTF8,
		MYSQLCharsetBIG5:    ORACLECharsetZHT16BIG5,
		MYSQLCharsetGBK:     ORACLECharsetZHS16GBK,
		MYSQLCharsetGB18030: ORACLECharsetZHS32GB18030,
	},
	TaskTypeTiDB2Oracle: {
		MYSQLCharsetUTF8MB4: ORACLECharsetAL32UTF8,
		MYSQLCharsetUTF8:    ORACLECharsetAL32UTF8,
		MYSQLCharsetGBK:     ORACLECharsetZHS16GBK,
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
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AS_CI",
			MYSQLCharsetUTF8:    "UTF8_0900_AS_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",    // 无此排序规则，用 BIG5_CHINESE_CI 代替
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",     // 无此排序规则，用 GBK_CHINESE_CI 代替
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI", // 无此排序规则，用 GB18030_CHINESE_CI 代替，gb18030_unicode_520_ci 区分大小写，但不区分重音
		},
		// 不区分大小写和重音
		"BINARY_AI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_GENERAL_CI",
			MYSQLCharsetUTF8:    "UTF8_GENERAL_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值
		"BINARY_CS": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
		// ORACLE 12.2 以下版本
		// 区分大小写和重音
		"BINARY": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
	},
	// Charset 统一 UTF8MB4
	TaskTypeOracle2TiDB: {
		// ORACLE 12.2 及以上版本
		// 不区分大小写，但区分重音
		// MySQL 8.0 ONlY
		"BINARY_CI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AS_CI",
			MYSQLCharsetUTF8:    "UTF8_0900_AS_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",    // 无此排序规则，用 BIG5_CHINESE_CI 代替
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",     // 无此排序规则，用 GBK_CHINESE_CI 代替
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI", // 无此排序规则，用 GB18030_CHINESE_CI 代替，gb18030_unicode_520_ci 区分大小写，但不区分重音
		},
		// 不区分大小写和重音
		"BINARY_AI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_GENERAL_CI",
			MYSQLCharsetUTF8:    "UTF8_GENERAL_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值
		"BINARY_CS": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
		// ORACLE 12.2 以下版本
		// 区分大小写和重音
		"BINARY": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
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
			ORACLECharsetAL32UTF8:     "BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_CI",
		},
		// 不区分大小写和重音
		"UTF8MB4_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"UTF8_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"BIG5_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		"GBK_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		"GB18030_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值 -> BINARY_CS
		// ORACLE 12.2 以下版本 -> BINARY
		// 区分大小写和重音
		"UTF8MB4_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"UTF8_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"BIG5_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"GBK_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"GB18030_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
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
			ORACLECharsetAL32UTF8:     "BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_CI",
		},
		// 不区分大小写和重音
		"UTF8MB4_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"UTF8_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"GBK_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		// 区分大小写和重音，如果不使用扩展名下，该规则是 ORACLE 默认值 -> BINARY_CS
		// ORACLE 12.2 以下版本 -> BINARY
		// 区分大小写和重音
		"UTF8MB4_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"UTF8_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"GBK_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
	},
}

// 数据迁移、数据校验、表结构默认值、注释
// 字符类型数据映射规则
// 1、用于程序连接源端数据库读取数据字符类型数据，以对应字符集写入下游数据库
// 2、用于数据表默认值或者注释
var MigrateStringDataTypeDatabaseCharsetMap = map[string]map[string]string{
	TaskTypeOracle2MySQL: {
		ORACLECharsetAL32UTF8:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHT16BIG5:    MYSQLCharsetBIG5,
		ORACLECharsetZHS16GBK:     MYSQLCharsetGBK,
		ORACLECharsetZHS32GB18030: MYSQLCharsetGB18030,
	},
	TaskTypeOracle2TiDB: {
		ORACLECharsetAL32UTF8:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHT16BIG5:    MYSQLCharsetBIG5,
		ORACLECharsetZHS16GBK:     MYSQLCharsetGBK,
		ORACLECharsetZHS32GB18030: MYSQLCharsetGB18030,
	},
	TaskTypeMySQL2Oracle: {
		MYSQLCharsetUTF8MB4: ORACLECharsetAL32UTF8,
		MYSQLCharsetBIG5:    ORACLECharsetZHT16BIG5,
		MYSQLCharsetGBK:     ORACLECharsetZHS16GBK,
		MYSQLCharsetGB18030: ORACLECharsetZHS32GB18030,
	},
	TaskTypeTiDB2Oracle: {
		MYSQLCharsetUTF8MB4: ORACLECharsetAL32UTF8,
		MYSQLCharsetBIG5:    ORACLECharsetZHT16BIG5,
		MYSQLCharsetGBK:     ORACLECharsetZHS16GBK,
		MYSQLCharsetGB18030: ORACLECharsetZHS32GB18030,
	},
}

var MigrateDataSupportCharset = []string{MYSQLCharsetUTF8MB4, MYSQLCharsetGBK, MYSQLCharsetBIG5, MYSQLCharsetGB18030}

// 表结构迁移大小写
const (
	MigrateTableStructFieldNameOriginCase = "0"
	MigrateTableStructFieldNameLowerCase  = "1"
	MigrateTableStructFieldNameUpperCase  = "2"
)

// Oracle Table Attr Null 特殊处理
const (
	OracleNULLSTRINGTableAttrWithoutNULL = "NULLSTRING"
	OracleNULLSTRINGTableAttrWithCustom  = ""
)

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
