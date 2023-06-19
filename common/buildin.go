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
	Database Default Value Map
*/
// ORACLE 默认值规则映射规则 O2M
const (
	BuildInOracleColumnDefaultValueSysdate = "SYSDATE"
	BuildInOracleColumnDefaultValueSYSGUID = "SYS_GUID()"
	BuildInOracleColumnDefaultValueNULL    = ""
)

var BuildInOracleO2MColumnDefaultValueMap = map[string]string{
	BuildInOracleColumnDefaultValueSysdate: "NOW()",
	BuildInOracleColumnDefaultValueSYSGUID: "UUID()",
	BuildInOracleColumnDefaultValueNULL:    "NULL",
}

// MySQL 默认值规则映射规则 M2O
const (
	BuildInMySQLColumnDefaultValueCurrentTimestamp = "CURRENT_TIMESTAMP"
	BuildInMySQLColumnDefaultValueNULL             = "NULL"
)

var BuildInMySQLM2OColumnDefaultValueMap = map[string]string{
	BuildInMySQLColumnDefaultValueCurrentTimestamp: "SYSDATE",
	BuildInMySQLColumnDefaultValueNULL:             "NULL",
}

/*
	Database Datatype Name
*/
// Oracle 数据类型名
const (
	BuildInOracleDatatypeNumber             = "NUMBER"
	BuildInOracleDatatypeBfile              = "BFILE"
	BuildInOracleDatatypeChar               = "CHAR"
	BuildInOracleDatatypeCharacter          = "CHARACTER"
	BuildInOracleDatatypeClob               = "CLOB"
	BuildInOracleDatatypeBlob               = "BLOB"
	BuildInOracleDatatypeDate               = "DATE"
	BuildInOracleDatatypeDecimal            = "DECIMAL"
	BuildInOracleDatatypeDec                = "DEC"
	BuildInOracleDatatypeDoublePrecision    = "DOUBLE PRECISION"
	BuildInOracleDatatypeFloat              = "FLOAT"
	BuildInOracleDatatypeInteger            = "INTEGER"
	BuildInOracleDatatypeInt                = "INT"
	BuildInOracleDatatypeLong               = "LONG"
	BuildInOracleDatatypeLongRAW            = "LONG RAW"
	BuildInOracleDatatypeBinaryFloat        = "BINARY_FLOAT"
	BuildInOracleDatatypeBinaryDouble       = "BINARY_DOUBLE"
	BuildInOracleDatatypeNchar              = "NCHAR"
	BuildInOracleDatatypeNcharVarying       = "NCHAR VARYING"
	BuildInOracleDatatypeNclob              = "NCLOB"
	BuildInOracleDatatypeNumeric            = "NUMERIC"
	BuildInOracleDatatypeNvarchar2          = "NVARCHAR2"
	BuildInOracleDatatypeRaw                = "RAW"
	BuildInOracleDatatypeReal               = "REAL"
	BuildInOracleDatatypeRowid              = "ROWID"
	BuildInOracleDatatypeSmallint           = "SMALLINT"
	BuildInOracleDatatypeUrowid             = "UROWID"
	BuildInOracleDatatypeVarchar2           = "VARCHAR2"
	BuildInOracleDatatypeVarchar            = "VARCHAR"
	BuildInOracleDatatypeXmltype            = "XMLTYPE"
	BuildInOracleDatatypeIntervalYearMonth0 = "INTERVAL YEAR(0) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth1 = "INTERVAL YEAR(1) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth2 = "INTERVAL YEAR(2) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth3 = "INTERVAL YEAR(3) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth4 = "INTERVAL YEAR(4) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth5 = "INTERVAL YEAR(5) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth6 = "INTERVAL YEAR(6) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth7 = "INTERVAL YEAR(7) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth8 = "INTERVAL YEAR(8) TO MONTH"
	BuildInOracleDatatypeIntervalYearMonth9 = "INTERVAL YEAR(9) TO MONTH"

	BuildInOracleDatatypeTimestamp  = "TIMESTAMP"
	BuildInOracleDatatypeTimestamp0 = "TIMESTAMP(0)"
	BuildInOracleDatatypeTimestamp1 = "TIMESTAMP(1)"
	BuildInOracleDatatypeTimestamp2 = "TIMESTAMP(2)"
	BuildInOracleDatatypeTimestamp3 = "TIMESTAMP(3)"
	BuildInOracleDatatypeTimestamp4 = "TIMESTAMP(4)"
	BuildInOracleDatatypeTimestamp5 = "TIMESTAMP(5)"
	// datatype timestamp with pricision,view query show default timestamp(6)
	BuildInOracleDatatypeTimestamp6                  = "TIMESTAMP(6)"
	BuildInOracleDatatypeTimestamp7                  = "TIMESTAMP(7)"
	BuildInOracleDatatypeTimestamp8                  = "TIMESTAMP(8)"
	BuildInOracleDatatypeTimestamp9                  = "TIMESTAMP(9)"
	BuildInOracleDatatypeTimestampWithTimeZone0      = "TIMESTAMP(0) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone1      = "TIMESTAMP(1) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone2      = "TIMESTAMP(2) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone3      = "TIMESTAMP(3) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone4      = "TIMESTAMP(4) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone5      = "TIMESTAMP(5) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone6      = "TIMESTAMP(6) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone7      = "TIMESTAMP(7) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone8      = "TIMESTAMP(8) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithTimeZone9      = "TIMESTAMP(9) WITH TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone0 = "TIMESTAMP(0) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone1 = "TIMESTAMP(1) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone2 = "TIMESTAMP(2) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone3 = "TIMESTAMP(3) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone4 = "TIMESTAMP(4) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone5 = "TIMESTAMP(5) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone6 = "TIMESTAMP(6) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone7 = "TIMESTAMP(7) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone8 = "TIMESTAMP(8) WITH LOCAL TIME ZONE"
	BuildInOracleDatatypeTimestampWithLocalTimeZone9 = "TIMESTAMP(9) WITH LOCAL TIME ZONE"

	// INTERVAL DAY(p) TO SECOND(s) 组合过多，代码内匹配处理
	// p and s value max 9 -> INTERVAL DAY(9) TO SECOND(9)
	BuildInOracleDatatypeIntervalDay = "INTERVAL DAY"
)

// Oracle 数据类型名映射规则 O2M
var BuildInOracleO2MDatatypeNameMap = map[string]string{
	BuildInOracleDatatypeNumber:                      "TINYINT/SMALLINT/INT/BIGINT/DECIMAL",
	BuildInOracleDatatypeBfile:                       "VARCHAR",
	BuildInOracleDatatypeChar:                        "VARCHAR",
	BuildInOracleDatatypeCharacter:                   "VARCHAR",
	BuildInOracleDatatypeClob:                        "LONGTEXT",
	BuildInOracleDatatypeBlob:                        "BLOB",
	BuildInOracleDatatypeDate:                        "DATETIME",
	BuildInOracleDatatypeDecimal:                     "DECIMAL",
	BuildInOracleDatatypeDec:                         "DECIMAL",
	BuildInOracleDatatypeDoublePrecision:             "DOUBLE PRECISION",
	BuildInOracleDatatypeFloat:                       "DOUBLE",
	BuildInOracleDatatypeInteger:                     "INT",
	BuildInOracleDatatypeInt:                         "INT",
	BuildInOracleDatatypeLong:                        "LONGTEXT",
	BuildInOracleDatatypeLongRAW:                     "LONGBLOB",
	BuildInOracleDatatypeBinaryFloat:                 "DOUBLE",
	BuildInOracleDatatypeBinaryDouble:                "DOUBLE",
	BuildInOracleDatatypeNchar:                       "VARCHAR",
	BuildInOracleDatatypeNcharVarying:                "NCHAR VARYING",
	BuildInOracleDatatypeNclob:                       "TEXT",
	BuildInOracleDatatypeNumeric:                     "NUMERIC",
	BuildInOracleDatatypeNvarchar2:                   "VARCHAR",
	BuildInOracleDatatypeRaw:                         "VARBINARY",
	BuildInOracleDatatypeReal:                        "DOUBLE",
	BuildInOracleDatatypeRowid:                       "VARCHAR",
	BuildInOracleDatatypeSmallint:                    "SMALLINT",
	BuildInOracleDatatypeUrowid:                      "VARCHAR",
	BuildInOracleDatatypeVarchar2:                    "VARCHAR",
	BuildInOracleDatatypeVarchar:                     "VARCHAR",
	BuildInOracleDatatypeXmltype:                     "LONGTEXT",
	BuildInOracleDatatypeIntervalYearMonth0:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth1:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth2:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth3:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth4:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth5:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth6:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth7:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth8:          "VARCHAR",
	BuildInOracleDatatypeIntervalYearMonth9:          "VARCHAR",
	BuildInOracleDatatypeTimestamp:                   "TIMESTAMP",
	BuildInOracleDatatypeTimestamp0:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp1:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp2:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp3:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp4:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp5:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp6:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp7:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp8:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestamp9:                  "TIMESTAMP",
	BuildInOracleDatatypeTimestampWithTimeZone0:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone1:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone2:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone3:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone4:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone5:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone6:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone7:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone8:      "DATETIME",
	BuildInOracleDatatypeTimestampWithTimeZone9:      "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone0: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone1: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone2: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone3: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone4: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone5: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone6: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone7: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone8: "DATETIME",
	BuildInOracleDatatypeTimestampWithLocalTimeZone9: "DATETIME",
	BuildInOracleDatatypeIntervalDay:                 "VARCHAR",
}

// MySQL 数据类型名
const (
	BuildInMySQLDatatypeBigint          = "BIGINT"
	BuildInMySQLDatatypeDecimal         = "DECIMAL"
	BuildInMySQLDatatypeDouble          = "DOUBLE"
	BuildInMySQLDatatypeDoublePrecision = "DOUBLE PRECISION"
	BuildInMySQLDatatypeFloat           = "FLOAT"
	BuildInMySQLDatatypeInt             = "INT"
	BuildInMySQLDatatypeInteger         = "INTEGER"
	BuildInMySQLDatatypeMediumint       = "MEDIUMINT"
	BuildInMySQLDatatypeNumeric         = "NUMERIC"
	BuildInMySQLDatatypeReal            = "REAL"
	BuildInMySQLDatatypeSmallint        = "SMALLINT"
	BuildInMySQLDatatypeTinyint         = "TINYINT"
	BuildInMySQLDatatypeBit             = "BIT"
	BuildInMySQLDatatypeDate            = "DATE"
	BuildInMySQLDatatypeDatetime        = "DATETIME"
	BuildInMySQLDatatypeTimestamp       = "TIMESTAMP"
	BuildInMySQLDatatypeTime            = "TIME"
	BuildInMySQLDatatypeYear            = "YEAR"

	BuildInMySQLDatatypeBlob       = "BLOB"
	BuildInMySQLDatatypeChar       = "CHAR"
	BuildInMySQLDatatypeLongBlob   = "LONGBLOB"
	BuildInMySQLDatatypeLongText   = "LONGTEXT"
	BuildInMySQLDatatypeMediumBlob = "MEDIUMBLOB"
	BuildInMySQLDatatypeMediumText = "MEDIUMTEXT"
	BuildInMySQLDatatypeText       = "TEXT"
	BuildInMySQLDatatypeTinyBlob   = "TINYBLOB"
	BuildInMySQLDatatypeTinyText   = "TINYTEXT"
	BuildInMySQLDatatypeVarchar    = "VARCHAR"

	BuildInMySQLDatatypeBinary    = "BINARY"
	BuildInMySQLDatatypeVarbinary = "VARBINARY"

	// ORACLE ISN'T SUPPORT
	BuildInMySQLDatatypeSet  = "SET"
	BuildInMySQLDatatypeEnum = "ENUM"
)

// MySQL 数据类型名映射规则 M2O
var BuildInMySQLM2ODatatypeNameMap = map[string]string{
	BuildInMySQLDatatypeSmallint:        "NUMBER",
	BuildInMySQLDatatypeTinyint:         "NUMBER",
	BuildInMySQLDatatypeBigint:          "NUMBER",
	BuildInMySQLDatatypeDecimal:         "DECIMAL",
	BuildInMySQLDatatypeDouble:          "BINARY_DOUBLE",
	BuildInMySQLDatatypeDoublePrecision: "BINARY_DOUBLE",
	BuildInMySQLDatatypeFloat:           "BINARY_FLOAT",
	BuildInMySQLDatatypeInt:             "NUMBER",
	BuildInMySQLDatatypeInteger:         "NUMBER",
	BuildInMySQLDatatypeMediumint:       "NUMBER",
	BuildInMySQLDatatypeNumeric:         "NUMBER",
	BuildInMySQLDatatypeReal:            "BINARY_FLOAT",
	BuildInMySQLDatatypeBit:             "RAW",
	BuildInMySQLDatatypeDate:            "DATE",
	BuildInMySQLDatatypeDatetime:        "DATE",
	BuildInMySQLDatatypeTimestamp:       "TIMESTAMP",
	BuildInMySQLDatatypeTime:            "DATE",
	BuildInMySQLDatatypeYear:            "NUMBER",
	BuildInMySQLDatatypeBlob:            "BLOB",
	BuildInMySQLDatatypeChar:            "CHAR",
	BuildInMySQLDatatypeLongBlob:        "BLOB",
	BuildInMySQLDatatypeLongText:        "CLOB",
	BuildInMySQLDatatypeMediumBlob:      "BLOB",
	BuildInMySQLDatatypeMediumText:      "CLOB",
	BuildInMySQLDatatypeText:            "CLOB",
	BuildInMySQLDatatypeTinyBlob:        "BLOB",
	BuildInMySQLDatatypeTinyText:        "VARCHAR2",
	BuildInMySQLDatatypeVarchar:         "VARCHAR2",
	BuildInMySQLDatatypeBinary:          "RAW",
	BuildInMySQLDatatypeVarbinary:       "RAW",
}

/*
	Database Object Name
*/
// Oracle 字符集
const (
	BuildInOracleCharacterSetAL32UTF8 = "AL32UTF8"
	BuildInOracleCharacterSetZHS16GBK = "ZHS16GBK"
)

// Oracle 数据类型名字
const (
	BuildInOracleTableTypeHeap      = "HEAP"
	BuildInOracleTableTypeClustered = "CLUSTERED"
	BuildInOracleTableTypeTemporary = "TEMPORARY"
	BuildInOracleTableTypePartition = "PARTITIONED"

	BuildInOracleConstraintTypePrimary = "P"
	BuildInOracleConstraintTypeCheck   = "C"
	BuildInOracleConstraintTypeUnique  = "U"
	BuildInOracleConstraintTypeForeign = "F"

	BuildInOracleIndexTypeNormal              = "NORMAL"
	BuildInOracleIndexTypeFunctionBasedNormal = "FUNCTION-BASED NORMAL"
	BuildInOracleIndexTypeBitmap              = "BITMAP"
	BuildInOracleIndexTypeFunctionBasedBitmap = "FUNCTION-BASED BITMAP"
	BuildInOracleIndexTypeDomain              = "DOMAIN"

	BuildInOracleViewTypeView = "VIEW"

	BuildInOracleCodeTypeMaterializedView   = "MATERIALIZED VIEW"
	BuildInOracleCodeTypeCluster            = "CLUSTER"
	BuildInOracleCodeTypeConsumerGroup      = "CONSUMER GROUP"
	BuildInOracleCodeTypeContext            = "CONTEXT"
	BuildInOracleCodeTypeDestination        = "DESTINATION"
	BuildInOracleCodeTypeDirectory          = "DIRECTORY"
	BuildInOracleCodeTypeEdition            = "EDITION"
	BuildInOracleCodeTypeEvaluationContext  = "EVALUATION CONTEXT"
	BuildInOracleCodeTypeFunction           = "FUNCTION"
	BuildInOracleCodeTypeIndexPartition     = "INDEX PARTITION"
	BuildInOracleCodeTypeIndexType          = "INDEXTYPE"
	BuildInOracleCodeTypeJavaClass          = "JAVA CLASS"
	BuildInOracleCodeTypeJavaData           = "JAVA DATA"
	BuildInOracleCodeTypeJavaResource       = "JAVA RESOURCE"
	BuildInOracleCodeTypeJavaSource         = "JAVA SOURCE"
	BuildInOracleCodeTypeJob                = "JOB"
	BuildInOracleCodeTypeJobClass           = "JOB CLASS"
	BuildInOracleCodeTypeLibrary            = "LIBRARY"
	BuildInOracleCodeTypeLob                = "LOB"
	BuildInOracleCodeTypeLobPartition       = "LOB PARTITION"
	BuildInOracleCodeTypeLockdownProfile    = "LOCKDOWN PROFILE"
	BuildInOracleCodeTypeOperator           = "OPERATOR"
	BuildInOracleCodeTypePackage            = "PACKAGE"
	BuildInOracleCodeTypePackageBody        = "PACKAGE BODY"
	BuildInOracleCodeTypeProcedure          = "PROCEDURE"
	BuildInOracleCodeTypeProgram            = "PROGRAM"
	BuildInOracleCodeTypeQueue              = "QUEUE"
	BuildInOracleCodeTypeResourcePlan       = "RESOURCE PLAN"
	BuildInOracleCodeTypeRule               = "RULE"
	BuildInOracleCodeTypeRuleSet            = "RULE SET"
	BuildInOracleCodeTypeSchedule           = "SCHEDULE"
	BuildInOracleCodeTypeSchedulerGroup     = "SCHEDULER GROUP"
	BuildInOracleCodeTypeSequence           = "SEQUENCE"
	BuildInOracleCodeTypeTrigger            = "TRIGGER"
	BuildInOracleCodeTypeType               = "TYPE"
	BuildInOracleCodeTypeTypeBody           = "TYPE BODY"
	BuildInOracleCodeTypeUndefined          = "UNDEFINED"
	BuildInOracleCodeTypeUnifiedAuditPolicy = "UNIFIED AUDIT POLICY"
	BuildInOracleCodeTypeWindow             = "WINDOW"
	BuildInOracleCodeTypeXMLSchema          = "XML SCHEMA"
	BuildInOracleCodeTypeDatabaseLink       = "DATABASE LINK"
	BuildInOracleCodeTypeSynonym            = "SYNONYM"

	BuildInOraclePartitionTypeRange     = "RANGE"
	BuildInOraclePartitionTypeHash      = "HASH"
	BuildInOraclePartitionTypeList      = "LIST"
	BuildInOraclePartitionTypeInterval  = "INTERVAL"
	BuildInOraclePartitionTypeReference = "REFERENCE"
	BuildInOraclePartitionTypeComposite = "COMPOSITE"
	BuildInOraclePartitionTypeSystem    = "SYSTEM"

	BuildInOraclePartitionTypeRangeHash  = "RANGE-HASH"
	BuildInOraclePartitionTypeRangeList  = "RANGE-LIST"
	BuildInOraclePartitionTypeRangeRange = "RANGE-RANGE"
	BuildInOraclePartitionTypeListList   = "LIST-LIST"
	BuildInOraclePartitionTypeListHash   = "LIST-HASH"
	BuildInOraclePartitionTypeListRange  = "LIST-RANGE"

	BuildInOracleTemporaryTypeSession     = "SYS$SESSION"
	BuildInOracleTemporaryTypeTransaction = "SYS$TRANSACTION"
)
