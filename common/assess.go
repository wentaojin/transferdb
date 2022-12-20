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

// Assess Status
const (
	AssessYesCompatible  = "Y"
	AssessNoCompatible   = "N"
	AssessYesConvertible = "Y"
	AssessNoConvertible  = "N"
	AssessYesEquivalent  = "Y"
	AssessNoEquivalent   = "N"
)

// Assess Type
const (
	AssessTypeDatabaseOverview     = "DATABASE_OVERVIEW"
	AssessTypeObjectTypeCompatible = "OBJECT_TYPE_COMPATIBLE"
	AssessTypeObjectTypeCheck      = "OBJECT_TYPE_CHECK"
	AssessTypeObjectTypeRelated    = "OBJECT_TYPE_RELATED"
)

// Assess Name
const (
	AssessNameDBOverview = "DB_OVERVIEW"

	AssessNameTableTypeCompatible          = "TABLE_TYPE_COMPATIBLE"
	AssessNameColumnTypeCompatible         = "COLUMN_TYPE_COMPATIBLE"
	AssessNameConstraintTypeCompatible     = "CONSTRAINT_TYPE_COMPATIBLE"
	AssessNameIndexTypeCompatible          = "INDEX_TYPE_COMPATIBLE"
	AssessNameDefaultValueCompatible       = "DEFAULT_VALUE_COMPATIBLE"
	AssessNameViewTypeCompatible           = "VIEW_TYPE_COMPATIBLE"
	AssessNameObjectTypeCompatible         = "OBJECT_TYPE_COMPATIBLE"
	AssessNamePartitionTypeCompatible      = "PARTITION_TYPE_COMPATIBLE"
	AssessNameSubPartitionTypeCompatible   = "SUBPARTITION_TYPE_COMPATIBLE"
	AssessNameTemporaryTableTypeCompatible = "TEMPORARY_TABLE_TYPE_COMPATIBLE"

	AssessNamePartitionTableCountsCheck = "PARTITION_TABLE_COUNTS_CHECK"
	AssessNameTableColumnCountsCheck    = "TABLE_COLUMN_COUNTS_CHECK"
	AssessNameTableIndexCountsCheck     = "TABLE_INDEX_COUNTS_CHECK"

	AssessNameTableRowLengthCheck     = "TABLE_ROW_LENGTH_CHECK"
	AssessNameIndexRowLengthCheck     = "INDEX_ROW_LENGTH_CHECK"
	AssessNameUsernameLengthCheck     = "USERNAME_LENGTH_CHECK"
	AssessNameTableNameLengthCheck    = "TABLE_NAME_LENGTH_CHECK"
	AssessNameColumnNameLengthCheck   = "COLUMN_NAME_LENGTH_CHECK"
	AssessNameIndexNameLengthCheck    = "INDEX_NAME_LENGTH_CHECK"
	AssessNameViewNameLengthCheck     = "VIEW_NAME_LENGTH_CHECK"
	AssessNameSequenceNameLengthCheck = "SEQUENCE_NAME_LENGTH_CHECK"

	AssessNameSchemaDataSizeRelated             = "SCHEMA_DATA_SIZE_RELATED"
	AssessNameSchemaActiveSessionRelated        = "SCHEMA_ACTIVE_SESSION_RELATED"
	AssessNameSchemaTableRowsTopRelated         = "SCHEMA_TABLE_ROWS_TOP_RELATED"
	AssessNameSchemaCodeObjectRelated           = "SCHEMA_CODE_OBJECT_RELATED"
	AssessNameSchemaSynonymObjectRelated        = "SCHEMA_SYNONYM_OBJECT_RELATED"
	AssessNameSchemaMaterializedViewRelated     = "SCHEMA_MATERIALIZED_VIEW_OBJECT_RELATED"
	AssessNameSchemaTableAvgRowLengthTopRelated = "SCHEMA_TABLE_AVG_ROW_LENGTH_TOP_RELATED"
	AssessNameSchemaTableNumberTypeEqual0       = "SCHEMA_TABLE_NUMBER_TYPE_EQUAL0"
)
