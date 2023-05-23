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
package public

import "encoding/json"

type ReportCompatible struct {
	ListSchemaTableTypeCompatibles          []SchemaTableTypeCompatibles          `json:"list_schema_table_type_compatibles"`
	ListSchemaColumnTypeCompatibles         []SchemaColumnTypeCompatibles         `json:"list_schema_column_type_compatibles"`
	ListSchemaConstraintTypeCompatibles     []SchemaConstraintTypeCompatibles     `json:"list_schema_constraint_type_compatibles"`
	ListSchemaIndexTypeCompatibles          []SchemaIndexTypeCompatibles          `json:"list_schema_index_type_compatibles"`
	ListSchemaDefaultValueCompatibles       []SchemaDefaultValueCompatibles       `json:"list_schema_default_value_compatibles"`
	ListSchemaViewTypeCompatibles           []SchemaViewTypeCompatibles           `json:"list_schema_view_type_compatibles"`
	ListSchemaObjectTypeCompatibles         []SchemaObjectTypeCompatibles         `json:"list_schema_object_type_compatibles"`
	ListSchemaPartitionTypeCompatibles      []SchemaPartitionTypeCompatibles      `json:"list_schema_partition_type_compatibles"`
	ListSchemaSubPartitionTypeCompatibles   []SchemaSubPartitionTypeCompatibles   `json:"list_schema_sub_partition_type_compatibles"`
	ListSchemaTemporaryTableTypeCompatibles []SchemaTemporaryTableTypeCompatibles `json:"list_schema_temporary_table_type_compatibles"`
}

func (sc *ReportCompatible) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaTableTypeCompatibles struct {
	Schema        string `json:"schema"`
	TableType     string `json:"table_type"`
	ObjectCounts  string `json:"object_counts"`
	ObjectSize    string `json:"object_size"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaTableTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaColumnTypeCompatibles struct {
	Schema        string `json:"schema"`
	ColumnType    string `json:"column_type"`
	ObjectCounts  string `json:"object_counts"`
	MaxDataLength string `json:"max_data_length"`
	ColumnTypeMap string `json:"column_type_map"`
	IsEquivalent  string `json:"is_equivalent"`
}

func (sc *SchemaColumnTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaConstraintTypeCompatibles struct {
	Schema         string `json:"schema"`
	ConstraintType string `json:"constraint_type"`
	ObjectCounts   string `json:"object_counts"`
	IsCompatible   string `json:"is_compatible"`
	IsConvertible  string `json:"is_convertible"`
}

func (sc *SchemaConstraintTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaIndexTypeCompatibles struct {
	Schema        string `json:"schema"`
	IndexType     string `json:"index_type"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaIndexTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaDefaultValueCompatibles struct {
	Schema             string `json:"schema"`
	ColumnDefaultValue string `json:"column_default_value"`
	ObjectCounts       string `json:"object_counts"`
	DefaultValueMap    string `json:"default_value_map"`
	IsCompatible       string `json:"is_compatible"`
	IsConvertible      string `json:"is_convertible"`
}

func (sc *SchemaDefaultValueCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaViewTypeCompatibles struct {
	Schema        string `json:"schema"`
	ViewType      string `json:"view_type"`
	ViewTypeOwner string `json:"view_type_owner"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaViewTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaObjectTypeCompatibles struct {
	Schema        string `json:"schema"`
	ObjectType    string `json:"object_type"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaObjectTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaPartitionTypeCompatibles struct {
	Schema        string `json:"schema"`
	PartitionType string `json:"partition_type"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaPartitionTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaSubPartitionTypeCompatibles struct {
	Schema           string `json:"schema"`
	SubPartitionType string `json:"sub_partition_type"`
	ObjectCounts     string `json:"object_counts"`
	IsCompatible     string `json:"is_compatible"`
	IsConvertible    string `json:"is_convertible"`
}

func (sc *SchemaSubPartitionTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaTemporaryTableTypeCompatibles struct {
	Schema             string `json:"schema"`
	TemporaryTableType string `json:"temporary_table_type"`
	ObjectCounts       string `json:"object_counts"`
	IsCompatible       string `json:"is_compatible"`
	IsConvertible      string `json:"is_convertible"`
}

func (sc *SchemaTemporaryTableTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}
