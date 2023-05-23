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

type ReportCheck struct {
	ListSchemaPartitionTableCountsCheck  []SchemaPartitionTableCountsCheck  `json:"list_schema_partition_table_counts_check"`
	ListSchemaTableRowLengthCheck        []SchemaTableRowLengthCheck        `json:"list_schema_table_row_length_check"`
	ListSchemaTableIndexRowLengthCheck   []SchemaTableIndexRowLengthCheck   `json:"list_schema_table_index_row_length_check"`
	ListSchemaTableColumnCountsCheck     []SchemaTableColumnCountsCheck     `json:"list_schema_table_column_counts_check"`
	ListSchemaIndexCountsCheck           []SchemaTableIndexCountsCheck      `json:"list_schema_index_counts_check"`
	ListUsernameLengthCheck              []UsernameLengthCheck              `json:"list_username_length_check"`
	ListSchemaTableNameLengthCheck       []SchemaTableNameLengthCheck       `json:"list_schema_table_name_length_check"`
	ListSchemaTableColumnNameLengthCheck []SchemaTableColumnNameLengthCheck `json:"list_schema_table_column_name_length_check"`
	ListSchemaTableIndexNameLengthCheck  []SchemaTableIndexNameLengthCheck  `json:"list_schema_table_index_name_length_check"`
	ListSchemaViewNameLengthCheck        []SchemaViewNameLengthCheck        `json:"list_schema_view_name_length_check"`
	ListSchemaSequenceNameLengthCheck    []SchemaSequenceNameLengthCheck    `json:"list_schema_sequence_name_length_check"`
}

func (rc *ReportCheck) String() string {
	jsonStr, _ := json.Marshal(rc)
	return string(jsonStr)
}

type SchemaPartitionTableCountsCheck struct {
	Schema          string `json:"schema"`
	TableName       string `json:"table_name"`
	PartitionCounts string `json:"partition_counts"`
}

func (ro *SchemaPartitionTableCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableRowLengthCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	AvgRowLength string `json:"avg_row_length"`
}

func (ro *SchemaTableRowLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableIndexRowLengthCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	IndexName    string `json:"index_name"`
	ColumnLength string `json:"column_length"`
}

func (ro *SchemaTableIndexRowLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableColumnCountsCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	ColumnCounts string `json:"column_counts"`
}

func (ro *SchemaTableColumnCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableIndexCountsCheck struct {
	Schema      string `json:"schema"`
	TableName   string `json:"table_name"`
	IndexCounts string `json:"index_counts"`
}

func (ro *SchemaTableIndexCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type UsernameLengthCheck struct {
	Schema        string `json:"schema"`
	AccountStatus string `json:"account_status"`
	Created       string `json:"created"`
	Length        string `json:"length"`
}

func (ro *UsernameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableNameLengthCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	Length    string `json:"length"`
}

func (ro *SchemaTableNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableColumnNameLengthCheck struct {
	Schema     string `json:"schema"`
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	Length     string `json:"length"`
}

func (ro *SchemaTableColumnNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableIndexNameLengthCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	IndexName string `json:"index_name"`
	Length    string `json:"length"`
}

func (ro *SchemaTableIndexNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaViewNameLengthCheck struct {
	Schema   string `json:"schema"`
	ViewName string `json:"view_name"`
	ReadOnly string `json:"read_only"`
	Length   string `json:"length"`
}

func (ro *SchemaViewNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaSequenceNameLengthCheck struct {
	Schema       string `json:"schema"`
	SequenceName string `json:"sequence_name"`
	OrderFlag    string `json:"order_flag"`
	Length       string `json:"length"`
}

func (ro *SchemaSequenceNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}
