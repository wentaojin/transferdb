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
package o2m

import "encoding/json"

type ReportOverview struct {
	ReportName        string `json:"report_name"`
	ReportUser        string `json:"report_user"`
	HostName          string `json:"host_name"`
	PlatformName      string `json:"platform_name"`
	DBName            string `json:"db_name"`
	GlobalDBName      string `json:"global_db_name"`
	ClusterDB         string `json:"cluster_db"`
	ClusterDBInstance string `json:"cluster_db_instance"`
	InstanceName      string `json:"instance_name"`
	InstanceNumber    string `json:"instance_number"`
	ThreadNumber      string `json:"thread_number"`
	BlockSize         string `json:"block_size"`
	TotalUsedSize     string `json:"total_used_size"`
	HostCPUS          string `json:"host_cpus"`
	HostMem           string `json:"host_mem"`
	CharacterSet      string `json:"character_set"`
}

type ReportSchema struct {
	ListSchemaActiveSession               []ListSchemaActiveSession               `json:"list_schema_active_session"`
	ListSchemaTableSizeData               []ListSchemaTableSizeData               `json:"list_schema_table_size_data"`
	ListSchemaTableRowsTOP                []ListSchemaTableRowsTOP                `json:"list_schema_table_rows_top"`
	ListSchemaTableObjectCounts           []ListSchemaTableObjectCounts           `json:"list_schema_table_object_counts"`
	ListSchemaTablePartitionType          []ListSchemaTablePartitionType          `json:"list_schema_table_partition_type"`
	ListSchemaTableColumnTypeAndMaxLength []ListSchemaTableColumnTypeAndMaxLength `json:"list_schema_table_column_type_and_max_length"`
	ListSchemaTableAvgRowLength           []ListSchemaTableAvgRowLength           `json:"list_schema_table_avg_row_length"`
	ListSchemaTemporaryTableCounts        []ListSchemaTemporaryTableCounts        `json:"list_schema_temporary_table_counts"`
}

type ReportType struct {
	ListSchemaIndexType      []ListSchemaIndexType      `json:"list_schema_index_type"`
	ListSchemaConstraintType []ListSchemaConstraintType `json:"list_schema_constraint_type"`
	ListSchemaCodeType       []ListSchemaCodeType       `json:"list_schema_code_type"`
	ListSchemaSynonymType    []ListSchemaSynonymType    `json:"list_schema_synonym_type"`
}

type ReportCheck struct {
	ListSchemaPartitionTableCountsCheck  []ListSchemaPartitionTableCountsCheck  `json:"list_schema_partition_table_counts_check"`
	ListSchemaTableRowLengthCheck        []ListSchemaTableRowLengthCheck        `json:"list_schema_table_row_length_check"`
	ListSchemaTableIndexRowLengthCheck   []ListSchemaTableIndexRowLengthCheck   `json:"list_schema_table_index_row_length_check"`
	ListSchemaTableColumnCountsCheck     []ListSchemaTableAndIndexCountsCheck   `json:"list_schema_table_column_counts_check"`
	ListSchemaIndexCountsCheck           []ListSchemaTableAndIndexCountsCheck   `json:"list_schema_index_counts_check"`
	ListSchemaTableNumberTypeCheck       []ListSchemaTableNumberTypeCheck       `json:"list_schema_table_number_type_check"`
	ListUsernameLengthCheck              []ListUsernameLengthCheck              `json:"list_username_length_check"`
	ListSchemaTableNameLengthCheck       []ListSchemaTableNameLengthCheck       `json:"list_schema_table_name_length_check"`
	ListSchemaTableColumnNameLengthCheck []ListSchemaTableColumnNameLengthCheck `json:"list_schema_table_column_name_length_check"`
	ListSchemaTableIndexNameLengthCheck  []ListSchemaTableIndexNameLengthCheck  `json:"list_schema_table_index_name_length_check"`
	ListSchemaViewNameLengthCheck        []ListSchemaViewNameLengthCheck        `json:"list_schema_view_name_length_check"`
	ListSchemaSequenceNameLengthCheck    []ListSchemaSequenceNameLengthCheck    `json:"list_schema_sequence_name_length_check"`
}

type ListSchemaPartitionTableCountsCheck struct {
	Schema          string `json:"schema"`
	TableName       string `json:"table_name"`
	PartitionCounts string `json:"partition_counts"`
}

func (ro *ListSchemaPartitionTableCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableRowLengthCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	AvgRowLength string `json:"avg_row_length"`
}

func (ro *ListSchemaTableRowLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableIndexRowLengthCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	IndexName    string `json:"index_name"`
	ColumnLength string `json:"column_length"`
}

func (ro *ListSchemaTableIndexRowLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableAndIndexCountsCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	Counts    string `json:"counts"`
}

func (ro *ListSchemaTableAndIndexCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableNumberTypeCheck struct {
	Schema        string `json:"schema"`
	TableName     string `json:"table_name"`
	ColumnName    string `json:"column_name"`
	DataPrecision string `json:"data_precision"`
	DataScale     string `json:"data_scale"`
}

func (ro *ListSchemaTableNumberTypeCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListUsernameLengthCheck struct {
	Schema        string `json:"schema"`
	AccountStatus string `json:"account_status"`
	Created       string `json:"created"`
}

func (ro *ListUsernameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableNameLengthCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
}

func (ro *ListSchemaTableNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableColumnNameLengthCheck struct {
	Schema     string `json:"schema"`
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
}

func (ro *ListSchemaTableColumnNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableIndexNameLengthCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	IndexName string `json:"index_name"`
}

func (ro *ListSchemaTableIndexNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaViewNameLengthCheck struct {
	Schema   string `json:"schema"`
	ViewName string `json:"view_name"`
	ReadOnly string `json:"read_only"`
}

func (ro *ListSchemaViewNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaSequenceNameLengthCheck struct {
	Schema       string `json:"schema"`
	SequenceName string `json:"sequence_name"`
	OrderFlag    string `json:"order_flag"`
}

func (ro *ListSchemaSequenceNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaIndexType struct {
	Schema    string `json:"schema"`
	IndexType string `json:"index_type"`
	Counts    string `json:"counts"`
}

func (ro *ListSchemaIndexType) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaConstraintType struct {
	Schema         string `json:"schema"`
	ConstraintType string `json:"constraint_type"`
	Counts         string `json:"counts"`
}

func (ro *ListSchemaConstraintType) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaCodeType struct {
	Schema   string `json:"schema"`
	CodeName string `json:"code_name"`
	CodeType string `json:"code_type"`
	Counts   string `json:"counts"`
}

func (ro *ListSchemaCodeType) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaSynonymType struct {
	Schema     string `json:"schema"`
	TableOwner string `json:"table_owner"`
	Counts     string `json:"counts"`
}

func (ro *ListSchemaSynonymType) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaActiveSession struct {
	Rownum         string `json:"rownum"`
	DBID           string `json:"dbid"`
	InstanceNumber string `json:"instance_number"`
	SampleID       string `json:"sample_id"`
	SampleTime     string `json:"sample_time"`
	SessionCounts  string `json:"session_counts"`
}

func (ro *ListSchemaActiveSession) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableSizeData struct {
	Schema        string `json:"schema"`
	TableSize     string `json:"table_size"`
	IndexSize     string `json:"index_size"`
	LobTableSize  string `json:"lob_table_size"`
	LobIndexSize  string `json:"lob_index_size"`
	AllTablesRows string `json:"all_tables_rows"`
}

func (ro *ListSchemaTableSizeData) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableRowsTOP struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	TableType string `json:"table_type"`
	TableSize string `json:"table_size"`
}

func (ro *ListSchemaTableRowsTOP) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableObjectCounts struct {
	Schema     string `json:"schema"`
	ObjectType string `json:"object_type"`
	Counts     string `json:"counts"`
}

func (ro *ListSchemaTableObjectCounts) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTablePartitionType struct {
	Schema           string `json:"schema"`
	TableName        string `json:"table_name"`
	PartitionType    string `json:"partition_type"`
	SubPartitionType string `json:"sub_partition_type"`
}

func (ro *ListSchemaTablePartitionType) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableColumnTypeAndMaxLength struct {
	Schema        string `json:"schema"`
	DataType      string `json:"data_type"`
	Counts        string `json:"counts"`
	MaxDataLength string `json:"max_data_length"`
}

func (ro *ListSchemaTableColumnTypeAndMaxLength) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTableAvgRowLength struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	AvgRowLength string `json:"avg_row_length"`
}

func (ro *ListSchemaTableAvgRowLength) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ListSchemaTemporaryTableCounts struct {
	Schema string `json:"schema"`
	Counts string `json:"counts"`
}

func (ro *ListSchemaTemporaryTableCounts) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

func (ro *ReportOverview) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

func (rs *ReportSchema) String() string {
	jsonStr, _ := json.Marshal(rs)
	return string(jsonStr)
}

func (rt *ReportType) String() string {
	jsonStr, _ := json.Marshal(rt)
	return string(jsonStr)
}

func (rc *ReportCheck) String() string {
	jsonStr, _ := json.Marshal(rc)
	return string(jsonStr)
}
