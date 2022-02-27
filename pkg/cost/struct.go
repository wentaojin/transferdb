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
package cost

type ReportOverview struct {
	ReportName        string
	ReportUser        string
	HostName          string
	PlatformName      string
	DBName            string
	GlobalDBName      string
	ClusterDB         string
	ClusterDBInstance string
	InstanceName      string
	InstanceNumber    string
	ThreadNumber      string
	BlockSize         string
	TotalUsedSize     string
	HostCPUS          string
	HostMem           string
	CharacterSet      string
}

type ReportSchema struct {
	ListSchemaActiveSession               []ListSchemaActiveSession
	ListSchemaTableSizeData               []ListSchemaTableSizeData
	ListSchemaTableRowsTOP                []ListSchemaTableRowsTOP
	ListSchemaTableObjectCounts           []ListSchemaTableObjectCounts
	ListSchemaTablePartitionType          []ListSchemaTablePartitionType
	ListSchemaTableColumnTypeAndMaxLength []ListSchemaTableColumnTypeAndMaxLength
	ListSchemaTableAvgRowLength           []ListSchemaTableAvgRowLength
	ListSchemaTemporaryTableCounts        []ListSchemaTemporaryTableCounts
}

type ReportType struct {
	ListSchemaIndexType      []ListSchemaIndexType
	ListSchemaConstraintType []ListSchemaConstraintType
	ListSchemaCodeType       []ListSchemaCodeType
	ListSchemaSynonymType    []ListSchemaSynonymType
}

type ReportCheck struct {
	ListSchemaPartitionTableCountsCheck  []ListSchemaPartitionTableCountsCheck
	ListSchemaTableRowLengthCheck        []ListSchemaTableRowLengthCheck
	ListSchemaTableIndexRowLengthCheck   []ListSchemaTableIndexRowLengthCheck
	ListSchemaTableColumnCountsCheck     []ListSchemaTableAndIndexCountsCheck
	ListSchemaIndexCountsCheck           []ListSchemaTableAndIndexCountsCheck
	ListSchemaTableNumberTypeCheck       []ListSchemaTableNumberTypeCheck
	ListUsernameLengthCheck              []ListUsernameLengthCheck
	ListSchemaTableNameLengthCheck       []ListSchemaTableNameLengthCheck
	ListSchemaTableColumnNameLengthCheck []ListSchemaTableColumnNameLengthCheck
	ListSchemaTableIndexNameLengthCheck  []ListSchemaTableIndexNameLengthCheck
	ListSchemaViewNameLengthCheck        []ListSchemaViewNameLengthCheck
	ListSchemaSequenceNameLengthCheck    []ListSchemaSequenceNameLengthCheck
}

type ListSchemaPartitionTableCountsCheck struct {
	Schema          string
	TableName       string
	PartitionCounts string
}

type ListSchemaTableRowLengthCheck struct {
	Schema       string
	TableName    string
	AvgRowLength string
}

type ListSchemaTableIndexRowLengthCheck struct {
	Schema       string
	TableName    string
	IndexName    string
	ColumnLength string
}

type ListSchemaTableAndIndexCountsCheck struct {
	Schema    string
	TableName string
	Counts    string
}

type ListSchemaTableNumberTypeCheck struct {
	Schema        string
	TableName     string
	ColumnName    string
	DataPrecision string
	DataScale     string
}

type ListUsernameLengthCheck struct {
	Schema        string
	AccountStatus string
	Created       string
}

type ListSchemaTableNameLengthCheck struct {
	Schema    string
	TableName string
}

type ListSchemaTableColumnNameLengthCheck struct {
	Schema     string
	TableName  string
	ColumnName string
}

type ListSchemaTableIndexNameLengthCheck struct {
	Schema    string
	TableName string
	IndexName string
}

type ListSchemaViewNameLengthCheck struct {
	Schema   string
	ViewName string
	ReadOnly string
}

type ListSchemaSequenceNameLengthCheck struct {
	Schema       string
	SequenceName string
	OrderFlag    string
}

type ListSchemaIndexType struct {
	Schema    string
	IndexType string
	Counts    string
}

type ListSchemaConstraintType struct {
	Schema         string
	ConstraintType string
	Counts         string
}

type ListSchemaCodeType struct {
	Schema   string
	CodeName string
	CodeType string
	Counts   string
}

type ListSchemaSynonymType struct {
	Schema     string
	TableOwner string
	Counts     string
}

type ListSchemaActiveSession struct {
	Rownum         string
	DBID           string
	InstanceNumber string
	SampleID       string
	SampleTime     string
	SessionCounts  string
}
type ListSchemaTableSizeData struct {
	Schema        string
	TableSize     string
	IndexSize     string
	LobTableSize  string
	LobIndexSize  string
	AllTablesRows string
}

type ListSchemaTableRowsTOP struct {
	Schema    string
	TableName string
	TableType string
	TableSize string
}

type ListSchemaTableObjectCounts struct {
	Schema     string
	ObjectType string
	Counts     string
}

type ListSchemaTablePartitionType struct {
	Schema           string
	TableName        string
	PartitionType    string
	SubPartitionType string
}

type ListSchemaTableColumnTypeAndMaxLength struct {
	Schema        string
	DataType      string
	Counts        string
	MaxDataLength string
}

type ListSchemaTableAvgRowLength struct {
	Schema       string
	TableName    string
	AvgRowLength string
}

type ListSchemaTemporaryTableCounts struct {
	Schema string
	Counts string
}
