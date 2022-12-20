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

import (
	"encoding/json"
	"github.com/wentaojin/transferdb/database/oracle"
)

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

/*
Oracle Database Check
*/
func GetAssessDatabaseCheckResult(schemaName []string, oracle *oracle.Oracle) (*ReportCheck, *ReportSummary, error) {
	var (
		ListSchemaPartitionTableCountsCheck  []SchemaPartitionTableCountsCheck
		ListSchemaTableRowLengthCheck        []SchemaTableRowLengthCheck
		ListSchemaTableIndexRowLengthCheck   []SchemaTableIndexRowLengthCheck
		ListSchemaTableColumnCountsCheck     []SchemaTableColumnCountsCheck
		ListSchemaIndexCountsCheck           []SchemaTableIndexCountsCheck
		ListUsernameLengthCheck              []UsernameLengthCheck
		ListSchemaTableNameLengthCheck       []SchemaTableNameLengthCheck
		ListSchemaTableColumnNameLengthCheck []SchemaTableColumnNameLengthCheck
		ListSchemaTableIndexNameLengthCheck  []SchemaTableIndexNameLengthCheck
		ListSchemaViewNameLengthCheck        []SchemaViewNameLengthCheck
		ListSchemaSequenceNameLengthCheck    []SchemaSequenceNameLengthCheck
	)

	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0

	ListSchemaPartitionTableCountsCheck, partitionSummary, err := AssessOraclePartitionTableCountsCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += partitionSummary.AssessTotal
	compatibleS += partitionSummary.Compatible
	incompatibleS += partitionSummary.Incompatible
	convertibleS += partitionSummary.Convertible
	inconvertibleS += partitionSummary.InConvertible

	ListSchemaTableRowLengthCheck, tableSummary, err := AssessOracleTableRowLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableSummary.AssessTotal
	compatibleS += tableSummary.Compatible
	incompatibleS += tableSummary.Incompatible
	convertibleS += tableSummary.Convertible
	inconvertibleS += tableSummary.InConvertible

	ListSchemaTableIndexRowLengthCheck, indexSummary, err := AssessOracleTableIndexRowLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += indexSummary.AssessTotal
	compatibleS += indexSummary.Compatible
	incompatibleS += indexSummary.Incompatible
	convertibleS += indexSummary.Convertible
	inconvertibleS += indexSummary.InConvertible

	ListSchemaTableColumnCountsCheck, columnSummary, err := AssessOracleTableColumnCountsCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += columnSummary.AssessTotal
	compatibleS += columnSummary.Compatible
	incompatibleS += columnSummary.Incompatible
	convertibleS += columnSummary.Convertible
	inconvertibleS += columnSummary.InConvertible

	ListSchemaIndexCountsCheck, indexCSummary, err := AssessOracleTableIndexCountsCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += indexCSummary.AssessTotal
	compatibleS += indexCSummary.Compatible
	incompatibleS += indexCSummary.Incompatible
	convertibleS += indexCSummary.Convertible
	inconvertibleS += indexCSummary.InConvertible

	ListUsernameLengthCheck, usernameLSummary, err := AssessOracleUsernameLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += usernameLSummary.AssessTotal
	compatibleS += usernameLSummary.Compatible
	incompatibleS += usernameLSummary.Incompatible
	convertibleS += usernameLSummary.Convertible
	inconvertibleS += usernameLSummary.InConvertible

	ListSchemaTableNameLengthCheck, tableLSummary, err := AssessOracleTableNameLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableLSummary.AssessTotal
	compatibleS += tableLSummary.Compatible
	incompatibleS += tableLSummary.Incompatible
	convertibleS += tableLSummary.Convertible
	inconvertibleS += tableLSummary.InConvertible

	ListSchemaTableColumnNameLengthCheck, columnLSummary, err := AssessOracleColumnNameLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += columnLSummary.AssessTotal
	compatibleS += columnLSummary.Compatible
	incompatibleS += columnLSummary.Incompatible
	convertibleS += columnLSummary.Convertible
	inconvertibleS += columnLSummary.InConvertible

	ListSchemaTableIndexNameLengthCheck, indexLSummary, err := AssessOracleIndexNameLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += indexLSummary.AssessTotal
	compatibleS += indexLSummary.Compatible
	incompatibleS += indexLSummary.Incompatible
	convertibleS += indexLSummary.Convertible
	inconvertibleS += indexLSummary.InConvertible

	ListSchemaViewNameLengthCheck, viewSummary, err := AssessOracleViewNameLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += viewSummary.AssessTotal
	compatibleS += viewSummary.Compatible
	incompatibleS += viewSummary.Incompatible
	convertibleS += viewSummary.Convertible
	inconvertibleS += viewSummary.InConvertible

	ListSchemaSequenceNameLengthCheck, seqSummary, err := AssessOracleSequenceNameLengthCheck(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += seqSummary.AssessTotal
	compatibleS += seqSummary.Compatible
	incompatibleS += seqSummary.Incompatible
	convertibleS += seqSummary.Convertible
	inconvertibleS += seqSummary.InConvertible

	return &ReportCheck{
			ListSchemaPartitionTableCountsCheck:  ListSchemaPartitionTableCountsCheck,
			ListSchemaTableRowLengthCheck:        ListSchemaTableRowLengthCheck,
			ListSchemaTableIndexRowLengthCheck:   ListSchemaTableIndexRowLengthCheck,
			ListSchemaTableColumnCountsCheck:     ListSchemaTableColumnCountsCheck,
			ListSchemaIndexCountsCheck:           ListSchemaIndexCountsCheck,
			ListUsernameLengthCheck:              ListUsernameLengthCheck,
			ListSchemaTableNameLengthCheck:       ListSchemaTableNameLengthCheck,
			ListSchemaTableColumnNameLengthCheck: ListSchemaTableColumnNameLengthCheck,
			ListSchemaTableIndexNameLengthCheck:  ListSchemaTableIndexNameLengthCheck,
			ListSchemaViewNameLengthCheck:        ListSchemaViewNameLengthCheck,
			ListSchemaSequenceNameLengthCheck:    ListSchemaSequenceNameLengthCheck,
		}, &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: incompatibleS,
		}, nil
}
