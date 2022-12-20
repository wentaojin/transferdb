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

type ReportRelated struct {
	ListSchemaActiveSession          []SchemaActiveSession          `json:"list_schema_active_session"`
	ListSchemaTableSizeData          []SchemaTableSizeData          `json:"list_schema_table_size_data"`
	ListSchemaTableRowsTOP           []SchemaTableRowsTOP           `json:"list_schema_table_rows_top"`
	ListSchemaCodeObject             []SchemaCodeObject             `json:"list_schema_code_object"`
	ListSchemaSynonymObject          []SchemaSynonymObject          `json:"list_schema_synonym_object"`
	ListSchemaMaterializedViewObject []SchemaMaterializedViewObject `json:"list_schema_materialized_view_object"`
	ListSchemaTableAvgRowLengthTOP   []SchemaTableAvgRowLengthTOP   `json:"list_schema_table_avg_row_length_top"`
	ListSchemaTableNumberTypeEqual0  []SchemaTableNumberTypeEqual0  `json:"list_schema_table_number_type_equal_0"`
}

func (rr *ReportRelated) String() string {
	jsonStr, _ := json.Marshal(rr)
	return string(jsonStr)
}

type SchemaActiveSession struct {
	Rownum         string `json:"rownum"`
	DBID           string `json:"dbid"`
	InstanceNumber string `json:"instance_number"`
	SampleID       string `json:"sample_id"`
	SampleTime     string `json:"sample_time"`
	SessionCounts  string `json:"session_counts"`
}

func (ro *SchemaActiveSession) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableSizeData struct {
	Schema        string `json:"schema"`
	TableSize     string `json:"table_size"`
	IndexSize     string `json:"index_size"`
	LobTableSize  string `json:"lob_table_size"`
	LobIndexSize  string `json:"lob_index_size"`
	AllTablesRows string `json:"all_tables_rows"`
}

func (ro *SchemaTableSizeData) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableRowsTOP struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	TableType string `json:"table_type"`
	TableSize string `json:"table_size"`
}

func (ro *SchemaTableRowsTOP) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaCodeObject struct {
	Schema     string `json:"schema"`
	ObjectName string `json:"object_name"`
	ObjectType string `json:"object_type"`
	Lines      string `json:"lines"`
}

func (ro *SchemaCodeObject) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaSynonymObject struct {
	Schema      string `json:"schema"`
	SynonymName string `json:"synonym_name"`
	TableOwner  string `json:"table_owner"`
	TableName   string `json:"table_name"`
}

func (ro *SchemaSynonymObject) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaMaterializedViewObject struct {
	Schema            string `json:"schema"`
	MviewName         string `json:"mview_name"`
	RewriteCapability string `json:"rewrite_capability"`
	RefreshMode       string `json:"refresh_mode"`
	RefreshMethod     string `json:"refresh_method"`
	FastRefreshable   string `json:"fast_refreshable"`
}

func (ro *SchemaMaterializedViewObject) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableAvgRowLengthTOP struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	AvgRowLength string `json:"avg_row_length"`
}

func (ro *SchemaTableAvgRowLengthTOP) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableNumberTypeEqual0 struct {
	Schema        string `json:"schema"`
	TableName     string `json:"table_name"`
	ColumnName    string `json:"column_name"`
	DataPrecision string `json:"data_precision"`
	DataScale     string `json:"data_scale"`
}

func (ro *SchemaTableNumberTypeEqual0) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

/*
Oracle Database Related
*/
func GetAssessDatabaseRelatedResult(schemaName []string, oracle *oracle.Oracle) (*ReportRelated, *ReportSummary, error) {
	var (
		ListSchemaActiveSession          []SchemaActiveSession
		ListSchemaTableSizeData          []SchemaTableSizeData
		ListSchemaTableRowsTOP           []SchemaTableRowsTOP
		ListSchemaCodeObject             []SchemaCodeObject
		ListSchemaSynonymObject          []SchemaSynonymObject
		ListSchemaMaterializedViewObject []SchemaMaterializedViewObject
		ListSchemaTableAvgRowLengthTOP   []SchemaTableAvgRowLengthTOP
		ListSchemaTableNumberTypeEqual0  []SchemaTableNumberTypeEqual0
	)

	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0

	ListSchemaActiveSession, sessionSummary, err := AssessOracleMaxActiveSessionCount(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += sessionSummary.AssessTotal
	compatibleS += sessionSummary.Compatible
	incompatibleS += sessionSummary.Incompatible
	convertibleS += sessionSummary.Convertible
	inconvertibleS += sessionSummary.InConvertible

	ListSchemaTableSizeData, overviewSummary, err := AssessOracleSchemaOverview(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += overviewSummary.AssessTotal
	compatibleS += overviewSummary.Compatible
	incompatibleS += overviewSummary.Incompatible
	convertibleS += overviewSummary.Convertible
	inconvertibleS += overviewSummary.InConvertible

	ListSchemaTableRowsTOP, tableSummary, err := AssessOracleSchemaTableRowsTOP(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableSummary.AssessTotal
	compatibleS += tableSummary.Compatible
	incompatibleS += tableSummary.Incompatible
	convertibleS += tableSummary.Convertible
	inconvertibleS += tableSummary.InConvertible

	ListSchemaCodeObject, objSummary, err := AssessOracleSchemaCodeOverview(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += objSummary.AssessTotal
	compatibleS += objSummary.Compatible
	incompatibleS += objSummary.Incompatible
	convertibleS += objSummary.Convertible
	inconvertibleS += objSummary.InConvertible

	ListSchemaSynonymObject, seqSummary, err := AssessOracleSchemaSynonymOverview(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += seqSummary.AssessTotal
	compatibleS += seqSummary.Compatible
	incompatibleS += seqSummary.Incompatible
	convertibleS += seqSummary.Convertible
	inconvertibleS += seqSummary.InConvertible

	ListSchemaMaterializedViewObject, mViewSummary, err := AssessOracleSchemaMaterializedViewOverview(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += mViewSummary.AssessTotal
	compatibleS += mViewSummary.Compatible
	incompatibleS += mViewSummary.Incompatible
	convertibleS += mViewSummary.Convertible
	inconvertibleS += mViewSummary.InConvertible

	ListSchemaTableAvgRowLengthTOP, tableTSummary, err := AssessOracleSchemaTableAvgRowLengthTOP(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableTSummary.AssessTotal
	compatibleS += tableTSummary.Compatible
	incompatibleS += tableTSummary.Incompatible
	convertibleS += tableTSummary.Convertible
	inconvertibleS += tableTSummary.InConvertible

	ListSchemaTableNumberTypeEqual0, equalSummary, err := AssessOracleSchemaTableNumberTypeEqual0(schemaName, oracle)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += equalSummary.AssessTotal
	compatibleS += equalSummary.Compatible
	incompatibleS += equalSummary.Incompatible
	convertibleS += equalSummary.Convertible
	inconvertibleS += equalSummary.InConvertible

	return &ReportRelated{
			ListSchemaActiveSession:          ListSchemaActiveSession,
			ListSchemaTableSizeData:          ListSchemaTableSizeData,
			ListSchemaTableRowsTOP:           ListSchemaTableRowsTOP,
			ListSchemaCodeObject:             ListSchemaCodeObject,
			ListSchemaSynonymObject:          ListSchemaSynonymObject,
			ListSchemaMaterializedViewObject: ListSchemaMaterializedViewObject,
			ListSchemaTableAvgRowLengthTOP:   ListSchemaTableAvgRowLengthTOP,
			ListSchemaTableNumberTypeEqual0:  ListSchemaTableNumberTypeEqual0,
		}, &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		}, nil
}
