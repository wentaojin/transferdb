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
package o2t

import (
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/assess/oracle/public"
)

/*
Oracle Database Related
*/
func GetAssessDatabaseRelatedResult(schemaName []string, oracle *oracle.Oracle) (*public.ReportRelated, *public.ReportSummary, error) {
	var (
		ListSchemaActiveSession          []public.SchemaActiveSession
		ListSchemaTableSizeData          []public.SchemaTableSizeData
		ListSchemaTableRowsTOP           []public.SchemaTableRowsTOP
		ListSchemaCodeObject             []public.SchemaCodeObject
		ListSchemaSynonymObject          []public.SchemaSynonymObject
		ListSchemaMaterializedViewObject []public.SchemaMaterializedViewObject
		ListSchemaTableAvgRowLengthTOP   []public.SchemaTableAvgRowLengthTOP
		ListSchemaTableNumberTypeEqual0  []public.SchemaTableNumberTypeEqual0
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

	return &public.ReportRelated{
			ListSchemaActiveSession:          ListSchemaActiveSession,
			ListSchemaTableSizeData:          ListSchemaTableSizeData,
			ListSchemaTableRowsTOP:           ListSchemaTableRowsTOP,
			ListSchemaCodeObject:             ListSchemaCodeObject,
			ListSchemaSynonymObject:          ListSchemaSynonymObject,
			ListSchemaMaterializedViewObject: ListSchemaMaterializedViewObject,
			ListSchemaTableAvgRowLengthTOP:   ListSchemaTableAvgRowLengthTOP,
			ListSchemaTableNumberTypeEqual0:  ListSchemaTableNumberTypeEqual0,
		}, &public.ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		}, nil
}
