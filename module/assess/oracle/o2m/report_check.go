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
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/assess/oracle/public"
)

/*
Oracle Database Check
*/
func GetAssessDatabaseCheckResult(schemaName []string, oracle *oracle.Oracle) (*public.ReportCheck, *public.ReportSummary, error) {
	var (
		ListSchemaPartitionTableCountsCheck  []public.SchemaPartitionTableCountsCheck
		ListSchemaTableRowLengthCheck        []public.SchemaTableRowLengthCheck
		ListSchemaTableIndexRowLengthCheck   []public.SchemaTableIndexRowLengthCheck
		ListSchemaTableColumnCountsCheck     []public.SchemaTableColumnCountsCheck
		ListSchemaIndexCountsCheck           []public.SchemaTableIndexCountsCheck
		ListUsernameLengthCheck              []public.UsernameLengthCheck
		ListSchemaTableNameLengthCheck       []public.SchemaTableNameLengthCheck
		ListSchemaTableColumnNameLengthCheck []public.SchemaTableColumnNameLengthCheck
		ListSchemaTableIndexNameLengthCheck  []public.SchemaTableIndexNameLengthCheck
		ListSchemaViewNameLengthCheck        []public.SchemaViewNameLengthCheck
		ListSchemaSequenceNameLengthCheck    []public.SchemaSequenceNameLengthCheck
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

	return &public.ReportCheck{
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
		}, &public.ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: incompatibleS,
		}, nil
}
