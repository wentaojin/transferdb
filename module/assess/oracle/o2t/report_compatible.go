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
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/assess/oracle/public"
)

/*
Oracle Database Compatible
*/
func GetAssessDatabaseCompatibleResult(ctx context.Context, metaDB *meta.Meta, oracle *oracle.Oracle, schemaName []string, dbTypeS, dbTypeT string) (*public.ReportCompatible, *public.ReportSummary, error) {
	var (
		ListSchemaTableTypeCompatibles          []public.SchemaTableTypeCompatibles
		ListSchemaColumnTypeCompatibles         []public.SchemaColumnTypeCompatibles
		ListSchemaConstraintTypeCompatibles     []public.SchemaConstraintTypeCompatibles
		ListSchemaIndexTypeCompatibles          []public.SchemaIndexTypeCompatibles
		ListSchemaDefaultValueCompatibles       []public.SchemaDefaultValueCompatibles
		ListSchemaViewTypeCompatibles           []public.SchemaViewTypeCompatibles
		ListSchemaObjectTypeCompatibles         []public.SchemaObjectTypeCompatibles
		ListSchemaPartitionTypeCompatibles      []public.SchemaPartitionTypeCompatibles
		ListSchemaSubPartitionTypeCompatibles   []public.SchemaSubPartitionTypeCompatibles
		ListSchemaTemporaryTableTypeCompatibles []public.SchemaTemporaryTableTypeCompatibles
	)

	// 获取自定义兼容性内容
	compatibles, err := meta.NewBuildinObjectCompatibleModel(metaDB).BatchQueryObjAssessCompatible(ctx, &meta.BuildinObjectCompatible{
		DBTypeS: dbTypeS,
		DBTypeT: dbTypeT,
	})
	if err != nil {
		return nil, nil, err
	}
	objAssessCompsMap := make(map[string]meta.BuildinObjectCompatible)
	for _, c := range compatibles {
		objAssessCompsMap[common.StringUPPER(c.ObjectNameS)] = c
	}

	// 获取自定义数据类型
	buildDatatypeRules, err := meta.NewBuildinDatatypeRuleModel(metaDB).BatchQueryBuildinDatatype(ctx, &meta.BuildinDatatypeRule{
		DBTypeS: dbTypeS,
		DBTypeT: dbTypeT,
	})
	if err != nil {
		return nil, nil, err
	}
	buildDatatypeMap := make(map[string]meta.BuildinDatatypeRule)
	for _, d := range buildDatatypeRules {
		buildDatatypeMap[common.StringUPPER(d.DatatypeNameS)] = d
	}

	// 获取自定义默认值内容
	defaultValues, err := meta.NewBuildinGlobalDefaultvalModel(metaDB).DetailGlobalDefaultVal(ctx, &meta.BuildinGlobalDefaultval{
		DBTypeS: dbTypeS,
		DBTypeT: dbTypeT})
	if err != nil {
		return nil, nil, err
	}
	defaultValuesMap := make(map[string]meta.BuildinGlobalDefaultval)
	for _, d := range defaultValues {
		defaultValuesMap[common.StringUPPER(d.DefaultValueS)] = d
	}

	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0

	ListSchemaTableTypeCompatibles, tableSummary, err := AssessOracleSchemaTableTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}

	assessTotal += tableSummary.AssessTotal
	compatibleS += tableSummary.Compatible
	incompatibleS += tableSummary.Incompatible
	convertibleS += tableSummary.Convertible
	inconvertibleS += tableSummary.InConvertible

	ListSchemaColumnTypeCompatibles, columnSummary, err := AssessOracleSchemaColumnTypeCompatible(schemaName, oracle, buildDatatypeMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += columnSummary.AssessTotal
	compatibleS += columnSummary.Compatible
	incompatibleS += columnSummary.Incompatible
	convertibleS += columnSummary.Convertible
	inconvertibleS += columnSummary.InConvertible

	ListSchemaConstraintTypeCompatibles, constraintSummary, err := AssessOracleSchemaConstraintTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += constraintSummary.AssessTotal
	compatibleS += constraintSummary.Compatible
	incompatibleS += constraintSummary.Incompatible
	convertibleS += constraintSummary.Convertible
	inconvertibleS += constraintSummary.InConvertible

	ListSchemaIndexTypeCompatibles, indexSummary, err := AssessOracleSchemaIndexTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}

	assessTotal += indexSummary.AssessTotal
	compatibleS += indexSummary.Compatible
	incompatibleS += indexSummary.Incompatible
	convertibleS += indexSummary.Convertible
	inconvertibleS += indexSummary.InConvertible

	ListSchemaDefaultValueCompatibles, defaultValSummary, err := AssessOracleSchemaDefaultValue(schemaName, oracle, defaultValuesMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += defaultValSummary.AssessTotal
	compatibleS += defaultValSummary.Compatible
	incompatibleS += defaultValSummary.Incompatible
	convertibleS += defaultValSummary.Convertible
	inconvertibleS += defaultValSummary.InConvertible

	ListSchemaViewTypeCompatibles, viewSummary, err := AssessOracleSchemaViewTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += viewSummary.AssessTotal
	compatibleS += viewSummary.Compatible
	incompatibleS += viewSummary.Incompatible
	convertibleS += viewSummary.Convertible
	inconvertibleS += viewSummary.InConvertible

	ListSchemaObjectTypeCompatibles, codeSummary, err := AssessOracleSchemaObjectTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += codeSummary.AssessTotal
	compatibleS += codeSummary.Compatible
	incompatibleS += codeSummary.Incompatible
	convertibleS += codeSummary.Convertible
	inconvertibleS += codeSummary.InConvertible

	ListSchemaPartitionTypeCompatibles, partitionSummary, err := AssessOracleSchemaPartitionTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += partitionSummary.AssessTotal
	compatibleS += partitionSummary.Compatible
	incompatibleS += partitionSummary.Incompatible
	convertibleS += partitionSummary.Convertible
	inconvertibleS += partitionSummary.InConvertible

	ListSchemaSubPartitionTypeCompatibles, subPartitionSummary, err := AssessOracleSchemaSubPartitionTypeCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += subPartitionSummary.AssessTotal
	compatibleS += subPartitionSummary.Compatible
	incompatibleS += subPartitionSummary.Incompatible
	convertibleS += subPartitionSummary.Convertible
	inconvertibleS += subPartitionSummary.InConvertible

	ListSchemaTemporaryTableTypeCompatibles, tempSummary, err := AssessOracleSchemaTemporaryTableCompatible(schemaName, oracle, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tempSummary.AssessTotal
	compatibleS += tempSummary.Compatible
	incompatibleS += tempSummary.Incompatible
	convertibleS += tempSummary.Convertible
	inconvertibleS += tempSummary.InConvertible

	return &public.ReportCompatible{
			ListSchemaTableTypeCompatibles:          ListSchemaTableTypeCompatibles,
			ListSchemaColumnTypeCompatibles:         ListSchemaColumnTypeCompatibles,
			ListSchemaConstraintTypeCompatibles:     ListSchemaConstraintTypeCompatibles,
			ListSchemaIndexTypeCompatibles:          ListSchemaIndexTypeCompatibles,
			ListSchemaDefaultValueCompatibles:       ListSchemaDefaultValueCompatibles,
			ListSchemaViewTypeCompatibles:           ListSchemaViewTypeCompatibles,
			ListSchemaObjectTypeCompatibles:         ListSchemaObjectTypeCompatibles,
			ListSchemaPartitionTypeCompatibles:      ListSchemaPartitionTypeCompatibles,
			ListSchemaSubPartitionTypeCompatibles:   ListSchemaSubPartitionTypeCompatibles,
			ListSchemaTemporaryTableTypeCompatibles: ListSchemaTemporaryTableTypeCompatibles,
		}, &public.ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		}, nil
}
