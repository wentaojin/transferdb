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
	"context"
	"encoding/json"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
)

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

/*
Oracle Database Compatible
*/
func GetAssessDatabaseCompatibleResult(ctx context.Context, metaDB *meta.Meta, oracle *oracle.Oracle, schemaName []string) (*ReportCompatible, *ReportSummary, error) {
	var (
		ListSchemaTableTypeCompatibles          []SchemaTableTypeCompatibles
		ListSchemaColumnTypeCompatibles         []SchemaColumnTypeCompatibles
		ListSchemaConstraintTypeCompatibles     []SchemaConstraintTypeCompatibles
		ListSchemaIndexTypeCompatibles          []SchemaIndexTypeCompatibles
		ListSchemaDefaultValueCompatibles       []SchemaDefaultValueCompatibles
		ListSchemaViewTypeCompatibles           []SchemaViewTypeCompatibles
		ListSchemaObjectTypeCompatibles         []SchemaObjectTypeCompatibles
		ListSchemaPartitionTypeCompatibles      []SchemaPartitionTypeCompatibles
		ListSchemaSubPartitionTypeCompatibles   []SchemaSubPartitionTypeCompatibles
		ListSchemaTemporaryTableTypeCompatibles []SchemaTemporaryTableTypeCompatibles
	)

	// 获取自定义兼容性内容
	compatibles, err := meta.NewBuildinObjectCompatibleModel(metaDB).BatchQueryObjAssessCompatible(ctx, &meta.BuildinObjectCompatible{
		DBTypeS: common.DatabaseTypeOracle,
		DBTypeT: common.DatabaseTypeMySQL,
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
		DBTypeS: common.DatabaseTypeOracle,
		DBTypeT: common.DatabaseTypeMySQL,
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
		DBTypeS: common.DatabaseTypeOracle,
		DBTypeT: common.DatabaseTypeMySQL})
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

	return &ReportCompatible{
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
		}, &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		}, nil
}
