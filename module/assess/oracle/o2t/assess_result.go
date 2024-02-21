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
	"fmt"
	"strings"

	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/assess/oracle/public"
)

/*
Oracle Database Overview
*/
func AssessOracleDBOverview(oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible, reportName, reportUser string) (*public.ReportOverview, public.ReportSummary, error) {

	dbName, platformID, platformName, err := oracle.GetOracleDBName()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	dbVersion, err := oracle.GetOracleSoftVersion()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	globalName, err := oracle.GetOracleGlobalName()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}
	dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err := oracle.GetOracleParameters()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	instanceRes, err := oracle.GetOracleInstance()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	dataSize, err := oracle.GetOracleDataTotal()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	hostCPU, err := oracle.GetOracleNumCPU()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}
	memorySize, err := oracle.GetOracleMemoryGB()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	if val, ok := objAssessCompsMap[common.StringUPPER(characterSet)]; ok {
		if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
			assessComp += 1
		}
		if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
			assessInComp += 1
		}
		if strings.EqualFold(val.IsCompatible, common.AssessYesConvertible) {
			assessConvert += 1
		}
		if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
			assessInConvert += 1
		}
	} else {
		assessInComp += 1
		assessConvert += 1
	}

	return &public.ReportOverview{
			ReportName:        reportName,
			ReportUser:        reportUser,
			HostName:          instanceRes[0]["HOST_NAME"],
			PlatformName:      fmt.Sprintf("%s/%s", platformName, platformID),
			DBName:            dbName,
			DBVersion:         dbVersion,
			GlobalDBName:      globalName,
			ClusterDB:         clusterDatabase,
			ClusterDBInstance: CLusterDatabaseInstance,
			InstanceName:      instanceRes[0]["INSTANCE_NAME"],
			InstanceNumber:    instanceRes[0]["INSTANCE_NUMBER"],
			ThreadNumber:      instanceRes[0]["THREAD_NUMBER"],
			BlockSize:         dbBlockSize,
			TotalUsedSize:     dataSize,
			HostCPUS:          hostCPU,
			HostMem:           memorySize,
			CharacterSet:      characterSet},
		public.ReportSummary{
			AssessType:    common.AssessTypeDatabaseOverview,
			AssessName:    common.AssessNameDBOverview,
			AssessTotal:   1,
			Compatible:    assessComp,
			Incompatible:  assessInComp,
			Convertible:   assessConvert,
			InConvertible: assessInConvert,
		}, nil
}

/*
Oracle Database Compatible
*/
func AssessOracleSchemaTableTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaTableTypeCompatibles, public.ReportSummary, error) {
	tableTypeCounts, err := oracle.GetOracleSchemaTableTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}
	if len(tableTypeCounts) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableTypeCompatibles

	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range tableTypeCounts {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["TABLE_TYPE"])]; ok {
			listData = append(listData, public.SchemaTableTypeCompatibles{
				Schema:        ow["SCHEMA_NAME"],
				TableType:     ow["TABLE_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				ObjectSize:    ow["OBJECT_SIZE"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaTableTypeCompatibles{
				Schema:        ow["SCHEMA_NAME"],
				TableType:     ow["TABLE_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				ObjectSize:    ow["OBJECT_SIZE"],
				IsCompatible:  common.AssessNoCompatible,
				IsConvertible: common.AssessNoConvertible,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameTableTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaColumnTypeCompatible(schemaName []string, oracle *oracle.Oracle, buildinDatatypeMap map[string]meta.BuildinDatatypeRule) ([]public.SchemaColumnTypeCompatibles, public.ReportSummary, error) {

	columnInfo, err := oracle.GetOracleSchemaColumnTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(columnInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaColumnTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range columnInfo {
		if val, ok := buildinDatatypeMap[common.StringUPPER(ow["DATA_TYPE"])]; ok {
			listData = append(listData, public.SchemaColumnTypeCompatibles{
				Schema:        ow["OWNER"],
				ColumnType:    ow["DATA_TYPE"],
				ObjectCounts:  ow["COUNT"],
				MaxDataLength: ow["MAX_DATA_LENGTH"],
				ColumnTypeMap: val.DatatypeNameT,
				IsEquivalent:  common.AssessYesEquivalent,
			})

			assessComp += 1
			assessConvert += 1
		} else {
			listData = append(listData, public.SchemaColumnTypeCompatibles{
				Schema:        ow["OWNER"],
				ColumnType:    ow["DATA_TYPE"],
				ObjectCounts:  ow["COUNT"],
				MaxDataLength: ow["MAX_DATA_LENGTH"],
				ColumnTypeMap: val.DatatypeNameT,
				IsEquivalent:  common.AssessNoEquivalent,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameColumnTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaConstraintTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaConstraintTypeCompatibles, public.ReportSummary, error) {

	columnInfo, err := oracle.GetOracleSchemaConstraintTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(columnInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaConstraintTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range columnInfo {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["CONSTRAINT_TYPE"])]; ok {
			listData = append(listData, public.SchemaConstraintTypeCompatibles{
				Schema:         ow["OWNER"],
				ConstraintType: ow["CONSTRAINT_TYPE"],
				ObjectCounts:   ow["COUNT"],
				IsCompatible:   val.IsCompatible,
				IsConvertible:  val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaConstraintTypeCompatibles{
				Schema:         ow["OWNER"],
				ConstraintType: ow["CONSTRAINT_TYPE"],
				ObjectCounts:   ow["COUNT"],
				IsCompatible:   common.AssessNoCompatible,
				IsConvertible:  common.AssessNoConvertible,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameConstraintTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaIndexTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaIndexTypeCompatibles, public.ReportSummary, error) {

	columnInfo, err := oracle.GetOracleSchemaIndexTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(columnInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaIndexTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range columnInfo {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["INDEX_TYPE"])]; ok {
			listData = append(listData, public.SchemaIndexTypeCompatibles{
				Schema:        ow["TABLE_OWNER"],
				IndexType:     ow["INDEX_TYPE"],
				ObjectCounts:  ow["COUNT"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaIndexTypeCompatibles{
				Schema:        ow["TABLE_OWNER"],
				IndexType:     ow["INDEX_TYPE"],
				ObjectCounts:  ow["COUNT"],
				IsCompatible:  common.AssessNoCompatible,
				IsConvertible: common.AssessNoConvertible,
			})
			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameIndexTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaDefaultValue(schemaName []string, oracle *oracle.Oracle, defaultValueMap map[string]meta.BuildinGlobalDefaultval) ([]public.SchemaDefaultValueCompatibles, public.ReportSummary, error) {

	dataDefaults, err := oracle.GetOracleSchemaColumnDataDefaultCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(dataDefaults) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaDefaultValueCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range dataDefaults {
		if val, ok := defaultValueMap[common.StringUPPER(ow["DATA_DEFAULT"])]; ok {
			listData = append(listData, public.SchemaDefaultValueCompatibles{
				Schema:             ow["OWNER"],
				ColumnDefaultValue: ow["DATA_DEFAULT"],
				ObjectCounts:       ow["COUNTS"],
				DefaultValueMap:    val.DefaultValueT,
				IsCompatible:       common.AssessYesCompatible,
				IsConvertible:      common.AssessYesConvertible,
			})

			assessComp += 1
			assessConvert += 1

		} else {
			listData = append(listData, public.SchemaDefaultValueCompatibles{
				Schema:             ow["OWNER"],
				ColumnDefaultValue: ow["DATA_DEFAULT"],
				ObjectCounts:       ow["COUNTS"],
				DefaultValueMap:    val.DefaultValueT,
				IsCompatible:       common.AssessNoCompatible,
				IsConvertible:      common.AssessNoConvertible,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameDefaultValueCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaViewTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaViewTypeCompatibles, public.ReportSummary, error) {
	viewTypes, err := oracle.GetOracleSchemaViewTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(viewTypes) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaViewTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range viewTypes {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["VIEW_TYPE"])]; ok {
			listData = append(listData, public.SchemaViewTypeCompatibles{
				Schema:        ow["OWNER"],
				ViewType:      ow["VIEW_TYPE"],
				ViewTypeOwner: ow["VIEW_TYPE_OWNER"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaViewTypeCompatibles{
				Schema:        ow["OWNER"],
				ViewType:      ow["VIEW_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  common.AssessNoCompatible,
				IsConvertible: common.AssessNoConvertible,
			})
			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameViewTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaObjectTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaObjectTypeCompatibles, public.ReportSummary, error) {

	codeInfo, err := oracle.GetOracleSchemaObjectTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(codeInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaObjectTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range codeInfo {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["OBJECT_TYPE"])]; ok {
			listData = append(listData, public.SchemaObjectTypeCompatibles{
				Schema:        ow["OWNER"],
				ObjectType:    ow["OBJECT_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaObjectTypeCompatibles{
				Schema:        ow["OWNER"],
				ObjectType:    ow["OBJECT_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  common.AssessNoCompatible,
				IsConvertible: common.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameObjectTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaPartitionTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaPartitionTypeCompatibles, public.ReportSummary, error) {

	partitionInfo, err := oracle.GetOracleSchemaPartitionTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(partitionInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaPartitionTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range partitionInfo {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["PARTITIONING_TYPE"])]; ok {
			listData = append(listData, public.SchemaPartitionTypeCompatibles{
				Schema:        ow["OWNER"],
				PartitionType: ow["PARTITIONING_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaPartitionTypeCompatibles{
				Schema:        ow["OWNER"],
				PartitionType: ow["PARTITIONING_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  common.AssessNoCompatible,
				IsConvertible: common.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNamePartitionTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaSubPartitionTypeCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaSubPartitionTypeCompatibles, public.ReportSummary, error) {

	partitionInfo, err := oracle.GetOracleSchemaSubPartitionTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(partitionInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaSubPartitionTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range partitionInfo {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["SUBPARTITIONING_TYPE"])]; ok {
			listData = append(listData, public.SchemaSubPartitionTypeCompatibles{
				Schema:           ow["OWNER"],
				SubPartitionType: ow["SUBPARTITIONING_TYPE"],
				ObjectCounts:     ow["COUNTS"],
				IsCompatible:     val.IsCompatible,
				IsConvertible:    val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaSubPartitionTypeCompatibles{
				Schema:           ow["OWNER"],
				SubPartitionType: ow["SUBPARTITIONING_TYPE"],
				ObjectCounts:     ow["COUNTS"],
				IsCompatible:     common.AssessNoCompatible,
				IsConvertible:    common.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameSubPartitionTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaTemporaryTableCompatible(schemaName []string, oracle *oracle.Oracle, objAssessCompsMap map[string]meta.BuildinObjectCompatible) ([]public.SchemaTemporaryTableTypeCompatibles, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTemporaryTableTypeCounts(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTemporaryTableTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		if val, ok := objAssessCompsMap[common.StringUPPER(ow["TEMP_TYPE"])]; ok {
			listData = append(listData, public.SchemaTemporaryTableTypeCompatibles{
				Schema:             ow["OWNER"],
				TemporaryTableType: ow["TEMP_TYPE"],
				ObjectCounts:       ow["COUNTS"],
				IsCompatible:       val.IsCompatible,
				IsConvertible:      val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, common.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, common.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, common.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, public.SchemaTemporaryTableTypeCompatibles{
				Schema:             ow["OWNER"],
				TemporaryTableType: ow["TEMP_TYPE"],
				ObjectCounts:       ow["COUNTS"],
				IsCompatible:       common.AssessNoCompatible,
				IsConvertible:      common.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCompatible,
		AssessName:    common.AssessNameTemporaryTableTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

/*
Oracle Database Check
*/
func AssessOraclePartitionTableCountsCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaPartitionTableCountsCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaPartitionTableCountsOver1024(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaPartitionTableCountsCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaPartitionTableCountsCheck{
			Schema:          ow["OWNER"],
			TableName:       ow["TABLE_NAME"],
			PartitionCounts: ow["PARTITION_COUNT"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNamePartitionTableCountsCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleTableRowLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableRowLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableRowLengthOver6M(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableRowLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableRowLengthCheck{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			AvgRowLength: ow["AVG_ROW_LEN"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameTableRowLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleTableIndexRowLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableIndexRowLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableIndexLengthOver3072(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableIndexRowLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableIndexRowLengthCheck{
			Schema:       ow["INDEX_OWNER"],
			TableName:    ow["TABLE_NAME"],
			IndexName:    ow["INDEX_NAME"],
			ColumnLength: ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameIndexRowLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleTableColumnCountsCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableColumnCountsCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableColumnCountsOver512(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableColumnCountsCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableColumnCountsCheck{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			ColumnCounts: ow["COUNT_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameTableColumnCountsCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleTableIndexCountsCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableIndexCountsCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableIndexCountsOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableIndexCountsCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableIndexCountsCheck{
			Schema:      ow["TABLE_OWNER"],
			TableName:   ow["TABLE_NAME"],
			IndexCounts: ow["COUNT_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameTableIndexCountsCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleUsernameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.UsernameLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleUsernameLengthOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.UsernameLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.UsernameLengthCheck{
			Schema:        ow["USERNAME"],
			AccountStatus: ow["ACCOUNT_STATUS"],
			Created:       ow["CREATED"],
			Length:        ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameUsernameLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleTableNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableNameLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableNameLengthOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableNameLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableNameLengthCheck{
			Schema:    ow["OWNER"],
			TableName: ow["TABLE_NAME"],
			Length:    ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameTableNameLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleColumnNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableColumnNameLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableColumnNameLengthOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableColumnNameLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableColumnNameLengthCheck{
			Schema:     ow["OWNER"],
			TableName:  ow["TABLE_NAME"],
			ColumnName: ow["COLUMN_NAME"],
			Length:     ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameColumnNameLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleIndexNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableIndexNameLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableIndexNameLengthOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableIndexNameLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0
	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableIndexNameLengthCheck{
			Schema:    ow["INDEX_OWNER"],
			TableName: ow["TABLE_NAME"],
			IndexName: ow["INDEX_NAME"],
			Length:    ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameIndexNameLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleViewNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaViewNameLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableViewNameLengthOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaViewNameLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaViewNameLengthCheck{
			Schema:   ow["OWNER"],
			ViewName: ow["VIEW_NAME"],
			ReadOnly: ow["READ_ONLY"],
			Length:   ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameViewNameLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSequenceNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaSequenceNameLengthCheck, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableSequenceNameLengthOver64(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaSequenceNameLengthCheck
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaSequenceNameLengthCheck{
			Schema:       ow["SEQUENCE_OWNER"],
			SequenceName: ow["SEQUENCE_NAME"],
			OrderFlag:    ow["ORDER_FLAG"],
			Length:       ow["LENGTH_OVER"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeCheck,
		AssessName:    common.AssessNameSequenceNameLengthCheck,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

/*
Oracle Database Reference
*/
func AssessOracleSchemaOverview(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableSizeData, public.ReportSummary, error) {

	overview, err := oracle.GetOracleSchemaOverview(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableSizeData
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, public.SchemaTableSizeData{
			Schema:        ow["SCHEMA"],
			TableSize:     ow["TABLE"],
			IndexSize:     ow["INDEX"],
			LobTableSize:  ow["LOBTABLE"],
			LobIndexSize:  ow["LOBINDEX"],
			AllTablesRows: ow["ROWCOUNT"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaDataSizeRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleMaxActiveSessionCount(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaActiveSession, public.ReportSummary, error) {

	listActiveSession, err := oracle.GetOracleMaxActiveSessionCount()
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(listActiveSession) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaActiveSession
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range listActiveSession {
		listData = append(listData, public.SchemaActiveSession{
			Rownum:         ow["ROWNUM"],
			DBID:           ow["DBID"],
			InstanceNumber: ow["INSTANCE_NUMBER"],
			SampleID:       ow["SAMPLE_ID"],
			SampleTime:     ow["SAMPLE_TIME"],
			SessionCounts:  ow["SESSION_COUNT"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaActiveSessionRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaTableRowsTOP(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableRowsTOP, public.ReportSummary, error) {

	overview, err := oracle.GetOracleSchemaTableRowsTOP(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}
	if len(overview) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableRowsTOP
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, public.SchemaTableRowsTOP{
			Schema:    ow["SCHEMA"],
			TableName: ow["SEGMENT_NAME"],
			TableType: ow["SEGMENT_TYPE"],
			TableSize: ow["TABLE_SIZE"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaTableRowsTopRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaCodeOverview(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaCodeObject, public.ReportSummary, error) {

	overview, err := oracle.GetOracleSchemaCodeObject(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaCodeObject
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, public.SchemaCodeObject{
			Schema:     ow["OWNER"],
			ObjectName: ow["NAME"],
			ObjectType: ow["TYPE"],
			Lines:      ow["LINES"],
		})

	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaCodeObjectRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaSynonymOverview(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaSynonymObject, public.ReportSummary, error) {
	overview, err := oracle.GetOracleSchemaCodeObject(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaSynonymObject
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, public.SchemaSynonymObject{
			Schema:      ow["OWNER"],
			SynonymName: ow["SYNONYM_NAME"],
			TableOwner:  ow["TABLE_OWNER"],
			TableName:   ow["TABLE_NAME"],
		})

	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaSynonymObjectRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaMaterializedViewOverview(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaMaterializedViewObject, public.ReportSummary, error) {
	overview, err := oracle.GetOracleSchemaMaterializedViewObject(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaMaterializedViewObject
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, public.SchemaMaterializedViewObject{
			Schema:            ow["OWNER"],
			MviewName:         ow["MVIEW_NAME"],
			RewriteCapability: ow["REWRITE_CAPABILITY"],
			RefreshMode:       ow["REFRESH_MODE"],
			RefreshMethod:     ow["REFRESH_METHOD"],
			FastRefreshable:   ow["FAST_REFRESHABLE"],
		})

	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaMaterializedViewRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaTableAvgRowLengthTOP(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableAvgRowLengthTOP, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableAvgRowLengthTOP(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableAvgRowLengthTOP
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableAvgRowLengthTOP{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			AvgRowLength: ow["AVG_ROW_LEN"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaTableAvgRowLengthTopRelated,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessOracleSchemaTableNumberTypeEqual0(schemaName []string, oracle *oracle.Oracle) ([]public.SchemaTableNumberTypeEqual0, public.ReportSummary, error) {

	synonymInfo, err := oracle.GetOracleSchemaTableNumberTypeEqual0(schemaName)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, public.ReportSummary{}, nil
	}

	var listData []public.SchemaTableNumberTypeEqual0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, public.SchemaTableNumberTypeEqual0{
			Schema:        ow["OWNER"],
			TableName:     ow["TABLE_NAME"],
			ColumnName:    ow["COLUMN_NAME"],
			DataPrecision: ow["DATA_PRECISION"],
			DataScale:     ow["DATA_SCALE"],
		})
	}

	return listData, public.ReportSummary{
		AssessType:    common.AssessTypeObjectTypeRelated,
		AssessName:    common.AssessNameSchemaTableNumberTypeEqual0,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}
