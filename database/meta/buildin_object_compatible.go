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
package meta

import (
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type BuildinObjectCompatible struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS       string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT       string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'目标数据库类型'" json:"db_type_t"`
	ObjectNameS   string `gorm:"type:varchar(300);index:idx_dbtype_st_obj,unique;comment:'源数据库对象名'" json:"object_name_s"`
	IsCompatible  string `gorm:"type:char(1);comment:'对象是否可兼容'" json:"is_compatible"`
	IsConvertible string `gorm:"type:char(1);comment:'对象是否可改造'" json:"is_convertible"`
	*BaseModel
}

func NewBuildinObjectCompatibleModel(m *Meta) *BuildinObjectCompatible {
	return &BuildinObjectCompatible{BaseModel: &BaseModel{
		Meta: m,
	}}
}

func (rw *BuildinObjectCompatible) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [BuildinObjectCompatible] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *BuildinObjectCompatible) BatchQueryObjAssessCompatible(ctx context.Context, detailS *BuildinObjectCompatible) ([]BuildinObjectCompatible, error) {
	var objAssessComp []BuildinObjectCompatible
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return objAssessComp, err
	}

	if err := rw.DB(ctx).Where("UPPER(db_type_s) = ? AND UPPER(db_type_t) = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT)).Find(&objAssessComp).Error; err != nil {
		return objAssessComp, fmt.Errorf("batch query table [%s] record failed: %v", table, err)
	}
	return objAssessComp, nil
}

func (rw *BuildinObjectCompatible) InitO2MBuildinObjectCompatible(ctx context.Context) error {
	var buildinObjComps []*BuildinObjectCompatible
	/*
		O2M Build-IN Compatible Rule
	*/
	// oracle character set
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCharacterSetAL32UTF8,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCharacterSetZHS16GBK,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	// oracle table type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleTableTypeHeap,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleTableTypeClustered,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleTableTypeTemporary,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleTableTypePartition,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	// oracle constraint type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleConstraintTypePrimary,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleConstraintTypeUnique,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleConstraintTypeCheck,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleConstraintTypeForeign,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	// oracle index type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleIndexTypeNormal,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleIndexTypeFunctionBasedNormal,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleIndexTypeBitmap,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleIndexTypeFunctionBasedBitmap,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleIndexTypeDomain,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	// oracle view type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleViewTypeView,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessYesConvertible,
	})
	// oracle code type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeMaterializedView,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeCluster,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeConsumerGroup,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeContext,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeDestination,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeDirectory,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeEdition,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeEvaluationContext,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeFunction,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeIndexPartition,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeIndexType,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeJavaClass,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeJavaData,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeJavaResource,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeJavaSource,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeJob,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeJobClass,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeLibrary,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeLob,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeLobPartition,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeLockdownProfile,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeOperator,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypePackage,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypePackageBody,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeProcedure,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeProgram,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeQueue,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeResourcePlan,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeRule,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeRuleSet,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeSchedule,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeSchedulerGroup,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeSequence,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeTrigger,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeType,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeTypeBody,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeUndefined,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeUnifiedAuditPolicy,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeWindow,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeXMLSchema,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeDatabaseLink,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleCodeTypeSynonym,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})

	// oracle partitions/subpartition type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeRange,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeList,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeHash,
		IsCompatible:  common.AssessYesCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeSystem,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeReference,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeReference,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeComposite,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeInterval,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeRangeHash,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeRangeList,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeRangeRange,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeListHash,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeListHash,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeListList,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOraclePartitionTypeListRange,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})

	// oracle temporary type
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleTemporaryTypeSession,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinObjectCompatible{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		ObjectNameS:   common.BuildInOracleTemporaryTypeTransaction,
		IsCompatible:  common.AssessNoCompatible,
		IsConvertible: common.AssessNoConvertible,
	})

	return rw.DB(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).CreateInBatches(buildinObjComps, 20).Error
}
