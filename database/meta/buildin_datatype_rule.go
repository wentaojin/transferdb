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

type BuildinDatatypeRule struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS       string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT       string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'目标数据库类型'" json:"db_type_t"`
	DatatypeNameS string `gorm:"type:varchar(300);index:idx_dbtype_st_obj,unique;comment:'源数据类型名字'" json:"datatype_name_s"`
	DatatypeNameT string `gorm:"type:varchar(300);comment:'目标数据类型名字'" json:"datatype_name_t"`
	*BaseModel
}

func NewBuildinDatatypeRuleModel(m *Meta) *BuildinDatatypeRule {
	return &BuildinDatatypeRule{BaseModel: &BaseModel{
		Meta: m,
	}}
}

func (rw *BuildinDatatypeRule) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [BuildinDatatypeRule] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *BuildinDatatypeRule) BatchQueryBuildinDatatype(ctx context.Context, detailS *BuildinDatatypeRule) ([]BuildinDatatypeRule, error) {
	var objAssessComp []BuildinDatatypeRule

	tableName, err := rw.ParseSchemaTable()
	if err != nil {
		return nil, err
	}
	if err := rw.DB(ctx).Where("UPPER(db_type_s) = ? AND UPPER(db_type_t) = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT)).Find(&objAssessComp).Error; err != nil {
		return objAssessComp, fmt.Errorf("batch query table [%s] record failed: %v", tableName, err)
	}
	return objAssessComp, nil
}

func (rw *BuildinDatatypeRule) InitO2MBuildinDatatypeRule(ctx context.Context) error {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		O2M Build-IN Compatible Rule
	*/
	// oracle column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeNumber,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeNumber],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeBfile,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeBfile],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeChar,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeChar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeCharacter,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeCharacter],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeClob,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeClob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeBlob,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeDate,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeDecimal,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeDec,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeDec],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeDoublePrecision,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeFloat,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeInteger,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeInt,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeLong,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeLong],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeLongRAW,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeLongRAW],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeBinaryFloat,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeBinaryFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeBinaryDouble,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeBinaryDouble],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeNchar,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeNchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeNcharVarying,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeNcharVarying],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeNclob,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeNclob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeNumeric,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeNvarchar2,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeNvarchar2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeRaw,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeRaw],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeReal,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeRowid,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeRowid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeUrowid,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeUrowid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeSmallint,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeSmallint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeVarchar2,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeVarchar2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeVarchar,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeVarchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeXmltype,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeXmltype],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp0,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp1,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp2,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp3,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp4,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp5,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp6,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp7,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp8,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestamp9,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestamp9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth0,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth1,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth2,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth3,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth4,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth5,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth6,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth7,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth8,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeIntervalYearMonth9,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeIntervalYearMonth9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone0,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone1,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone2,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone3,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone4,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone5,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone6,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone7,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone8,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithTimeZone9,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithTimeZone9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone0,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone1,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone2,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone3,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone4,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone5,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone6,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone7,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone8,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       common.TaskDBOracle,
		DBTypeT:       common.TaskDBMySQL,
		DatatypeNameS: common.BuildInOracleDatatypeTimestampWithLocalTimeZone9,
		DatatypeNameT: common.BuildInOracleO2MDatatypeNameMap[common.BuildInOracleDatatypeTimestampWithLocalTimeZone9],
	})

	return rw.DB(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "db_type_s"},
			{Name: "db_type_t"},
			{Name: "datatype_name_s"},
		},
		DoNothing: true,
	}).CreateInBatches(buildinDataTypeR, 20).Error
}
