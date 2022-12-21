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
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"strconv"
	"strings"
)

type Column struct {
	DataType                string
	CharLength              string
	CharUsed                string
	CharacterSet            string
	Collation               string
	OracleOriginDataDefault string
	MySQLOriginDataDefault  string
	ColumnInfo
}

type ColumnInfo struct {
	DataLength        string
	DataPrecision     string
	DataScale         string
	DatetimePrecision string
	NULLABLE          string
	DataDefault       string
	Comment           string
}

func OracleTableColumnMapRule(sourceSchema, sourceTable string, column Column, buildinDatatypes []meta.BuildinDatatypeRule) (string, string, error) {
	var (
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
	)

	dataLength, err := strconv.Atoi(column.DataLength)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, sourceTable, err)
	}
	dataPrecision, err := strconv.Atoi(column.DataPrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, sourceTable, err)
	}
	dataScale, err := strconv.Atoi(column.DataScale)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, sourceTable, err)
	}

	// 内置数据类型转换
	buildinDatatypeMap := make(map[string]string)
	numberDatatypeMap := make(map[string]struct{})

	for _, b := range buildinDatatypes {
		buildinDatatypeMap[common.StringUPPER(b.DatatypeNameS)] = b.DatatypeNameT

		if strings.EqualFold(common.StringUPPER(b.DatatypeNameS), common.BuildInOracleDatatypeNumber) {
			for _, c := range strings.Split(b.DatatypeNameT, "/") {
				numberDatatypeMap[common.StringUPPER(c)] = struct{}{}
			}
		}
	}

	switch common.StringUPPER(column.DataType) {
	case common.BuildInOracleDatatypeNumber:
		if _, ok := buildinDatatypeMap[common.BuildInOracleDatatypeNumber]; ok {
			switch {
			case dataScale > 0:
				switch {
				// oracle 真实数据类型 number(*) -> number(38,127)
				// number  -> number(38,127)
				// number(*,x) ->  number(38,x)
				// decimal(x,y) -> y max 30
				case dataPrecision == 38 && dataScale > 30:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, 30)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", originColumnType)
					}
				case dataPrecision == 38 && dataScale <= 30:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", originColumnType)
					}
				default:
					if dataScale <= 30 {
						originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
						if _, ok = numberDatatypeMap["DECIMAL"]; ok {
							buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
						} else {
							return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", originColumnType)
						}
					} else {
						originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
						if _, ok = numberDatatypeMap["DECIMAL"]; ok {
							buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, 30)
						} else {
							return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", originColumnType)
						}
					}
				}
			case dataScale == 0:
				switch {
				case dataPrecision >= 1 && dataPrecision < 3:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["TINYINT"]; ok {
						buildInColumnType = "TINYINT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [TINYINT]", originColumnType)
					}
				case dataPrecision >= 3 && dataPrecision < 5:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["SMALLINT"]; ok {
						buildInColumnType = "SMALLINT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [SMALLINT]", originColumnType)
					}
				case dataPrecision >= 5 && dataPrecision < 9:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["INT"]; ok {
						buildInColumnType = "INT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [INT]", originColumnType)
					}
				case dataPrecision >= 9 && dataPrecision < 19:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["BIGINT"]; ok {
						buildInColumnType = "BIGINT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [BIGINT]", originColumnType)
					}
				case dataPrecision >= 19 && dataPrecision <= 38:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d)", dataPrecision)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", originColumnType)
					}
				default:
					originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", originColumnType)
					}
				}
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeNumber)
		}
	case common.BuildInOracleDatatypeBfile:
		originColumnType = common.BuildInOracleDatatypeBfile
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeBfile]; ok {
			buildInColumnType = fmt.Sprintf("%s(255)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeBfile)
		}
	case common.BuildInOracleDatatypeChar:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeChar, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeChar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeChar)
		}
	case common.BuildInOracleDatatypeCharacter:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeCharacter, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeCharacter]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeCharacter)
		}
	case common.BuildInOracleDatatypeClob:
		originColumnType = common.BuildInOracleDatatypeClob
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeClob]; ok {
			buildInColumnType = common.StringUPPER(val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeClob)
		}
	case common.BuildInOracleDatatypeBlob:
		originColumnType = common.BuildInOracleDatatypeBlob
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeBlob]; ok {
			buildInColumnType = common.StringUPPER(val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeBlob)
		}
	case common.BuildInOracleDatatypeDate:
		originColumnType = common.BuildInOracleDatatypeDate
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeDate]; ok {
			buildInColumnType = common.StringUPPER(val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeDate)
		}
	case common.BuildInOracleDatatypeDecimal:
		originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeDecimal, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeDecimal]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d,%d)", common.StringUPPER(val), dataPrecision, dataScale)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeDecimal)
		}
	case common.BuildInOracleDatatypeDec:
		originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeDecimal, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeDec]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d,%d)", common.StringUPPER(val), dataPrecision, dataScale)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeDec)
		}
	case common.BuildInOracleDatatypeDoublePrecision:
		originColumnType = common.BuildInOracleDatatypeDoublePrecision
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeDoublePrecision]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeDoublePrecision)
		}
	case common.BuildInOracleDatatypeFloat:
		originColumnType = common.BuildInOracleDatatypeFloat
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeFloat]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeFloat)
		}
	case common.BuildInOracleDatatypeInteger:
		originColumnType = common.BuildInOracleDatatypeInteger
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeInteger]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeInteger)
		}
	case common.BuildInOracleDatatypeInt:
		originColumnType = common.BuildInOracleDatatypeInteger
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeInt]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeInt)
		}
	case common.BuildInOracleDatatypeLong:
		originColumnType = common.BuildInOracleDatatypeLong
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeLong]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeLong)
		}
	case common.BuildInOracleDatatypeLongRAW:
		originColumnType = common.BuildInOracleDatatypeLongRAW
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeLongRAW]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeLongRAW)
		}
	case common.BuildInOracleDatatypeBinaryFloat:
		originColumnType = common.BuildInOracleDatatypeBinaryFloat
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeBinaryFloat]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeBinaryFloat)
		}
	case common.BuildInOracleDatatypeBinaryDouble:
		originColumnType = common.BuildInOracleDatatypeBinaryDouble
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeBinaryDouble]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeBinaryDouble)
		}
	case common.BuildInOracleDatatypeNchar:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeNchar, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeNchar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeNchar)
		}
	case common.BuildInOracleDatatypeNcharVarying:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeNcharVarying, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeNcharVarying]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeNcharVarying)
		}
	case common.BuildInOracleDatatypeNclob:
		originColumnType = common.BuildInOracleDatatypeNclob
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeNclob]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeNclob)
		}
	case common.BuildInOracleDatatypeNumeric:
		originColumnType = fmt.Sprintf("%s(%d,%d)", common.BuildInOracleDatatypeNumeric, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeNumeric]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d,%d)", common.StringUPPER(val), dataPrecision, dataScale)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeNumeric)
		}
	case common.BuildInOracleDatatypeNvarchar2:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeNvarchar2, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeNvarchar2]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeNvarchar2)
		}
	case common.BuildInOracleDatatypeRaw:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeRaw, dataLength)
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if dataLength < 256 {
		//	buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		//} else {
		//	buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		//}
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeRaw]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeRaw)
		}
	case common.BuildInOracleDatatypeReal:
		originColumnType = common.BuildInOracleDatatypeReal
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeReal]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeReal)
		}
	case common.BuildInOracleDatatypeRowid:
		originColumnType = common.BuildInOracleDatatypeRowid
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeRowid]; ok {
			buildInColumnType = fmt.Sprintf("%s(10)", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeRowid)
		}
	case common.BuildInOracleDatatypeSmallint:
		originColumnType = common.BuildInOracleDatatypeSmallint
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeSmallint]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeSmallint)
		}
	case common.BuildInOracleDatatypeUrowid:
		originColumnType = common.BuildInOracleDatatypeUrowid
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeUrowid]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeUrowid)
		}
	case common.BuildInOracleDatatypeVarchar2:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeVarchar2, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeVarchar2]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeVarchar2)
		}
	case common.BuildInOracleDatatypeVarchar:
		originColumnType = fmt.Sprintf("%s(%d)", common.BuildInOracleDatatypeVarchar, dataLength)
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeVarchar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeVarchar)
		}
	case common.BuildInOracleDatatypeXmltype:
		originColumnType = common.BuildInOracleDatatypeXmltype
		if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeXmltype]; ok {
			buildInColumnType = fmt.Sprintf("%s", common.StringUPPER(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.BuildInOracleDatatypeXmltype)
		}
	default:
		if strings.Contains(column.DataType, "INTERVAL YEAR") {
			originColumnType = column.DataType
			if val, ok := buildinDatatypeMap[common.StringUPPER(originColumnType)]; ok {
				buildInColumnType = fmt.Sprintf("%s(30)", common.StringUPPER(val))
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.StringUPPER(originColumnType))
			}
		} else if strings.Contains(column.DataType, "INTERVAL DAY") {
			originColumnType = column.DataType
			if val, ok := buildinDatatypeMap[common.BuildInOracleDatatypeIntervalDay]; ok {
				buildInColumnType = fmt.Sprintf("%s(30)", common.StringUPPER(val))
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.StringUPPER(originColumnType))
			}
		} else if strings.Contains(column.DataType, "TIMESTAMP") {
			originColumnType = column.DataType
			if dataScale <= 6 {
				if val, ok := buildinDatatypeMap[common.StringUPPER(originColumnType)]; ok {
					buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), dataScale)
					return originColumnType, buildInColumnType, nil
				} else {
					return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.StringUPPER(originColumnType))
				}
			} else {
				if val, ok := buildinDatatypeMap[common.StringUPPER(originColumnType)]; ok {
					buildInColumnType = fmt.Sprintf("%s(%d)", common.StringUPPER(val), 6)
					return originColumnType, buildInColumnType, nil
				} else {
					return originColumnType, buildInColumnType, fmt.Errorf("oracle table column type [%s] map mysql column type rule isn't exist, please checkin", common.StringUPPER(originColumnType))
				}
			}
		} else {
			originColumnType = column.DataType
			buildInColumnType = "TEXT"
		}
		return originColumnType, buildInColumnType, nil
	}
}
