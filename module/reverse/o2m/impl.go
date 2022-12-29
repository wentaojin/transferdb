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
	"github.com/wentaojin/transferdb/module/reverse"
	"strings"
)

func IChanger(c reverse.Changer) (map[string]string, map[string]map[string]string, map[string]map[string]string, error) {
	tableNameRuleMap, err := c.ChangeTableName()
	if err != nil {
		return nil, nil, nil, err
	}
	tableColumnDatatypeMap, err := c.ChangeTableColumnDatatype()
	if err != nil {
		return nil, nil, nil, err
	}
	tableDefaultValueMap, err := c.ChangeTableColumnDefaultValue()
	if err != nil {
		return nil, nil, nil, err
	}
	return tableNameRuleMap, tableColumnDatatypeMap, tableDefaultValueMap, nil
}

func IReader(t *Table, rd reverse.Reader) (*Rule, error) {
	primaryKey, err := rd.GetTablePrimaryKey()
	if err != nil {
		return nil, err
	}
	uniqueKey, err := rd.GetTableUniqueKey()
	if err != nil {
		return nil, err
	}
	foreignKey, err := rd.GetTableForeignKey()
	if err != nil {
		return nil, err
	}
	checkKey, err := rd.GetTableCheckKey()
	if err != nil {
		return nil, err
	}
	uniqueIndex, err := rd.GetTableUniqueIndex()
	if err != nil {
		return nil, err
	}
	normalIndex, err := rd.GetTableNormalIndex()
	if err != nil {
		return nil, err
	}
	tableComment, err := rd.GetTableComment()
	if err != nil {
		return nil, err
	}
	columnMeta, err := rd.GetTableColumnMeta()
	if err != nil {
		return nil, err
	}
	// M2O -> mysql/tidb need, because oracle comment sql special
	// O2M -> it is not need
	columnComment, err := rd.GetTableColumnComment()
	if err != nil {
		return nil, err
	}

	return &Rule{
		SourceSchema:             t.SourceSchemaName,
		SourceTableName:          t.SourceTableName,
		TargetSchema:             t.TargetSchemaName,
		TargetTableName:          t.TargetTableName,
		PrimaryKeyINFO:           primaryKey,
		UniqueKeyINFO:            uniqueKey,
		ForeignKeyINFO:           foreignKey,
		CheckKeyINFO:             checkKey,
		UniqueIndexINFO:          uniqueIndex,
		NormalIndexINFO:          normalIndex,
		TableCommentINFO:         tableComment,
		TableColumnINFO:          columnMeta,
		ColumnCommentINFO:        columnComment,
		ColumnDatatypeRule:       t.TableColumnDatatypeRule,
		ColumnDataDefaultvalRule: t.TableColumnDefaultValRule,
		OracleCollation:          t.OracleCollation,
	}, nil
}

func IReverse(t *Table, s reverse.Generator) (*DDL, error) {
	reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err := s.GenCreateTableDDL()
	if err != nil {
		return nil, err
	}
	tableComment, err := s.GenTableComment()
	if err != nil {
		return nil, err
	}

	primaryKeyMap, err := t.GetTablePrimaryKey()
	if err != nil {
		return nil, err
	}
	var primaryColumns []string
	if len(primaryKeyMap) > 0 {
		for _, col := range strings.Split(primaryKeyMap[0]["COLUMN_LIST"], ",") {
			primaryColumns = append(primaryColumns, fmt.Sprintf("`%s`", col))
		}
	}

	columnMetas, err := s.GenTableColumn()
	if err != nil {
		return nil, err
	}

	tableSuffix, err := t.GenTableSuffix(primaryColumns, t.IsSingleIntegerPK(primaryColumns, columnMetas))
	if err != nil {
		return nil, err
	}

	return &DDL{
		SourceSchemaName: t.SourceSchemaName,
		SourceTableName:  t.SourceTableName,
		SourceTableType:  t.SourceTableType,
		TargetSchemaName: s.GenSchemaName(), // change schema name
		TargetTableName:  s.GenTableName(),  // change table name
		TargetDBType:     t.TargetDBType,
		TargetDBVersion:  t.TargetDBVersion,
		ReverseDDL:       reverseDDL,
		TableSuffix:      tableSuffix,
		TableComment:     tableComment,
		CheckKeyDDL:      checkKeyDDL,
		ForeignKeyDDL:    foreignKeyDDL,
		CompatibleDDL:    compatibleDDL,
	}, nil
}

func IWriter(f *reverse.File, w reverse.Writer) error {
	err := w.Writer(f)
	if err != nil {
		return err
	}
	return nil
}
