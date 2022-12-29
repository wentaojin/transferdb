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
package reverse

type Changer interface {
	ChangeTableName() (map[string]string, error)
	ChangeTableColumnDatatype() (map[string]map[string]string, error)
	ChangeTableColumnDefaultValue() (map[string]map[string]string, error)
}

type Reader interface {
	GetTablePrimaryKey() ([]map[string]string, error)
	GetTableUniqueKey() ([]map[string]string, error)
	GetTableForeignKey() ([]map[string]string, error)
	GetTableCheckKey() ([]map[string]string, error)
	GetTableUniqueIndex() ([]map[string]string, error)
	GetTableNormalIndex() ([]map[string]string, error)
	GetTableComment() ([]map[string]string, error)
	GetTableColumnMeta() ([]map[string]string, error)
	GetTableColumnComment() ([]map[string]string, error)
}

type Generator interface {
	GenSchemaName() string
	GenTableName() string
	GenTableNamePrefix() (string, string)
	GenTableKeyMeta() (tableKeyMetas []string, compatibilityIndexSQL []string, err error)
	GenTablePrimaryKey() (primaryKeyMetas []string, err error)
	GenTableUniqueKey() (uniqueKeyMetas []string, err error)
	GenTableForeignKey() (foreignKeyMetas []string, err error)
	GenTableCheckKey() (checkKeyMetas []string, err error)
	GenTableUniqueIndex() (uniqueIndexMetas []string, compatibilityIndexSQL []string, err error)
	GenTableNormalIndex() (normalIndexMetas []string, compatibilityIndexSQL []string, err error)
	GenTableComment() (tableComment string, err error)
	GenTableColumn() (columnMetas []string, err error)
	GenTableColumnComment() (columnComments []string, err error)
	GenCreateTableDDL() (reverseDDL string, checkKeyDDL []string, foreignKeyDDL []string, compatibleDDL []string, err error)
}

type Writer interface {
	Writer(f *File) error
}

type Reverser interface {
	NewReverse() error
}
