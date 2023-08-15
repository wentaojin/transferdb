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
	ChangeTableColumnDefaultValue() (map[string]map[string]bool, map[string]map[string]string, error)
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
	GetTableInfo() (interface{}, error)
}

type Generator interface {
	GenSchemaName() string
	GenTableName() string
	GenTablePrefix() (string, string)
	GenTableSuffix() (string, error)
	GenTableKeys() (tableKeys []string, compatibilityIndexSQL []string, err error)
	GenTablePrimaryKey() (primaryKeys []string, err error)
	GenTableUniqueKey() (uniqueKeys []string, err error)
	GenTableForeignKey() (foreignKeys []string, err error)
	GenTableCheckKey() (checkKeys []string, err error)
	GenTableUniqueIndex() (uniqueIndexes []string, compatibilityIndexSQL []string, err error)
	GenTableNormalIndex() (normalIndexes []string, compatibilityIndexSQL []string, err error)
	GenTableComment() (tableComment string, err error)
	GenTableColumn() (tableColumns []string, err error)
	GenTableColumnComment() (columnComments []string, err error)
	GenCreateTableDDL() (interface{}, error)
}

type Writer interface {
	Write(w *Write) (string, error)
}

type Reverser interface {
	Reverse() error
}
