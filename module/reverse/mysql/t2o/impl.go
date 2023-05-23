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
package t2o

import (
	"github.com/wentaojin/transferdb/module/reverse"
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

func IReader(r reverse.Reader) (*Rule, error) {
	i, err := r.GetTableInfo()
	if err != nil {
		return nil, err
	}
	return &Rule{
		Table: r.(*Table),
		Info:  i.(*Info),
	}, nil
}

func IReverse(s reverse.Generator) (*DDL, error) {
	d, err := s.GenCreateTableDDL()
	if err != nil {
		return nil, err
	}
	return d.(*DDL), nil
}

func IWriter(w *reverse.Write, iw reverse.Writer) (string, error) {
	return iw.Write(w)
}
