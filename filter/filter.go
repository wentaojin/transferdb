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
package filter

// 表过滤接口
type Filter interface {
	// MatchTable 检查表是否匹配
	MatchTable(table string) bool
}

// tableFilter Filter 接口具体实现
type tableFilter []tableRule

// Parse 序列化 tableFilter 规则列表的 tableFilter
func Parse(args []string) (Filter, error) {
	p := tableRulesParser{make([]tableRule, 0, len(args))}

	for _, arg := range args {
		if err := p.parse(arg); err != nil {
			return nil, err
		}
	}

	return tableFilter(p.rules), nil
}

// MatchTable 检查应用 tableFilter `f` 是否匹配
func (f tableFilter) MatchTable(table string) bool {
	for _, rule := range f {
		if rule.table.matchString(table) {
			return true
		}
	}
	return false
}
