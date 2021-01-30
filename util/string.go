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
package util

import (
	"strings"

	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
)

// 是否空字符串
func IsEmptyString(str string) bool {
	return str == "null" || strings.TrimSpace(str) == ""
}

// 数组中是否包含某元素
func IsContainString(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

// 获取数组元素下标
func GetStringSliceElementIndex(items []string, item string) int {
	for idx, eachItem := range items {
		if eachItem == item {
			return idx
		}
	}
	return -1
}

// 两个数组对比，判断 checkItems 是否是 originItems 子集
func IsSubsetString(originItems, checkItems []string) (bool, []string) {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range checkItems {
		s2.Add(strings.ToLower(t))
	}
	isSubset := s1.IsSubset(s2)
	var notExists []string
	if !isSubset {
		notExists = strset.Difference(s2, s1).List()
	}
	return isSubset, notExists
}

// 过滤排除元素，返回新数组
func FilterDifferenceStringItems(originItems, excludeItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range excludeItems {
		s2.Add(strings.ToLower(t))
	}
	return strset.Difference(s1, s2).List()
}

// 过滤两个数组相同元素（交集），返回新数组
func FilterIntersectionStringItems(originItems, newItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range newItems {
		s2.Add(strings.ToLower(t))
	}
	return strset.Intersection(s1, s2).List()
}
