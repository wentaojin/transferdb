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
package utils

import (
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
	"github.com/thinkeridea/go-extend/exbytes"
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
		s1.Add(strings.ToUpper(t))
	}
	s2 := set.NewStringSet()
	for _, t := range checkItems {
		s2.Add(strings.ToUpper(t))
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
		s1.Add(strings.ToUpper(t))
	}
	s2 := set.NewStringSet()
	for _, t := range excludeItems {
		s2.Add(strings.ToUpper(t))
	}
	return strset.Difference(s1, s2).List()
}

// 过滤两个数组相同元素（交集），返回新数组
func FilterIntersectionStringItems(originItems, newItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToUpper(t))
	}
	s2 := set.NewStringSet()
	for _, t := range newItems {
		s2.Add(strings.ToUpper(t))
	}
	return strset.Intersection(s1, s2).List()
}

// 删除字符中最后一个字母
func TrimLastChar(s string) string {
	r, size := utf8.DecodeLastRuneInString(s)
	if r == utf8.RuneError && (size == 0 || size == 1) {
		size = 0
	}
	return s[:len(s)-size]
}

// 判断字符是否是数字
func IsNum(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// 字符转换
func StrconvIntBitSize(s string, bitSize int) (int64, error) {
	i, err := strconv.ParseInt(s, 10, bitSize)
	if err != nil {
		return i, err
	}
	return i, nil
}

func StrconvUintBitSize(s string, bitSize int) (uint64, error) {
	i, err := strconv.ParseUint(s, 10, bitSize)
	if err != nil {
		return i, err
	}
	return i, nil
}

func StrconvFloatBitSize(s string, bitSize int) (float64, error) {
	i, err := strconv.ParseFloat(s, bitSize)
	if err != nil {
		return i, err
	}
	return i, nil
}

func StrconvRune(s string) (int32, error) {
	r, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return rune(r), err
	}
	return rune(r), nil
}

// 替换字符串引号字符
func ReplaceQuotesString(s string) string {
	return string(exbytes.Replace([]byte(s), []byte("\""), []byte(""), -1))
}

// 替换指定字符
func ReplaceSpecifiedString(s string, oldStr, newStr string) string {
	return string(exbytes.Replace([]byte(s), []byte(oldStr), []byte(newStr), -1))
}

// 忽略大小写切分字符串
func ReSplit(text string, cut string) []string {
	pattern := StringsBuilder("(?i)", cut)
	regex := regexp.MustCompile(pattern)
	result := regex.Split(text, -1)
	return result
}

// 字符数组转字符
func StringArrayToCapitalChar(strs []string) string {
	var newStrs []string
	for _, s := range strs {
		newStrs = append(newStrs, StringsBuilder("'", strings.ToUpper(s), "'"))
	}
	return strings.Join(newStrs, ",")
}

// 字符串拼接
func StringsBuilder(str ...string) string {
	var b strings.Builder
	for _, p := range str {
		b.WriteString(p)
	}
	return b.String() // no copying
}
