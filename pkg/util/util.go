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
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
)

// IsExistIncludeTable checks includeTables is whether exist
func IsExistIncludeTable(allTables, includeTables []string) (bool, []string) {
	s1 := set.NewStringSet()
	for _, t := range allTables {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range includeTables {
		s2.Add(strings.ToLower(t))
	}
	isSubset := s1.IsSubset(s2)
	var notExistTables []string
	if !isSubset {
		notExistTables = strset.Difference(s2, s1).List()
	}
	return isSubset, notExistTables
}

// FilterFromAllTables filters table from all tables
func FilterFromAllTables(allTables, excludeTables []string) []string {
	// exclude table from all tables
	s1 := set.NewStringSet()
	for _, t := range allTables {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range excludeTables {
		s2.Add(strings.ToLower(t))
	}
	return strset.Difference(s1, s2).List()
}

// RegexpFromAllTables regexp table from all tables
func RegexpFromAllTables(allTables []string, regex string) []string {
	var regexTables []string
	rep := regexp.MustCompile(regex)
	for _, v := range allTables {
		if rep.MatchString(v) {
			regexTables = append(regexTables, v)
		}
	}
	return regexTables
}

// IsExistPath checks path is whether exist
func IsExistPath(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// find two slice repeat elem
func FindStringSliceRepeatElem(slice1, slice2 []string) []string {
	var u []string
	for _, v := range slice1 {
		if stringsContains(slice2, v) {
			u = append(u, v)
		}
	}
	return u
}

func stringsContains(array []string, val string) bool {
	for i := 0; i < len(array); i++ {
		if array[i] == val {
			return true
		}
	}
	return false
}

// find two slice different elem
func FindStringSlicesDiffElem(slice1 []string, slice2 []string) []string {
	var diffStr []string
	m := map[string]int{}
	for _, s1Val := range slice1 {
		m[s1Val] = 1
	}
	for _, s2Val := range slice2 {
		m[s2Val] = m[s2Val] + 1
	}
	for mKey, mVal := range m {
		if mVal == 1 {
			diffStr = append(diffStr, mKey)
		}
	}
	return diffStr
}

// determine if an element is in a slice, array, map, if true, exist
func Contain(obj interface{}, target interface{}) bool {
	targetValue := reflect.ValueOf(target)
	switch reflect.TypeOf(target).Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < targetValue.Len(); i++ {
			if targetValue.Index(i).Interface() == obj {
				return true
			}
		}
	case reflect.Map:
		if targetValue.MapIndex(reflect.ValueOf(obj)).IsValid() {
			return true
		}
	}
	return false
}

// Paginate split slice by paginate
func Paginate(x []string, skip int, size int) []string {
	limit := func() int {
		if skip+size > len(x) {
			return len(x)
		} else {
			return skip + size
		}

	}

	start := func() int {
		if skip > len(x) {
			return len(x)
		} else {
			return skip
		}

	}
	return x[start():limit()]
}

//Int returns unique int values in a slice
func Int(slice []int) []int {
	uMap := make(map[int]struct{})
	result := []int{}
	for _, val := range slice {
		uMap[val] = struct{}{}
	}
	for key := range uMap {
		result = append(result, key)
	}
	sort.Ints(result)
	return result
}

//UniqueStrings returns unique string values in a slice
func UniqueStrings(slice []string) []string {
	uMap := make(map[string]struct{})
	result := []string{}
	for _, val := range slice {
		uMap[val] = struct{}{}
	}
	for key := range uMap {
		result = append(result, key)
	}
	return result
}
