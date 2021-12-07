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
	"net"
	"reflect"
	"strings"
)

// 数组拆分
func SplitIntSlice(arr []int, num int64) [][]int {
	var segmens = make([][]int, 0)
	if num == 0 {
		segmens = append(segmens, arr)
		return segmens
	}
	max := int64(len(arr))
	if max < num {
		quantity := max / max
		end := int64(0)
		for i := int64(1); i <= max; i++ {
			qu := i * quantity
			if i != max {
				segmens = append(segmens, arr[i-1+end:qu])
			} else {
				segmens = append(segmens, arr[i-1+end:])
			}
			end = qu - i
		}
		return segmens
	}
	quantity := max / num
	end := int64(0)
	for i := int64(1); i <= num; i++ {
		qu := i * quantity
		if i != num {
			segmens = append(segmens, arr[i-1+end:qu])
		} else {
			segmens = append(segmens, arr[i-1+end:])
		}
		end = qu - i
	}
	return segmens
}

// 多重数组拆分
func SplitMultipleStringSlice(arr []string, num int64) [][]string {
	var segmens = make([][]string, 0)
	if num == 0 {
		segmens = append(segmens, arr)
		return segmens
	}
	max := int64(len(arr))
	if max < num {
		quantity := max / max
		end := int64(0)
		for i := int64(1); i <= max; i++ {
			qu := i * quantity
			if i != max {
				segmens = append(segmens, arr[i-1+end:qu])
			} else {
				segmens = append(segmens, arr[i-1+end:])
			}
			end = qu - i
		}
		return segmens
	}
	quantity := max / num
	end := int64(0)
	for i := int64(1); i <= num; i++ {
		qu := i * quantity
		if i != num {
			segmens = append(segmens, arr[i-1+end:qu])
		} else {
			segmens = append(segmens, arr[i-1+end:])
		}
		end = qu - i
	}
	return segmens
}

// 用于版本号比较
func VersionOrdinal(version string) string {
	// ISO/IEC 14651:2011
	const maxByte = 1<<8 - 1
	vo := make([]byte, 0, len(version)+8)
	j := -1
	for i := 0; i < len(version); i++ {
		b := version[i]
		if '0' > b || b > '9' {
			vo = append(vo, b)
			j = -1
			continue
		}
		if j == -1 {
			vo = append(vo, 0x00)
			j = len(vo) - 1
		}
		if vo[j] == 1 && vo[j+1] == '0' {
			vo[j+1] = b
			continue
		}
		if vo[j]+1 > maxByte {
			panic("VersionOrdinal: invalid version")
		}
		vo = append(vo, b)
		vo[j]++
	}
	return string(vo)
}

// 用于对比 struct 是否相等
func DiffStructArray(structA, structB interface{}) ([]interface{}, []interface{}, bool) {
	var (
		addDiffs    []interface{}
		removeDiffs []interface{}
	)
	aVal := reflect.ValueOf(structA)
	bVal := reflect.ValueOf(structB)

	if !aVal.IsValid() && !bVal.IsValid() {
		return addDiffs, removeDiffs, true
	}

	if aVal.Kind() == reflect.Struct && bVal.Kind() == reflect.Struct {
		if !reflect.DeepEqual(structA, structB) {
			addDiffs = append(addDiffs, structA)
		}
		return addDiffs, removeDiffs, false
	}

	if aVal.IsNil() && bVal.IsNil() {
		return addDiffs, removeDiffs, true
	}

	if aVal.IsNil() && !bVal.IsNil() {
		if bVal.Kind() == reflect.Slice || bVal.Kind() == reflect.Array {
			for bi := 0; bi < bVal.Len(); bi++ {
				removeDiffs = append(removeDiffs, bVal.Index(bi).Interface())
			}
		}
	}

	if !aVal.IsNil() && bVal.IsNil() {
		if aVal.Kind() == reflect.Slice || aVal.Kind() == reflect.Array {
			for ai := 0; ai < aVal.Len(); ai++ {
				addDiffs = append(addDiffs, aVal.Index(ai).Interface())
			}
		}
	}

	if !aVal.IsNil() && !bVal.IsNil() {
		if (aVal.Kind() == reflect.Slice && bVal.Kind() == reflect.Slice) || (aVal.Kind() == reflect.Array && bVal.Kind() == reflect.Array) {
			dict := make(map[interface{}]bool)
			for bi := 0; bi < bVal.Len(); bi++ {
				dict[bVal.Index(bi).Interface()] = true
			}
			for ai := 0; ai < aVal.Len(); ai++ {
				if _, ok := dict[aVal.Index(ai).Interface()]; !ok {
					addDiffs = append(addDiffs, aVal.Index(ai).Interface())
				}
			}
		}
	}
	if len(addDiffs) == 0 && len(removeDiffs) == 0 {
		return addDiffs, removeDiffs, true
	}
	return addDiffs, removeDiffs, false
}

// 获取本机 IP
func GetOutBoundIP(pprofPort string) (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return "", err
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return StringsBuilder(strings.Split(localAddr.String(), ":")[0], pprofPort), nil
}
