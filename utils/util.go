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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"unicode"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// 数组拆分
func SplitMultipleStringSlice(arr [][]string, num int64) [][][]string {
	var segmens = make([][][]string, 0)
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
func SplitMultipleSlice(arr []interface{}, num int64) [][]interface{} {
	var segmens = make([][]interface{}, 0)
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

// GBK 转 UTF-8
func GbkToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	return d, nil
}

// UTF-8 转 GBK
func Utf8ToGbk(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	return d, nil
}

// 如果存在特殊字符，直接在特殊字符前添加\
/**
判断是否为字母： unicode.IsLetter(v)
判断是否为十进制数字： unicode.IsDigit(v)
判断是否为数字： unicode.IsNumber(v)
判断是否为空白符号： unicode.IsSpace(v)
判断师傅是特殊符号：unicode.IsSymbol(v)
判断是否为Unicode标点字符 :unicode.IsPunct(v)
判断是否为中文：unicode.Han(v)
*/
func SpecialLetters(letter rune) []rune {
	var chars []rune
	if unicode.IsPunct(letter) || unicode.IsSymbol(letter) || unicode.IsSpace(letter) {
		chars = append(chars, '\\', letter)
		return chars
	}
	chars = append(chars, letter)
	return chars
}

// 判断文件夹是否存在
func PathExist(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		// 创建文件夹
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("file dir MkdirAll failed: %v", err)
		}
	}
	return err
}
