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
package main

import (
	"fmt"
	"regexp"
	"strings"
)

func main() {
	// 多个检查约束匹配
	// 比如："LOC" IS noT nUll and loc in ('a','b','c')
	str := `df not in (ff) and "LOC" IS noT nUll or "Loc" in ('a','b','c') or lp not int('1') and "fg" is not null or ko in ( 'dd' ) and `
	r, err := regexp.Compile(`\s+(?i:AND)\s+|\s+(?i:OR)\s+`)
	if err != nil {
		fmt.Printf("check constraint regexp and/or failed: %v", err)
	}

	// 排除非空约束检查
	s := strings.TrimSpace(str)
	if !r.MatchString(s) {
		matchNull, err := regexp.MatchString(`(^.*)(?i:IS NOT NULL)`, s)
		if err != nil {
			fmt.Printf("check constraint remove not null failed: %v", err)
		}

		if !matchNull {
			fmt.Println("not match 1")
		} else {
			fmt.Println("match 1")
		}
	} else {
		strArray := strings.Fields(s)

		var (
			idxArray        []int
			checkArray      []string
			constraintArray []string
		)
		for idx, val := range strArray {
			if strings.EqualFold(val, "AND") || strings.EqualFold(val, "OR") {
				idxArray = append(idxArray, idx)
			}
		}

		idxArray = append(idxArray, len(strArray))
		fmt.Println(idxArray)

		for idx, val := range idxArray {
			if idx == 0 {
				checkArray = append(checkArray, strings.Join(strArray[0:val], " "))
			} else {
				checkArray = append(checkArray, strings.Join(strArray[idxArray[idx-1]:val], " "))
			}
		}

		for _, val := range checkArray {
			v := strings.TrimSpace(val)
			matchNull, err := regexp.MatchString(`(.*)(?i:IS NOT NULL)`, v)
			if err != nil {
				fmt.Printf("check constraint remove not null failed: %v", err)
			}

			if !matchNull {
				constraintArray = append(constraintArray, v)
			} else {
				fmt.Printf("match: %s\n", v)
			}
		}

		sd := strings.Join(constraintArray, " ")
		d := strings.Fields(sd)
		if strings.EqualFold(d[0], "AND") || strings.EqualFold(d[0], "OR") {
			d = d[1:]
		}
		if strings.EqualFold(d[len(d)-1], "AND") || strings.EqualFold(d[len(d)-1], "OR") {
			d = d[:len(d)-1]
		}
		fmt.Println(strings.Join(d, " "))
	}
}
