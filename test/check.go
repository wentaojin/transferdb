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

	"github.com/wentaojin/transferdb/pkg/check"
	"github.com/wentaojin/transferdb/utils"
)

func main() {
	ora := check.ColumnInfo{

		DataLength:    "10",
		DataPrecision: "12",
		DataScale:     "23",
		NULLABLE:      "34",
		DataDefault:   "'pc'",
		Comment:       "",
	}

	mysql := check.ColumnInfo{
		DataLength:    "10",
		DataPrecision: "12",
		DataScale:     "245",
		NULLABLE:      "34",
		DataDefault:   "pc",
		Comment:       "",
	}
	addDiff, removeDiff, isOK := utils.DiffStructArray(ora, mysql)
	if !isOK {
		fmt.Printf("%v: 1 \n", addDiff)
		fmt.Printf("%v: 2\n", removeDiff)
	} else {
		fmt.Println(11111)
	}
}
