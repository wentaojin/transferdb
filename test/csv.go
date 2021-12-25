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
	"bufio"
	"os"
	"path/filepath"

	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/utils"
)

func main() {
	current, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fileW, err := os.OpenFile(filepath.Join(current, "cst.csv"), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	defer fileW.Close()
	writer := bufio.NewWriter(fileW)

	var rows [][]string
	rows = append(rows, []string{"1", "f"})
	rows = append(rows, []string{"2", "f"})
	rows = append(rows, []string{"3", "f"})

	f := "\r\n"
	// 写入文件
	for _, r := range rows {
		if _, err := writer.WriteString(utils.StringsBuilder(exstrings.Join(r, ","), f)); err != nil {
			panic(err)
		}
	}

	if err := writer.Flush(); err != nil {
		panic(err)
	}
}
