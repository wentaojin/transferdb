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
	"github.com/wentaojin/transferdb/pkg/filter"
)

func main() {
	rxp := []string{"tab*", "tac_1", "t4", "t56_P"}
	tables := []string{"tab1", "tab2", "tab3", "t3", "ta_1", "tac_1", "t56_p"}
	f, err := filter.Parse(rxp)
	if err != nil {
		panic(err)
	}

	for _, t := range tables {
		fmt.Printf("%5v: %v\n", f.MatchTable(t), t)
	}
}
