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

	"github.com/xxjwxc/gowp/workpool"
)

func main() {
	var data [][]int
	data1 := []int{10, 11}
	data2 := []int{12, 13}
	data3 := []int{14, 15}
	data = append(data, data1)
	data = append(data, data2)
	data = append(data, data3)
	fmt.Println(data)

	for i := 0; i < 3; i++ {

		wp := workpool.New(2)
		for _, dt := range data {
			ft := dt
			wp.Do(func() error {
				fmt.Println(ft)
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			fmt.Println(err)
		}
	}
}
