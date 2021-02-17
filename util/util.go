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
