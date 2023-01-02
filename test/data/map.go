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
	"golang.org/x/sync/errgroup"
)

func main() {
	s := &errgroup.Group{}
	s.SetLimit(5)

	final := make(map[int]map[string]string)

	ch := make(chan map[int]map[string]string, 1024)

	k := make(chan struct{})
	go func(done func()) {
		for c := range ch {
			for key, val := range c {
				final[key] = val
			}
		}
		done()
	}(func() {
		k <- struct{}{}
	})

	for _, v := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 16, 18, 200, 300} {
		f := v
		s.Go(func() error {
			m := make(map[string]string, 1)
			m["h"] = "k"
			m["a"] = "p"

			t := make(map[int]map[string]string, 1)
			t[f] = m
			ch <- t
			return nil
		})
	}

	if err := s.Wait(); err != nil {
		panic(err)
	}
	close(ch)
	<-k

	fmt.Println(final)
}
