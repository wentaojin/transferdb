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
	"sync"
	"time"
)

type gp struct {
	id   int
	name string
}

func main() {

	startTime := time.Now()
	chS := make(chan int, 5513)
	chT := make(chan gp, 5513)
	var wg = sync.WaitGroup{}

	var s []gp

	ns := "gl"

	userCount := 5513
	threads := 20
	c := make(chan struct{})

	go func(done func()) {
		for t := range chT {
			s = append(s, t)
		}
		done()
	}(func() {
		c <- struct{}{}
	})

	go func() {
		for i := 0; i < userCount; i++ {
			chS <- i
		}
		close(chS)
	}()

	for c := 0; c < threads; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cs := range chS {
				fg := gp{
					id:   cs,
					name: ns,
				}
				chT <- fg
			}

		}()
	}

	wg.Wait()
	close(chT)
	<-c

	endTime := time.Now()
	fmt.Println(len(s))
	fmt.Println(endTime.Sub(startTime).String())
}
