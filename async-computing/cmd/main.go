package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var msg = make(chan int)
var computingCount int
const (
	totalComputing = 50000000000
	iter           = 10
)

func main() {
	for t := 1; t <= 10; t++ {
		computingCount =t
		for i := 0; i < iter; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			wg1 := &sync.WaitGroup{}
			wg1.Add(1)
			sum := 0
			go func() {
				defer wg1.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case number := <-msg:
						sum += number

					}
				}
			}()
			startTime := time.Now()
			wg2 := &sync.WaitGroup{}
			for j := 0; j < computingCount; j++ {
				wg2.Add(1)
				go computingAsync(wg2)
			}
			wg2.Wait()
			cancel()
			wg1.Wait()
			elapsedTime := time.Since(startTime)
			fmt.Printf("computingCount: %d,computing time: %v\n", t, elapsedTime.Seconds())
			time.Sleep(5*time.Second)
		}
	}
}

func computingAsync(wg *sync.WaitGroup) {
	defer wg.Done()
	sum := 1
	length := totalComputing / computingCount
	for j := 0; j < length; j++ {
		sum*=2
		sum/=2
	}
	msg <- sum
}
