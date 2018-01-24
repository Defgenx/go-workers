package main

import (
	"fmt"
	"github.com/go-workers/consumers"
	"flag"
	"time"
)

var (
	timer          = flag.Duration("lifetime", 5*time.Second, "lifetime of process before shutdown (0s=infinite)")
)

func Init() {
	flag.Parse()
}

func main() {
	Init()
	fmt.Println("Starting test worker...")
	_, msg, _ := consumers.Consume(timer)

	for {
		m, more := <-msg
		if more {
			fmt.Println("Message: ", m)
		} else {
			fmt.Println("No more message, shutting worker...")
			return
		}
	}
}
