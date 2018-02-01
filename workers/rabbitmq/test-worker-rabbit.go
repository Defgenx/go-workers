package main

import (
	"fmt"
	"github.com/go-workers/drivers/rabbitmq"
	"flag"
	"time"
	"sync"
	"log"
)

var (
	timer = flag.Duration("lifetime", 5*time.Second, "lifetime of process before shutdown (0s=infinite)")
)

func Init() {
	flag.Parse()
}

func main() {
	Init()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		fmt.Println("Starting test worker rabbitmq producer...")
		message := "test"
		producer, err := rabbitmq.Produce(message)

		if err != nil {
			log.Fatalf("error occured publishing : %s", err)
		} else {
			log.Print("Message %s published", message)
			producer.Shutdown()
		}

	}()

	go func() {
		defer wg.Done()
		fmt.Println("Starting test worker rabbitmq consumer...")
		_, msg, _ := rabbitmq.Consume(timer)

		for {
			m, more := <-msg
			if more {
				fmt.Println("Message: ", m)
			} else {
				fmt.Println("No more message, shutting worker rabbitmq...")
				break
			}
		}
	}()
	wg.Wait()
	log.Print("Produce / Consume finished...")
}
