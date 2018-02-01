package main

import (
	"fmt"
	"github.com/go-workers/drivers/elasticsearch"
	"time"
	"log"
)

var (
	ES_HOST = "http://localhost:9200/"
)

type EsShow struct {
	Id        string
	EventId   int
	EventName string
	EventDesc string
}

func main() {
	fmt.Println("Starting test worker elasticsearch...")
	esDriver := elasticsearch.NewElasticSearchClient(ES_HOST)
	response := esDriver.HandleCreateIndex("test", "")

	for {
		isCreated, m := <-response
		if !m {
			break
		}
		log.Print("Waiting index creation...")
		time.Sleep(1 * time.Second)
		log.Printf("Index created ? %v", isCreated)
	}

	data1 := elasticsearch.DocIndexUpdate{Action: "index", Document: EsShow{"123-123-123", 0, "test-alex", "test-alex"}}
	data2 := elasticsearch.DocIndexUpdate{Action: "index", Id: "1", Document: EsShow{"123-123-123", 0, "test-alex", "test-alex"}}
	data3 := elasticsearch.DocIndexUpdate{Action: "index", Id: "2", Document: EsShow{"123-123-123", 0, "test-alex", "test-alex"}}
	data4 := elasticsearch.DocIndexUpdate{Action: "update", Id: "2", Document: EsShow{"122-123-123", 0, "test-alex2", "test-alex2"}}
	data5 := elasticsearch.DocDelete{Id: "1"}
	success := esDriver.HandleBulk("test", "show", &data1, &data2, &data3, &data4, &data5)
	for {
		m, more := <-success
		if more {
			fmt.Println("Bulk response : ", m)
		} else {
			fmt.Println("No more document, shutting worker elastic...")
			return
		}
	}
}
