package elasticsearch

import (
	elastic "gopkg.in/olivere/elastic.v1"
	"sync"
	"net/http"
	"log"
	"encoding/json"
	"fmt"
)

type DocIndexUpdate struct {
	Action string
	Id string
	Document interface{}

}
type DocDelete struct {
	Id string

}
var synchro sync.Once
var elasticLib *ElasticSearchClient


type ElasticSearchClient struct {
	Client      *elastic.Client
}

func NewElasticSearchClient(domains... string) *ElasticSearchClient{
	synchro.Do(func() {
		client, err := elastic.NewClient(
			http.DefaultClient,
			domains...)
		if err != nil {
			log.Fatalf("Elasticsearch client error: %s", err)
		}
		elasticLib = &ElasticSearchClient{
			Client: client}
	})
	for i := 0; i < len(domains); i++ {
		ping, _, err := elasticLib.Client.Ping().URL(domains[i]).Do()
		if err != nil {
			log.Fatalf("Elasticsearch Ping error: %s", err)
		}
		if ping.Status != 200 {
			log.Fatalf("Elasticsearch server unreachable, error: %s", err)
		}
	}
	return elasticLib
}

func (e *ElasticSearchClient) HandleCommonQuery(query string, name string, boost float64, indexType string, index... string) <-chan map[string]interface{} {
	var response chan map[string]interface{}
	if len(name) == 0 {
		name = "default-name-query"
	}
	if boost < 0 {
		log.Fatal("Boost must not be negative")
	}
	go func() {
		defer close(response)

		commonQuery := elastic.NewCommonQuery(name, query)
		commonQuery.Boost(boost)
		results, err := e.Client.Search(index...).Type(indexType).Query(commonQuery).Do()
		if err != nil {
			log.Fatalf("query failed, %s", err)
		}
		if results.Hits == nil {
			log.Fatalf("expected SearchResult.Hits != nil; got nil")
		}
		if results.Hits.TotalHits != 1 {
			log.Fatalf("expected results.Hits.TotalHits = %d; got %d", 1, results.Hits.TotalHits)
		}
		if len(results.Hits.Hits) != 1 {
			log.Fatalf("expected len(results.Hits.Hits) = %d; got %d", 1, len(results.Hits.Hits))
		}

		for _, hit := range results.Hits.Hits {
			item := make(map[string]interface{})
			err := json.Unmarshal(*hit.Source, &item)
			response <- item
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	return response
}

func (e *ElasticSearchClient) HandleFindById(index string, indexType string, id string) <-chan *elastic.GetResult {
	var response chan *elastic.GetResult

	go func() {
		defer close(response)


		result, err := e.Client.Get().Index(index).Type(indexType).Id(id).Do()
		if err != nil {
			log.Fatalf("finding id failed, %s", err)
		}
		if result.Found {
			fmt.Printf("Got document %s in version %d from index %s, type %s\n", result.Id, result.Version, result.Index, result.Type)
		}
		response <- result
	}()

	return response
}

//func (e *ElasticSearchClient) HandleDeleteFromQuery(index string, indexType string, query string) <-chan *elastic.GetResult {
//
//}

func (e *ElasticSearchClient) HandleCreateIndex(index string, mapping string) <-chan bool {
	responseChan := make(chan bool)
	go func() {
		defer
			close(responseChan)
		if !e.indexExits(index) {
			indexCreate := elastic.NewCreateIndexService(e.Client)
			result, err := indexCreate.Index(index).Do()
			if result.Acknowledged {
				log.Printf("Index %s created with sucess", index)
			} else {
				log.Fatalf("Index %s creation failed", index)
			}

			if err != nil {
				log.Fatalf("Index %s creation failed with error: %s", index, err)
			}
			responseChan <- result.Acknowledged
		} else {
			responseChan <- true
			log.Printf("Index %s already exist skipping...", index)
		}
	}()
	return responseChan
}

func (e *ElasticSearchClient) HandleDeleteIndex(index string) <-chan bool {
	responseChan := make(chan bool)
	go func() {
		defer close(responseChan)
		if e.indexExits(index) {
			indexDelete := elastic.NewDeleteIndexService(e.Client)
			result, err := indexDelete.Index(index).Do()
			if result.Acknowledged {
				log.Printf("Index %s deleted with sucess", index)
			} else {
				log.Fatalf("Index %s delete failed", index)
			}

			if err != nil {
				log.Fatalf("Index %s delete failed with error: %s", index, err)
			}
			responseChan <- result.Acknowledged
		} else {
			log.Printf("Index %s does not exist skipping...", index)
			responseChan <- true
		}
	}()
	return responseChan
}

func (e *ElasticSearchClient) HandleBulk(index string, indexType string, data... interface{}) <-chan *elastic.BulkResponse {
	bulkResponseChan := make(chan *elastic.BulkResponse)
	bulkClient := e.Client.Bulk()
	if !e.indexExits(index) {
		log.Fatal("Index does not exist")
	}
	var deleteDoc *elastic.BulkDeleteRequest
	var updateDoc *elastic.BulkUpdateRequest
	var indexDoc *elastic.BulkIndexRequest
	go func() {
		defer close(bulkResponseChan)
		for i := 0; i < len(data); i++ {
			switch data[i].(type) {
			case *DocDelete:
				log.Print(data[i].(*DocDelete))
				deleteDoc = elastic.NewBulkDeleteRequest().Index(index).Type(indexType).Id(data[i].(*DocDelete).Id)
				bulkClient.Add(deleteDoc)
			case *DocIndexUpdate:
				if data[i].(*DocIndexUpdate).Action == "" {
					log.Fatal("Action property is mandatory in given structure.")
				}
				if data[i].(*DocIndexUpdate).Action == "update" {
					log.Print(data[i].(*DocIndexUpdate).Document)
					if  data[i].(*DocIndexUpdate).Id == "" {
						log.Fatal("Id property is mandatory when updating document.")
					}
					updateDoc = elastic.NewBulkUpdateRequest().Index(index).Type(indexType).Id(data[i].(*DocIndexUpdate).Id).Doc(data[i].(*DocIndexUpdate).Document)
					bulkClient.Add(updateDoc)
				} else {
					indexDoc = elastic.NewBulkIndexRequest().Index(index).Type(indexType).Doc(data[i].(*DocIndexUpdate).Document)
					if  data[i].(*DocIndexUpdate).Id != "" {
						indexDoc.Id(data[i].(*DocIndexUpdate).Id)
					}
					bulkClient.Add(indexDoc)
				}
			default:
				log.Fatalf("Given structure %v is not supported", data[i])
			}

		}

		bulkResponse, err := bulkClient.Do()
		if err != nil {
			log.Fatal(err)
		}
		if bulkResponse == nil {
			log.Fatalf("expected bulkResponse to be != nil; got nil")
		}
		if bulkResponse.Errors {
			log.Fatalf("Bulk response error %v", bulkResponse.Errors)
		}
		bulkResponseChan <- bulkResponse

	}()
	return bulkResponseChan
}

func (e *ElasticSearchClient) indexExits(index string) bool{
	exists, err := e.Client.IndexExists(index).Do()

	if err != nil {
		log.Fatal(err)
	}

	return exists
}