package main

import (
	"fmt"
	"log"
	"github.com/go-workers/workers"
	"github.com/go-workers/drivers/elasticsearch"
	"github.com/go-workers/drivers/mysql"
	"github.com/go-workers/drivers/rabbitmq"
	"time"
	"sync"
)

type EsShow struct {
	Id        string
	EventId   int
	EventName string
	EventDesc string
}

var esDriver *elasticsearch.ElasticSearchClient
var mysqlDriver *mysql.Mysql
var rabbitConsumerDriver *rabbitmq.Consumer
var rabbitProducerDriver *rabbitmq.Producer

func main() {
	fmt.Println("Starting test worker multi driver + config...")
	workerConfig := workers.NewConfig("/home/adelvecchio/go/src/github.com/go-workers/workers/multi/config/config.yml")
	startDrivers(workerConfig)

	for {
		if reload := <-workerConfig.IsReloaded; reload {
			log.Print("Config was modified... Recreate drivers asynchronously")
			startDrivers(workerConfig)
		}
	}
}

func startDrivers(workerConfig *workers.Config) {
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		if esDriver == nil {
			fmt.Println("Starting elasticsearch driver...")
		} else {
			fmt.Println("Stoping elasticsearch driver...")
			esDriver = nil
			fmt.Println("Restarting elasticsearch driver...")
		}
		log.Print(workerConfig.Elasticsearch["local"])
		esDriver = elasticsearch.NewElasticSearchClient(workerConfig.Elasticsearch["local"].Domains...)
		log.Printf("elastic driver initialized %v", esDriver)
	}()

	go func() {
		defer wg.Done()
		if mysqlDriver == nil {
			fmt.Println("Starting mysql driver...")
		} else {
			fmt.Println("Stoping mysql driver...")
			mysqlDriver.Shutdown()
			fmt.Println("Restarting mysql driver...")
		}
		log.Print(workerConfig.Mysql["maria"])
		mysqlDriver, _ = mysql.NewMysql(
			workerConfig.Mysql["maria"].Host,
			workerConfig.Mysql["maria"].Login,
			workerConfig.Mysql["maria"].Password,
			workerConfig.Mysql["maria"].Database,
			workerConfig.Mysql["maria"].Port,
			workerConfig.Mysql["maria"].Protocol)
		log.Printf("mysql driver initialized %v", mysqlDriver)
	}()

	go func() {
		defer wg.Done()
		if rabbitConsumerDriver == nil {
			fmt.Println("Starting rabbitmq consumer driver...")
		} else {
			fmt.Println("Stoping rabbitmq consumer driver...")
			rabbitConsumerDriver.Shutdown()
			fmt.Println("Restarting rabbitmq consumer driver...")
		}
		log.Print(workerConfig.RabbitMQConsumer["local"])
		timer := 0 * time.Second
		rabbitConsumerDriver, _, _ = rabbitmq.NewConsumer(
			workerConfig.RabbitMQConsumer["local"].Uri,
			workerConfig.RabbitMQConsumer["local"].Exchange,
			workerConfig.RabbitMQConsumer["local"].ExchangeType,
			workerConfig.RabbitMQConsumer["local"].QueueName,
			workerConfig.RabbitMQConsumer["local"].BindingKey,
			workerConfig.RabbitMQConsumer["local"].ConsumerTag,
			&timer)
		log.Printf("rabbitMQ consumer driver initialized %v", rabbitConsumerDriver)
	}()

	go func() {
		defer wg.Done()
		if rabbitProducerDriver == nil {
			fmt.Println("Starting rabbitmq producer driver...")
		} else {
			fmt.Println("Stoping rabbitmq producer driver...")
			rabbitProducerDriver.Shutdown()
			fmt.Println("Restarting rabbitmq producer driver...")
		}
		log.Print(workerConfig.RabbitMQProducer["local"])
		rabbitProducerDriver, _ = rabbitmq.NewProducer(
			workerConfig.RabbitMQProducer["local"].Uri,
			workerConfig.RabbitMQProducer["local"].Exchange,
			workerConfig.RabbitMQProducer["local"].ExchangeType,
			workerConfig.RabbitMQProducer["local"].RoutingKey,
			workerConfig.RabbitMQProducer["local"].Reliable,
			workerConfig.RabbitMQProducer["local"].ProducerTag)
		log.Printf("rabbitMQ producer driver initialized %v", rabbitConsumerDriver)
	}()
	wg.Wait()
}
