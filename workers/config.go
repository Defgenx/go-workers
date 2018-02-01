package workers

import (
	"io/ioutil"
	"path/filepath"
	"gopkg.in/yaml.v2"
	"sync"
	"log"
	"github.com/howeyc/fsnotify"
)

type ELasticsearchDriver struct {
	Domains []string `yaml:"domains"`
}

type MysqlDriver struct {
	Host     string `yaml:"host"`
	Login    string `yaml:"login"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Port     int    `yaml:"port"`
	Protocol string `yaml:"protocol"`
}

type RabbitMQConsumerDriver struct {
	Uri          string `yaml:"uri"`
	Exchange     string `yaml:"exchange"`
	ExchangeType string `yaml:"exchangeType"`
	QueueName    string `yaml:"queue"`
	BindingKey   string `yaml:"bindingKey"`
	ConsumerTag  string `yaml:"consumerTag"`
}

type RabbitMQProducerDriver struct {
	Uri          string `yaml:"uri"`
	Exchange     string `yaml:"exchange"`
	ExchangeType string `yaml:"exchangeType"`
	RoutingKey   string `yaml:"routingKey"`
	Reliable     bool   `yaml:"reliable"`
	ProducerTag  string `yaml:"producerTag"`
}

type Config struct {
	filePath         string
	IsReloaded       chan bool
	Elasticsearch    map[string]ELasticsearchDriver    `yaml:"elasticsearchv1"`
	Mysql            map[string]MysqlDriver            `yaml:"mysql"`
	RabbitMQConsumer map[string]RabbitMQConsumerDriver `yaml:"rabbitmq-consumer"`
	RabbitMQProducer map[string]RabbitMQProducerDriver `yaml:"rabbitmq-producer"`
}

var synchro sync.Once
var config *Config

func NewConfig(configFile string) *Config {
	synchro.Do(func() {
		config = getConfig(configFile)
		config.IsReloaded = make(chan bool)
	})

	go func() {
		config.watchConfigFile()
	}()

	return config
}

func getConfig(configFile string) *Config {
	filename, _ := filepath.Abs(configFile)
	yamlFile, err := ioutil.ReadFile(filename)

	if err != nil {
		panic(err)
	}

	parsedConf := make(map[string]*Config)
	err = yaml.Unmarshal(yamlFile, &parsedConf)
	if err != nil {
		panic(err)
	}
	conf, errRead := parsedConf["drivers"]
	if !errRead {
		panic(errRead)
	}
	conf.filePath = configFile

	return conf
}

func (c *Config) watchConfigFile() {
	// Create watcher instance
	configFileWatcher, _ := fsnotify.NewWatcher()
	// Process events for config file and
	defer configFileWatcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case ev := <-configFileWatcher.Event:
				if ev.IsModify() {
					c.IsReloaded <- true
					log.Println("Modification du fichier de config, reload interne. ", ev)
					// Get new routes array hash
					config = getConfig(c.filePath)
					c.IsReloaded <- false
				}
			}
		}
	}()

	errWatcher := configFileWatcher.Watch(filepath.Dir(c.filePath))
	if errWatcher != nil {
		log.Println(errWatcher)
	}

	<-done
}
