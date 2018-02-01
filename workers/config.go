package worker

import (
	"io/ioutil"
	"path/filepath"
	yaml "gopkg.in/yaml.v2"
	"sync"
)

type ELasticsearchDriver struct {
	Domains []string `yaml:"domains"`
}

type MysqlDriver struct {
	Host string `yaml:"host"`
	Login string `yaml:"login"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Port int `yaml:"port"`
	Protocol string `yaml:"protocol"`
}

type RabbitMQDriver struct {
	Uri string `yaml:"uri"`
	Exchange string `yaml:"exchange"`
	ExchangeType string `yaml:"exchangeType"`
	QueueName string `yaml:"queueName"`
	Key string `yaml:"key"`
	Ctag string `yaml:"ctag"`
}

type Config struct {
	Elasticsearch      map[string]ELasticsearchDriver `yaml:"elasticsearchv1"`
	Mysql      map[string]MysqlDriver `yaml:"mysql"`
	RabbitMQ      map[string]MysqlDriver `yaml:"mysql"`
}

var synchro sync.Once
var config *Config

func NewConfig(configFile string) *Config {
	synchro.Do(func() {
		config = config.GetConfig(configFile)

	})

	return config
}


func (config *Config) GetConfig(configFile string) *Config {
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
	confList, errRead := parsedConf["drivers"]
	if !errRead {
		panic(errRead)
	}
	return confList
}