package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ilyakaznacheev/cleanenv"
)

type PulsarConfig struct {
	Cluster struct {
		Url   string `yaml:"url" env:"URL"  env-description:"pulsar cluster url"`
		Token string `yaml:"token" env:"TOKEN"  env-description:"pulsar cluster UAA token"`
	} `yaml:"cluster"`
	Producer struct {
		Topic string `yaml:"topic" env:"TOPIC"  env-description:"pulsar topic to connect"`
	} `yaml:"producer"`
}

var cfg PulsarConfig

func Init() {
	err := cleanenv.ReadConfig("../config.yaml", &cfg)

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	Init()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            cfg.Cluster.Url,
		Authentication: pulsar.NewAuthenticationToken(cfg.Cluster.Token),
		//MaxConnectionsPerBroker: 10,
		//OperationTimeout:        30 * time.Second,
		//ConnectionTimeout:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: cfg.Producer.Topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("hello" + strconv.Itoa(i)),
		})
	}

	fmt.Println("Published message")
	defer client.Close()
	defer producer.Close()

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
}
