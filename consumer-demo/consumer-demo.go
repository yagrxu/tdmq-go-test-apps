package main

import (
	"context"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/ilyakaznacheev/cleanenv"
)

type PulsarConfig struct {
	Cluster struct {
		Url   string `yaml:"url" env:"URL"  env-description:"pulsar cluster url"`
		Token string `yaml:"token" env:"TOKEN"  env-description:"pulsar cluster UAA token"`
	} `yaml:"cluster"`
	Consumer struct {
		Topic string `yaml:"topic" env:"TOPIC"  env-description:"pulsar topic to connect"`
	} `yaml:"consumer"`
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
		URL:                     cfg.Cluster.Url,
		Authentication:          pulsar.NewAuthenticationToken(cfg.Cluster.Token),
		MaxConnectionsPerBroker: 10,
		OperationTimeout:        30 * time.Second,
		ConnectionTimeout:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            cfg.Consumer.Topic,
		SubscriptionName: uuid.New().String(),
	})

	if err != nil {
		log.Fatal(err)
	}
	for {
		message, error := consumer.Receive(context.Background())

		if error != nil {
			log.Fatalf("pulsar channel closed")
		}
		consumer.AckID(message.ID())
		val := string(message.Payload()[:])
		log.Printf("recv %s from pulsar\n", val)
	}
}
