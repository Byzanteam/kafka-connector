// Copyright (c) Alex Ellis 2017. All rights reserved.
// Copyright (c) OpenFaaS Project 2018. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/openfaas-incubator/connector-sdk/types"
)

var saramaKafkaProtocolVersion = sarama.V0_10_2_0

var _kafkaClient sarama.Client

type connectorConfig struct {
	*types.ControllerConfig
	Topics []string
	Broker string
}

const (
	DEFAULT_KAFKA_PORT = "9092"
)

func main() {

	credentials := types.GetCredentials()
	config := buildConnectorConfig()

	controller := types.NewController(credentials, config.ControllerConfig)

	controller.BeginMapBuilder()

	brokers := []string{config.Broker}

	s := &subscriber{config, credentials, 1 * time.Second, brokers}
	controller.Subscribe(s)
	go s.syncFunctions() // sync remote functions to local.

	waitForBrokers(brokers, config, controller)

	makeConsumer(brokers, config, controller)
}

func waitForBrokers(brokers []string, config connectorConfig, controller *types.Controller) {

	var err error

	for {
		if len(controller.Topics()) > 0 {
			_kafkaClient, err = sarama.NewClient(brokers, nil)
			if _kafkaClient != nil && err == nil {
				break
			}
			fmt.Println("Wait for brokers ("+config.Broker+") to come up.. ", brokers)
		}
		time.Sleep(1 * time.Second)
	}
}

func makeConsumer(brokers []string, config connectorConfig, controller *types.Controller) {
	//setup consumer
	cConfig := cluster.NewConfig()
	cConfig.Version = saramaKafkaProtocolVersion
	cConfig.Consumer.Return.Errors = true
	cConfig.Consumer.Offsets.Initial = sarama.OffsetNewest //OffsetOldest
	cConfig.Group.Return.Notifications = true
	cConfig.Group.Session.Timeout = 6 * time.Second
	cConfig.Group.Heartbeat.Interval = 2 * time.Second

	group := "faas-kafka-queue-workers"

	topics := config.Topics
	log.Printf("Binding to topics: %v", config.Topics)

	consumer, err := cluster.NewConsumer(brokers, group, topics, cConfig)
	if err != nil {
		log.Fatalln("Fail to create Kafka consumer: ", err)
	}

	defer consumer.Close()

	num := 0

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				num = (num + 1) % math.MaxInt32
				fmt.Printf("[#%d] Received on [%v,%v]: '%s'\n",
					num,
					msg.Topic,
					msg.Partition,
					string(msg.Value))

				controller.Invoke(msg.Topic, &msg.Value)

				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case err = <-consumer.Errors():

			fmt.Println("consumer error: ", err)

		case ntf := <-consumer.Notifications():

			fmt.Printf("Rebalanced: %+v\n", ntf)

		}
	}
}

func buildConnectorConfig() connectorConfig {

	broker := "kafka:9092"
	if val, exists := os.LookupEnv("broker_host"); exists {
		broker = val
	}

	topics := []string{}
	if val, exists := os.LookupEnv("topics"); exists {
		for _, topic := range strings.Split(val, ",") {
			if len(topic) > 0 {
				topics = append(topics, topic)
			}
		}
	}
	if len(topics) == 0 {
		log.Fatal(`Provide a list of topics i.e. topics="payment_published,slack_joined"`)
	}

	gatewayURL := "http://gateway:8080"
	if val, exists := os.LookupEnv("gateway_url"); exists {
		gatewayURL = val
	}

	upstreamTimeout := time.Second * 30
	rebuildInterval := time.Second * 3

	if val, exists := os.LookupEnv("upstream_timeout"); exists {
		parsedVal, err := time.ParseDuration(val)
		if err == nil {
			upstreamTimeout = parsedVal
		}
	}

	if val, exists := os.LookupEnv("rebuild_interval"); exists {
		parsedVal, err := time.ParseDuration(val)
		if err == nil {
			rebuildInterval = parsedVal
		}
	}

	printResponse := false
	if val, exists := os.LookupEnv("print_response"); exists {
		printResponse = (val == "1" || val == "true")
	}

	printResponseBody := false
	if val, exists := os.LookupEnv("print_response_body"); exists {
		printResponseBody = (val == "1" || val == "true")
	}

	delimiter := ","
	if val, exists := os.LookupEnv("topic_delimiter"); exists {
		if len(val) > 0 {
			delimiter = val
		}
	}

	asynchronousInvocation := false
	if val, exists := os.LookupEnv("asynchronous_invocation"); exists {
		asynchronousInvocation = (val == "1" || val == "true")
	}

	return connectorConfig{
		ControllerConfig: &types.ControllerConfig{
			UpstreamTimeout:          upstreamTimeout,
			GatewayURL:               gatewayURL,
			PrintResponse:            printResponse,
			PrintResponseBody:        printResponseBody,
			RebuildInterval:          rebuildInterval,
			TopicAnnotationDelimiter: delimiter,
			AsyncFunctionInvocation:  asynchronousInvocation,
		},
		Topics: topics,
		Broker: broker,
	}
}
