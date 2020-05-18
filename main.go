// Copyright (c) Alex Ellis 2017. All rights reserved.
// Copyright (c) OpenFaaS Project 2018. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/openfaas-incubator/connector-sdk/types"
)

var (
	saramaKafkaProtocolVersion = sarama.V0_10_2_0
)

type connectorConfig struct {
	*types.ControllerConfig
	Broker string
}

func main() {

	credentials := types.GetCredentials()
	config := buildConnectorConfig()

	controller := types.NewController(credentials, config.ControllerConfig)

	controller.BeginMapBuilder()

	brokers := []string{config.Broker}

	makeConsumer(brokers, controller)
}

func makeConsumer(brokers []string, controller *types.Controller) {
	//setup consumer
	cConfig := cluster.NewConfig()
	cConfig.Version = saramaKafkaProtocolVersion
	cConfig.Consumer.Return.Errors = true
	cConfig.Consumer.Offsets.Initial = sarama.OffsetNewest //OffsetOldest
	cConfig.Group.Return.Notifications = true
	cConfig.Group.Session.Timeout = 6 * time.Second
	cConfig.Group.Heartbeat.Interval = 2 * time.Second

	cConfig.Group.Topics.Whitelist = regexp.MustCompile(`faas-topics.*`)

	group := "faas-kafka-queue-workers"

	consumer, err := cluster.NewConsumer(brokers, group, nil, cConfig)
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
		Broker: broker,
	}
}
