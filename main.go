// Copyright (c) Alex Ellis 2017. All rights reserved.
// Copyright (c) OpenFaaS Project 2018. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/openfaas-incubator/connector-sdk/types"
	"github.com/pkg/errors"
)

var (
	saramaKafkaProtocolVersion = sarama.V0_10_2_0
	controller                 *types.Controller
)

type connectorConfig struct {
	*types.ControllerConfig
	Topics  *regexp.Regexp
	Brokers string
}

func main() {

	credentials := types.GetCredentials()
	configs := buildConnectorConfig()

	controller = types.NewController(credentials, configs.ControllerConfig)

	controller.BeginMapBuilder()

	brokers := []string{configs.Brokers}
	makeConsumer(brokers, configs.Topics)
}

func makeConsumer(brokers []string, topics *regexp.Regexp) {
	//setup consumer
	config := cluster.NewConfig()
	config.Version = saramaKafkaProtocolVersion
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest //OffsetOldest
	config.Group.Return.Notifications = true
	config.Group.Session.Timeout = 6 * time.Second
	config.Group.Heartbeat.Interval = 2 * time.Second
	config.Group.Topics.Whitelist = topics

	group := "faas-kafka-queue-workers"

	consumer, err := cluster.NewConsumer(brokers, group, nil, config)
	if err != nil {
		log.Fatalln("Fail to create Kafka consumer: ", err)
	}

	defer consumer.Close()

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Printf("Received on [%v,%v]: '%s'\n", msg.Topic, msg.Partition, string(msg.Value))
				if err = invoke(msg); err != nil {
					consumer.ResetOffset(msg, "to-earliest")
				} else {
					consumer.MarkOffset(msg, "") // mark message as processed
				}
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

	topics := regexp.MustCompile(`faas-topics.*`)
	if val, exists := os.LookupEnv("topics"); exists {
		topics = regexp.MustCompile(val)
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
		Brokers: broker,
		Topics:  topics,
	}
}

func invoke(message *sarama.ConsumerMessage) error {
	matchedFunctions := controller.TopicMap.Match(message.Topic)
	if len(matchedFunctions) < 1 {
		return errors.Errorf("topic %s has no matched functions", message.Topic)
	}
	controller.Invoke(message.Topic, &message.Value)
	return nil
}
