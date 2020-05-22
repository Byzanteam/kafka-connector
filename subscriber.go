package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/openfaas/faas-provider/auth"
	"github.com/openfaas/faas/gateway/requests"
	"github.com/pkg/errors"

	"github.com/openfaas-incubator/connector-sdk/types"
)

var (
	_functions = map[string]requests.Function{}
	_mutex     = sync.Mutex{}
	transport  = &http.Transport{}
	_client    = &http.Client{
		Transport: transport,
	}
)

type subscriber struct {
	config      connectorConfig
	credentials *auth.BasicAuthCredentials
	interval    time.Duration
	brokers     []string
}

func (s *subscriber) Response(response types.InvokerResponse) {
	funcInfo, ok := _functions[response.Function]
	if !ok || response.Body == nil {
		fmt.Printf("ignore response of function %v\n", response.Function)
		return
	}
	writer, err := getWriter(funcInfo)
	if err != nil {
		fmt.Printf("ignore response of function %s, err: %v\n", response.Function, err)
		return
	}

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 2
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(s.brokers, config)
	if err != nil {
		fmt.Printf("ignore response of function %s, err: %v\n", response.Function, err)
		return
	}
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: writer,
		Value: sarama.StringEncoder(string(*response.Body)),
	})
	if err != nil {
		fmt.Printf("function %s write to kafka err: %v\n", response.Function, err)
		return
	}
	fmt.Printf("function %s success write to kafka topic %s\n", response.Function, writer)
}

func (s *subscriber) syncFunctions() {
	ticker := time.NewTicker(s.interval)
	for {
		select {
		case <-ticker.C:
			funcs, err := s.fetchFunctions()
			if err != nil {
				fmt.Printf("Error when fetching functions %v\n", err)
				continue
			}
			_mutex.Lock()
			_functions = funcs
			_mutex.Unlock()
		}
	}
}

func (s *subscriber) fetchFunctions() (map[string]requests.Function, error) {
	var err error

	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/system/functions", s.config.GatewayURL), nil)

	if s.credentials != nil {
		req.SetBasicAuth(s.credentials.User, s.credentials.Password)
	}

	res, reqErr := _client.Do(req)

	if reqErr != nil {
		return nil, reqErr
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	bytesOut, _ := ioutil.ReadAll(res.Body)

	functions := []requests.Function{}
	marshalErr := json.Unmarshal(bytesOut, &functions)

	if marshalErr != nil {
		return nil, errors.Wrap(marshalErr, fmt.Sprintf("unable to unmarshal value: %q", string(bytesOut)))
	}
	result := make(map[string]requests.Function, len(functions))
	for index := range functions {
		result[functions[index].Name] = functions[index]
	}
	return result, err
}

func getWriter(f requests.Function) (string, error) {
	var writer string
	if f.Annotations == nil {
		return "", errors.New("function has no annotations")
	}
	for key, value := range *f.Annotations {
		if key == "writer" {
			writer = value
			return writer, nil
		}
	}
	return "", errors.New("function has no writer")
}
