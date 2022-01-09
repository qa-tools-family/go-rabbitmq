package test

import (
	"fmt"
	"github.com/qa-tools-family/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
	"time"
)

var consumerName = "example"

func processor (d rabbitmq.Delivery) rabbitmq.Action {
	log.Printf("received: %v", string(d.Body))
	time.Sleep(10 * time.Second)
	log.Printf("consumed: %v", string(d.Body))
	// Ack, NackDiscard, NackRequeue
	return rabbitmq.Ack
}

func TestConsume(t *testing.T)  {
	t.Log("begin test")
	consumer, err := rabbitmq.NewConsumer(
		"amqp://user:password@localhost:5672/", amqp.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	err = consumer.StartConsuming(
		processor,
		"my_queue",
		[]string{"routing_key", "routing_key_2"},
		rabbitmq.WithConsumeOptionsConcurrency(10),
		//WithConsumeOptionsQueueDurable,
		//WithConsumeOptionsQuorum,
		rabbitmq.WithConsumeOptionsBindingExchangeName("events"),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
		rabbitmq.WithConsumeOptionsConsumerName(consumerName),
	)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan bool, 1)
	time.Sleep(1 * time.Second)
	done <- true
	fmt.Println("awaiting signal")
	<-done
	fmt.Println("stopping consumer")

	// wait for server to acknowledge the cancel
	consumer.StopConsuming(consumerName, false)
	// consumer 需要支持一个 Wait Task Finished 方法
	consumer.WaitMessageDown()
	consumer.Disconnect()
}