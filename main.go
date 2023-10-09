package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "user-tracking"
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	processor := simpleMessageLoggingProcessor{}
	brokers := []string{"localhost:9093"}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		panic(err)
	}

	partitionList, _ := consumer.Partitions(topic)

	exitChan := make(chan any, len(partitionList))

	go func() {
		for range done {
			log.Println("Signal received to stop consumers")
			for i := 0; i < len(partitionList); i++ {
				exitChan <- struct{}{}
			}
		}
	}()

	wg := sync.WaitGroup{}

	initialOffset := sarama.OffsetOldest
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			msgChannel := pc.Messages()
			for {
				select {
				case <-exitChan:
					return
				case msg := <-msgChannel:
					processor.execute(msg)
				}
			}
		}(pc)
		defer pc.Close()
	}

	wg.Wait()
}

type simpleMessageLoggingProcessor struct{}

func (*simpleMessageLoggingProcessor) execute(msg *sarama.ConsumerMessage) {
	log.Printf(
		"Message %s received from topic  %s, partition %d, offset %d\n",
		string(msg.Value),
		msg.Topic,
		msg.Partition,
		msg.Offset,
	)
}
