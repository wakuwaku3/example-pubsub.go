package main

import (
	"errors"
	"log"
	"os"
	"sync"

	"github.com/wakuwaku3/example-pubsub.go/aws"
	"github.com/wakuwaku3/example-pubsub.go/pub"
	"github.com/wakuwaku3/example-pubsub.go/sub"
)

func main() {
	client, err := aws.NewClient(&aws.ProviderOption{
		AWSAccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWSRegion:    os.Getenv("AWS_DEFAULT_REGION"),
	})
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	awsPrefix := os.Getenv("AWS_PREFIX")

	subscriber := sub.NewSubscriber(client)
	subscriber.SetHandler(awsPrefix+"Queue1", handleQueue1, &sub.HandlerOption{WaitTime: 0})
	subscriber.SetHandler(awsPrefix+"Queue2", handleQueue2, &sub.HandlerOption{WaitTime: 0})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if err := subscriber.Subscribe(); err != nil {
			log.Print(err)
			os.Exit(1)
		}
		wg.Done()
	}()

	publisher := pub.NewPublisher(client)

	if err := publisher.Publish(awsPrefix+"SNSTopic", map[string]interface{}{
		"title": "test-title",
		"body":  "test-body",
	}); err != nil {
		log.Print(err)
		os.Exit(1)
	}

	if err := publisher.SendMessage(awsPrefix+"Queue1", map[string]interface{}{
		"title": "test-title2",
		"body":  "test-body2",
	}); err != nil {
		log.Print(err)
		os.Exit(1)
	}

	wg.Wait()
}

func handleQueue1(id string, message *string) error {
	log.Print("handleQueue1", id, *message)
	return errors.New("error handleQueue1")
}
func handleQueue2(id string, message *string) error {
	log.Print("handleQueue2", id, *message)
	return nil
}
