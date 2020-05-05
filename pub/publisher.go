package pub

import (
	"encoding/json"

	"github.com/wakuwaku3/example-pubsub.go/aws"
)

type (
	publisher struct {
		client aws.Client
	}
	// Publisher は メッセージを公開します
	Publisher interface {
		Publish(topicName string, obj interface{}) error
		SendMessage(queueName string, obj interface{}) error
	}
)

// NewPublisher はインスタンスを生成します
func NewPublisher(client aws.Client) Publisher {
	return &publisher{client}
}

func (t *publisher) Publish(topicName string, obj interface{}) error {
	messageBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	id, err := t.client.GetTopicID(topicName)
	if err != nil {
		return err
	}

	err = t.client.Publish(&aws.PublishArgs{
		Message: string(messageBytes),
		Subject: topicName,
		TopicID: id,
	})
	return err
}

func (t *publisher) SendMessage(queueName string, obj interface{}) error {
	messageBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	id, err := t.client.GetQueueID(queueName)
	if err != nil {
		return err
	}

	err = t.client.SendMessage(&aws.SendMessageArgs{
		Message: string(messageBytes),
		QueueID: id,
	})
	return err
}
