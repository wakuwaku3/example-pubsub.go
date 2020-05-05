package aws

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type (
	// ProviderOption は Provider 生成時の オプションです
	ProviderOption struct {
		AWSAccessKey string
		AWSSecretKey string
		AWSRegion    string
	}

	client struct {
		opt               *ProviderOption
		session           *session.Session
		sns               *sns.SNS
		sqs               *sqs.SQS
		topics            *sync.Map
		topicsGettingLock chan int
		queues            *sync.Map
		queuesGettingLock chan int
	}
	// Client は aws へのアクセサです
	Client interface {
		GetTopicID(name string) (string, error)
		GetQueueID(name string) (string, error)
		Publish(args *PublishArgs) error
		SendMessage(args *SendMessageArgs) error
		ReceiveMessages(args *ReceiveMessagesArgs) (*ReceiveMessagesResult, error)
		ReportFailureMessage(args *ReportFailureMessageArgs) error
		ReportSuccessMessage(args *ReportSuccessMessageArgs) error
	}
	// PublishArgs は Publish の引数です
	PublishArgs struct {
		TopicID string
		Subject string
		Message string
	}
	// SendMessageArgs は SendMessage の引数です
	SendMessageArgs struct {
		QueueID string
		Message string
	}
	// ReceiveMessagesArgs は ReceiveMessages の引数です
	ReceiveMessagesArgs struct {
		QueueID string
	}
	// ReceiveMessagesResult は ReceiveMessages の戻り値です
	ReceiveMessagesResult struct {
		ReceiveMessages []*ReceiveMessage
	}
	// ReceiveMessage です
	ReceiveMessage struct {
		MessageID     string
		ReceiptHandle string
		Body          *string
	}
	// ReportFailureMessageArgs は ReportFailureMessage の引数です
	ReportFailureMessageArgs struct {
		QueueID       string
		ReceiptHandle string
		WaitTime      int64
	}
	// ReportSuccessMessageArgs は ReportSuccessMessage の引数です
	ReportSuccessMessageArgs struct {
		QueueID       string
		ReceiptHandle string
	}
)

// NewClient はインスタンスを生成します
func NewClient(opt *ProviderOption) (Client, error) {
	if err := opt.valid(); err != nil {
		return nil, err
	}
	creds := credentials.NewStaticCredentials(opt.AWSAccessKey, opt.AWSSecretKey, "")
	session, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      &opt.AWSRegion,
	})
	if err != nil {
		return nil, err
	}
	return &client{
		opt,
		session,
		sns.New(session),
		sqs.New(session),
		&sync.Map{},
		make(chan int, 1),
		&sync.Map{},
		make(chan int, 1),
	}, nil
}

func (t *ProviderOption) valid() error {
	slice := make([]string, 0)
	if t.AWSAccessKey == "" {
		slice = append(slice, "AWSAccessKey is required.")
	}
	if t.AWSSecretKey == "" {
		slice = append(slice, "AWSSecretKey is required.")
	}
	if t.AWSRegion == "" {
		slice = append(slice, "AWSRegion is required.")
	}
	if len(slice) > 0 {
		return errors.New(strings.Join(slice, "\n"))
	}
	return nil
}

func (t *client) GetTopicID(name string) (string, error) {
	if value, ok := t.topics.Load(name); ok {
		return value.(string), nil
	}
	t.topicsGettingLock <- 1
	defer func() { <-t.topicsGettingLock }()
	if value, ok := t.topics.Load(name); ok {
		return value.(string), nil
	}

	var nextToken *string = nil
	first := true
	for nextToken != nil || first {
		resp, err := t.sns.ListTopics(&sns.ListTopicsInput{NextToken: nextToken})
		if err != nil {
			return "", err
		}
		for _, topic := range resp.Topics {
			arn := *topic.TopicArn
			slice := strings.Split(arn, ":")
			name := slice[len(slice)-1]
			t.topics.Store(name, arn)
		}
		first = false
		nextToken = resp.NextToken
	}

	if value, ok := t.topics.Load(name); ok {
		return value.(string), nil
	}
	return "", fmt.Errorf("There is no topic named %s", name)
}

func (t *client) GetQueueID(name string) (string, error) {
	if value, ok := t.queues.Load(name); ok {
		return value.(string), nil
	}
	t.queuesGettingLock <- 1
	defer func() { <-t.queuesGettingLock }()
	if value, ok := t.queues.Load(name); ok {
		return value.(string), nil
	}

	resp, err := t.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: &name})
	if err != nil {
		return "", err
	}
	t.queues.Store(name, *resp.QueueUrl)

	if value, ok := t.queues.Load(name); ok {
		return value.(string), nil
	}
	return "", fmt.Errorf("There is no queue named %s", name)
}

func (t *client) Publish(args *PublishArgs) error {
	_, err := t.sns.Publish(&sns.PublishInput{
		TopicArn: &args.TopicID,
		Subject:  &args.Subject,
		Message:  &args.Message,
	})
	return err
}

func (t *client) SendMessage(args *SendMessageArgs) error {
	_, err := t.sqs.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    &args.QueueID,
		MessageBody: &args.Message,
	})
	return err
}

func (t *client) ReceiveMessages(args *ReceiveMessagesArgs) (*ReceiveMessagesResult, error) {
	res, err := t.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: &args.QueueID,
		// 一度に取得する最大メッセージ数。最大でも10まで。
		MaxNumberOfMessages: aws.Int64(10),
		// これでキューが空の場合はロングポーリング(20秒間繋ぎっぱなし)になる。
		WaitTimeSeconds: aws.Int64(20),
	})
	if err != nil {
		return nil, err
	}

	result := &ReceiveMessagesResult{
		ReceiveMessages: make([]*ReceiveMessage, len(res.Messages)),
	}
	for i, msg := range res.Messages {
		result.ReceiveMessages[i] = &ReceiveMessage{
			Body:          msg.Body,
			MessageID:     *msg.MessageId,
			ReceiptHandle: *msg.ReceiptHandle,
		}
	}
	return result, err
}

func (t *client) ReportFailureMessage(args *ReportFailureMessageArgs) error {
	_, err := t.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &args.QueueID,
		ReceiptHandle:     &args.ReceiptHandle,
		VisibilityTimeout: &args.WaitTime,
	})
	if err != nil {
		return err
	}

	return nil
}

func (t *client) ReportSuccessMessage(args *ReportSuccessMessageArgs) error {
	_, err := t.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &args.QueueID,
		ReceiptHandle: &args.ReceiptHandle,
	})
	if err != nil {
		return err
	}

	return nil
}
