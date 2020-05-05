package sub

import (
	"errors"
	"fmt"
	"sync"

	"github.com/wakuwaku3/example-pubsub.go/aws"
)

type (
	subscriber struct {
		client   aws.Client
		handlers map[string]*struct {
			handler Handler
			option  *HandlerOption
		}
		option    *SubscriberOption
		semaphore chan int
	}
	// SubscriberOption は Subscriber の オプションです
	SubscriberOption struct {
		ConcurrencyMessageHandleLimit int
	}
	// HandlerOption は Handler の オプションです
	HandlerOption struct {
		WaitTime int64
	}
	// Subscriber は メッセージを購読します
	Subscriber interface {
		SetHandler(queueName string, handler Handler, option *HandlerOption) error
		Subscribe() error
	}
	// Handler です
	Handler func(id string, message *string) error
	// Middleware です
	Middleware func(next Handler) Handler
)

// NewSubscriber はインスタンスを生成します
func NewSubscriber(client aws.Client, option *SubscriberOption) (Subscriber, error) {
	if option.ConcurrencyMessageHandleLimit < 1 {
		return nil, errors.New("set 1 or more for ConcurrencyMessageHandleLimit")
	}
	return &subscriber{client, make(map[string]*struct {
		handler Handler
		option  *HandlerOption
	}), option, make(chan int, option.ConcurrencyMessageHandleLimit)}, nil
}

func (t *subscriber) SetHandler(queueName string, handler Handler, option *HandlerOption) error {
	if option.WaitTime < 0 {
		return errors.New("set 0 or more for WaitTime")
	}
	t.handlers[queueName] = &struct {
		handler Handler
		option  *HandlerOption
	}{
		handler: handler,
		option:  option,
	}
	return nil
}
func (t *subscriber) Subscribe() error {
	chFatal := make(chan error)
	go func() {
		defer func() {
			if info := recover(); info != nil {
				chFatal <- errors.New(fmt.Sprint(info))
			}
		}()
		for queueName, handler := range t.handlers {
			id, err := t.client.GetQueueID(queueName)
			if err != nil {
				chFatal <- err
				return
			}

			go func(queueName string, queueID string, handler *struct {
				handler Handler
				option  *HandlerOption
			}) {
				for true {
					res, err := t.client.ReceiveMessages(&aws.ReceiveMessagesArgs{
						QueueID: queueID,
					})
					if err != nil {
						chFatal <- err
						return
					}

					wgMessage := &sync.WaitGroup{}
					for _, msg := range res.ReceiveMessages {
						wgMessage.Add(1)
						go func(msg *aws.ReceiveMessage) {
							defer func() {
								if info := recover(); info != nil {
									chFatal <- errors.New(fmt.Sprint(info))
								}
								wgMessage.Done()
								<-t.semaphore
							}()
							t.semaphore <- 1
							// execute handler
							if err := handler.handler(msg.MessageID, msg.Body); err != nil {
								if err := t.client.ReportFailureMessage(&aws.ReportFailureMessageArgs{
									QueueID:       id,
									ReceiptHandle: msg.ReceiptHandle,
									WaitTime:      handler.option.WaitTime,
								}); err != nil {
									chFatal <- err
									return
								}
							} else if err := t.client.ReportSuccessMessage(&aws.ReportSuccessMessageArgs{
								QueueID:       id,
								ReceiptHandle: msg.ReceiptHandle,
							}); err != nil {
								chFatal <- err
								return
							}
						}(msg)
					}
					wgMessage.Wait()
				}
			}(queueName, id, handler)
		}
	}()

	return <-chFatal
}
