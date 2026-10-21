package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	QueueTypeDurable   SimpleQueueType = "durable"
	QueueTypeTransient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshaling failed: %v", err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBytes,
		},
	)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, *amqp.Queue, error) {
	chn, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	var (
		durable    bool = queueType == QueueTypeDurable
		autoDelete bool = queueType == QueueTypeTransient
		exclusive  bool = queueType == QueueTypeTransient
	)

	q, err := chn.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return nil, &q, err
	}

	if err := chn.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, &q, err
	}

	return chn, &q, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	boundchn, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	deliveryChan, err := boundchn.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume channel: %w", err)
	}

	go func() {
		for delivery := range deliveryChan {
			var message T
			if err := json.Unmarshal(delivery.Body, &message); err != nil {
				fmt.Println("unmarshal error: %v", err)
			}
			handler(message)
			delivery.Ack(false)
		}
	}()

	return nil
}
