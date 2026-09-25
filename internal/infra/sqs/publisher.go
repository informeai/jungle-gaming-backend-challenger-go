package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/resilience"
)

// EventPublisher sends outbox events to the wallet-events FIFO queue.
//
// Routing contract:
//   - MessageGroupId = partition key (walletId): per-wallet ordering.
//   - MessageDeduplicationId = eventId: republishing within SQS's 5-minute
//     window is dropped by the broker; after it, consumers deduplicate by
//     eventId (the id never changes between attempts).
//   - Attributes eventType and correlationId allow filtering without parsing.
type EventPublisher struct {
	client   *sqs.Client
	queueURL string
	// Breaker guards SendMessage; nil disables it.
	Breaker *resilience.Breaker
}

func NewEventPublisher(client *sqs.Client, queues Queues) *EventPublisher {
	return &EventPublisher{client: client, queueURL: queues.Events}
}

func (p *EventPublisher) Publish(ctx context.Context, m postgres.OutboxMessage) error {
	return p.Breaker.Do(func() error { return p.send(ctx, m) })
}

func (p *EventPublisher) send(ctx context.Context, m postgres.OutboxMessage) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(m.Payload)),
		MessageGroupId:         aws.String(m.PartitionKey.String()),
		MessageDeduplicationId: aws.String(m.ID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType":     {DataType: aws.String("String"), StringValue: aws.String(m.EventType)},
			"eventId":       {DataType: aws.String("String"), StringValue: aws.String(m.ID.String())},
			"correlationId": {DataType: aws.String("String"), StringValue: aws.String(m.CorrelationID)},
		},
	})
	return err
}
