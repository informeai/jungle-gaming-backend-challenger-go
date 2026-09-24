// Package sqs contains the AWS SQS adapters: the ingress consumer and the
// outbound event publisher.
package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

// NewClient builds an SQS client. Credentials come from the standard AWS
// chain (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY locally).
func NewClient(ctx context.Context, cfg config.AWS) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
	}), nil
}

// Queues holds the resolved queue URLs.
type Queues struct {
	Ingress string
	DLQ     string
	Events  string
}

// ResolveQueues looks up every queue URL, failing fast when one is missing.
func ResolveQueues(ctx context.Context, c *sqs.Client, cfg config.SQS) (Queues, error) {
	var q Queues
	for _, item := range []struct {
		name string
		dst  *string
	}{{cfg.IngressQueue, &q.Ingress}, {cfg.IngressDLQ, &q.DLQ}, {cfg.EventsQueue, &q.Events}} {
		out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(item.name)})
		if err != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", item.name, err)
		}
		*item.dst = aws.ToString(out.QueueUrl)
	}
	return q, nil
}

// Ping checks that the broker answers for the ingress queue (readiness).
func Ping(ctx context.Context, c *sqs.Client, queueURL string) error {
	_, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}
