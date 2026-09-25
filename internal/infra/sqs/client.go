// Package sqs contains the AWS SQS adapters: the ingress consumer and the
// outbound event publisher.
package sqs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/resilience"
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

// ResolveQueues looks up the URLs of the queues a component uses, failing
// fast when one is missing: the consumer needs ingress + DLQ, the outbox relay
// only the events queue. Unused queues are never touched, so each component
// can run with credentials limited to its own queues.
func ResolveQueues(ctx context.Context, c *sqs.Client, cfg config.SQS, consumer, relay bool) (Queues, error) {
	var q Queues
	type item struct {
		name string
		dst  *string
	}
	var items []item
	if consumer {
		items = append(items, item{cfg.IngressQueue, &q.Ingress}, item{cfg.IngressDLQ, &q.DLQ})
	}
	if relay {
		items = append(items, item{cfg.EventsQueue, &q.Events})
	}
	for _, it := range items {
		out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(it.name)})
		if err != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", it.name, err)
		}
		*it.dst = aws.ToString(out.QueueUrl)
	}
	return q, nil
}

// Ping checks that the broker answers for the given queue (readiness).
func Ping(ctx context.Context, c *sqs.Client, queueURL string) error {
	_, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}

// IsUnavailable is the SQS breaker's failure predicate: network errors,
// timeouts and 5xx responses count; 4xx responses (bad request, missing
// queue, access denied) mean the broker answered and do not. The SDK wraps
// timeouts and connection failures in a ResponseError with status 0 (no
// response at all), which counts as unavailable.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		if re.ResponseError == nil || re.Response == nil || re.Response.Response == nil {
			return true // no response at all
		}
		code := re.Response.StatusCode
		return code == 0 || code >= 500
	}
	return !errors.Is(err, context.Canceled)
}

// NewBreaker builds the SQS circuit breaker.
func NewBreaker(cfg config.Breaker, obs resilience.Observer, log *slog.Logger) *resilience.Breaker {
	return resilience.New("sqs", resilience.Settings{
		FailureThreshold: cfg.FailureThreshold, OpenTimeout: cfg.OpenTimeout, IsFailure: IsUnavailable,
	}, obs, log)
}

// NewGate pauses background work while the SQS breaker is open; the
// half-open probe is a cheap GetQueueAttributes on queueURL.
func NewGate(b *resilience.Breaker, c *sqs.Client, queueURL func() string) *resilience.Gate {
	if b == nil {
		return nil
	}
	return resilience.NewGate(b, func(ctx context.Context) error { return Ping(ctx, c, queueURL()) })
}
