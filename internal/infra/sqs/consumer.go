package sqs

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/contract"
)

// ConsumerMetrics is the instrumentation used by the consumer.
type ConsumerMetrics interface {
	SQSMessage(outcome string)
	SQSRetry()
	SQSDeadLetter(reason string)
}

// Consumer long-polls the ingress FIFO queue. Messages of the same
// MessageGroupId are handled sequentially (FIFO order); groups run in
// parallel up to the configured concurrency.
type Consumer struct {
	client  *sqs.Client
	queues  *Queues // resolved on start
	svc     *app.WageringService
	cfg     config.SQS
	metrics ConsumerMetrics
	log     *slog.Logger

	// AfterCommit, when set, runs after the durable commit and before the
	// message is deleted. Fault-injection hook for crash tests.
	AfterCommit func(messageID string)

	cancelPoll context.CancelFunc
	cancelWork context.CancelFunc
	loopDone   chan struct{}
	inflight   sync.WaitGroup
	mu         sync.Mutex
	pending    map[string]string // receipt handle by SQS message id (in flight)
}

func NewConsumer(client *sqs.Client, queues *Queues, svc *app.WageringService, cfg config.SQS, metrics ConsumerMetrics, log *slog.Logger) *Consumer {
	return &Consumer{client: client, queues: queues, svc: svc, cfg: cfg, metrics: metrics,
		log: log.With(slog.String("component", "sqs-consumer")), pending: map[string]string{}}
}

// Start begins polling. It returns immediately.
func (c *Consumer) Start() {
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	workCtx, cancelWork := context.WithCancel(context.Background())
	c.cancelPoll, c.cancelWork = cancelPoll, cancelWork
	c.loopDone = make(chan struct{})
	go c.loop(pollCtx, workCtx)
	c.log.Info("consumer started", slog.String("queue", c.cfg.IngressQueue))
}

// Stop stops fetching new messages, waits for in-flight handlers until ctx
// expires and then aborts them, releasing their visibility so another
// instance can take them over immediately.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancelPoll == nil {
		return nil
	}
	c.cancelPoll()
	<-c.loopDone
	done := make(chan struct{})
	go func() { c.inflight.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		c.cancelWork()
		<-done
	}
	c.cancelWork()
	c.releaseAll()
	c.log.Info("consumer stopped")
	return nil
}

func (c *Consumer) loop(pollCtx, workCtx context.Context) {
	defer close(c.loopDone)
	sem := make(chan struct{}, c.cfg.Concurrency)
	backoff := 200 * time.Millisecond
	for pollCtx.Err() == nil {
		out, err := c.client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queues.Ingress),
			MaxNumberOfMessages: c.cfg.MaxMessages,
			WaitTimeSeconds:     int32(c.cfg.WaitTime / time.Second),
			VisibilityTimeout:   int32(c.cfg.VisibilityTimeout / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			c.log.Warn("receive failed; broker unavailable?", slog.String("error", err.Error()))
			select {
			case <-pollCtx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = 200 * time.Millisecond
		groups := map[string][]types.Message{}
		var order []string
		for _, m := range out.Messages {
			g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
			if _, ok := groups[g]; !ok {
				order = append(order, g)
			}
			groups[g] = append(groups[g], m)
			c.track(m)
		}
		for _, g := range order {
			msgs := groups[g]
			select {
			case sem <- struct{}{}:
			case <-pollCtx.Done():
				// Not started: hand the messages back right away.
				for _, m := range msgs {
					c.release(m)
				}
				continue
			}
			c.inflight.Add(1)
			go func() {
				defer func() { <-sem; c.inflight.Done() }()
				blocked := false
				for _, m := range msgs {
					if blocked || workCtx.Err() != nil {
						// Keep FIFO order: later messages of a group wait for
						// the earlier one that is going to be retried.
						c.release(m)
						continue
					}
					blocked = !c.handle(workCtx, m)
				}
			}()
		}
	}
}

func (c *Consumer) track(m types.Message) {
	c.mu.Lock()
	c.pending[aws.ToString(m.MessageId)] = aws.ToString(m.ReceiptHandle)
	c.mu.Unlock()
}

func (c *Consumer) untrack(m types.Message) {
	c.mu.Lock()
	delete(c.pending, aws.ToString(m.MessageId))
	c.mu.Unlock()
}

func (c *Consumer) releaseAll() {
	c.mu.Lock()
	handles := make(map[string]string, len(c.pending))
	for k, v := range c.pending {
		handles[k] = v
	}
	c.mu.Unlock()
	for id, h := range handles {
		c.release(types.Message{MessageId: aws.String(id), ReceiptHandle: aws.String(h)})
	}
}

// release makes the message visible again immediately.
func (c *Consumer) release(m types.Message) {
	c.changeVisibility(m, 0)
	c.untrack(m)
}

func (c *Consumer) changeVisibility(m types.Message, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queues.Ingress), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(d / time.Second),
	}); err != nil {
		c.log.Warn("change visibility failed", slog.String("sqsMessageId", aws.ToString(m.MessageId)), slog.String("error", err.Error()))
	}
}

func (c *Consumer) delete(m types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queues.Ingress), ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		// Safe: the redelivery will be answered by the inbox as a duplicate.
		c.log.Warn("delete failed; message will be redelivered and deduplicated",
			slog.String("sqsMessageId", aws.ToString(m.MessageId)), slog.String("error", err.Error()))
	}
	c.untrack(m)
}

// deadLetter moves the message to the DLQ with the reason, then deletes it.
// It reports whether the message left the ingress queue.
func (c *Consumer) deadLetter(m types.Message, reason string, log *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "unknown"
	}
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(c.queues.DLQ), MessageBody: m.Body,
		MessageGroupId: aws.String(group), MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"dlqReason":    {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"sourceQueue":  {DataType: aws.String("String"), StringValue: aws.String(c.cfg.IngressQueue)},
			"receiveCount": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(receiveCount(m)))},
		},
	})
	if err != nil {
		// Leave it in the queue: the redrive policy is the safety net.
		log.Error("dead-letter send failed", slog.String("reason", reason), slog.String("error", err.Error()))
		c.untrack(m)
		return false
	}
	c.metrics.SQSDeadLetter(reason)
	log.Warn("message moved to DLQ", slog.String("reason", reason))
	c.delete(m)
	return true
}

func receiveCount(m types.Message) int {
	n, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	return max(n, 1)
}

func (c *Consumer) backoff(attempt int) time.Duration {
	d := c.cfg.RetryBaseDelay
	for i := 1; i < attempt && d < c.cfg.RetryMaxDelay; i++ {
		d *= 2
	}
	return min(d, c.cfg.RetryMaxDelay, 12*time.Hour)
}

// handle processes one message and reports whether it left the queue.
func (c *Consumer) handle(workCtx context.Context, m types.Message) bool {
	log := c.log.With(slog.String("sqsMessageId", aws.ToString(m.MessageId)))
	env, err := contract.DecodeEnvelope(aws.ToString(m.Body))
	if err != nil {
		c.metrics.SQSMessage("invalid")
		return c.deadLetter(m, "invalid_message", log.With(slog.String("error", err.Error())))
	}
	d := env.Data
	log = log.With(slog.String("messageId", env.MessageID), slog.String("providerId", d.ProviderID),
		slog.String("walletId", d.WalletID), slog.String("correlationId", env.MessageID))
	if !slices.Contains(c.cfg.AllowedProviders, d.ProviderID) {
		c.metrics.SQSMessage("unauthorized")
		return c.deadLetter(m, "unauthorized_provider", log)
	}
	causation := env.MessageID
	cmd, err := app.ParseSubmit(d.Input(d.IdempotencyKey, env.MessageID, &causation))
	if err != nil {
		c.metrics.SQSMessage("invalid")
		code, _ := contract.ValidationCode(err)
		return c.deadLetter(m, "validation:"+code, log)
	}

	ctx, cancel := context.WithTimeout(workCtx, c.cfg.HandlerTimeout)
	res, err := c.svc.SubmitMessage(ctx, c.cfg.ConsumerName, env.MessageID, cmd)
	cancel()
	switch {
	case err == nil:
		if c.AfterCommit != nil {
			c.AfterCommit(env.MessageID)
		}
		outcome := "processed"
		if res.DuplicateMessage {
			outcome = "duplicate"
		} else if res.Tx != nil {
			outcome = string(res.Tx.Status())
		}
		c.metrics.SQSMessage(outcome)
		c.delete(m)
		return true
	case errors.Is(err, app.ErrWalletNotFound):
		c.metrics.SQSMessage("rejected_input")
		return c.deadLetter(m, "wallet_not_found", log)
	case errors.Is(err, app.ErrIdempotencyConflict), errors.Is(err, app.ErrExternalIDConflict):
		c.metrics.SQSMessage("conflict")
		return c.deadLetter(m, "idempotency_conflict", log)
	case errors.Is(err, app.ErrInboxPayloadMismatch):
		c.metrics.SQSMessage("conflict")
		return c.deadLetter(m, "message_hash_mismatch", log)
	case app.IsValidation(err):
		c.metrics.SQSMessage("invalid")
		return c.deadLetter(m, "validation", log)
	case workCtx.Err() != nil:
		// Shutting down: nothing was committed; hand it back immediately.
		c.release(m)
		return false
	default:
		n := receiveCount(m)
		if n >= c.cfg.MaxReceives {
			c.metrics.SQSMessage("failed")
			return c.deadLetter(m, "retries_exhausted", log.With(slog.String("error", err.Error())))
		}
		delay := c.backoff(n)
		c.metrics.SQSRetry()
		log.Warn("transient failure; message will be retried", slog.String("error", err.Error()),
			slog.Int("receiveCount", n), slog.Duration("retryIn", delay))
		c.changeVisibility(m, delay)
		c.untrack(m)
		return false
	}
}
