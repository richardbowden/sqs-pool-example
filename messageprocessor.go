package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/rs/zerolog/log"
)

// handles different types of messages
type MessageProcessor struct {
	config     ProcessorConfig
	sqsClient  SQSClientInterface
	db         DatabaseInterface
	dedupStore DeduplicationStore
	workers    map[string]*WorkerPool
	ctx        context.Context
	cancel     context.CancelFunc
	quiet      bool // only logs stats and metrics
}

func NewMessageProcessor(awsConfig aws.Config, config ProcessorConfig, db DatabaseInterface, dedupStore DeduplicationStore, quiet bool) (*MessageProcessor, error) {
	ctx, cancel := context.WithCancel(context.Background())

	processor := &MessageProcessor{
		config:     config,
		sqsClient:  sqs.NewFromConfig(awsConfig),
		db:         db,
		dedupStore: dedupStore,
		workers:    make(map[string]*WorkerPool),
		ctx:        ctx,
		cancel:     cancel,
		quiet:      quiet,
	}

	// setup worker pools for each message type
	for messageType, workerCount := range config.WorkerCounts {
		pool := &WorkerPool{
			messageType: messageType,
			workerCount: workerCount,
			jobs:        make(chan *WorkerJob, workerCount*2),
			processor:   processor,
		}
		processor.workers[messageType] = pool
	}

	return processor, nil
}

func (mp *MessageProcessor) Start() {

	for messageType, pool := range mp.workers {
		log.Info().Str("message_type", messageType).Int("workers", pool.workerCount).Msg("Starting worker pool")
		pool.Start(mp.ctx)
	}

	go mp.monitorWorkerPools()
	go mp.monitorQueueStats()
	go mp.cleanupDeduplicationStore()

	mp.pollSQS()
}

func (mp *MessageProcessor) Stop() {
	log.Info().Msg("Stopping message processor")
	mp.cancel()

	for _, pool := range mp.workers {
		pool.Stop()
	}
	if err := mp.dedupStore.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close deduplication store")
	}
}

func (mp *MessageProcessor) cleanupDeduplicationStore() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := mp.dedupStore.Cleanup(mp.ctx, 7*24*time.Hour); err != nil {
				log.Error().Err(err).Msg("Failed to cleanup deduplication store")
			} else {
				log.Debug().Msg("Cleaned up old deduplication entries")
			}
		case <-mp.ctx.Done():
			return
		}
	}
}

func (mp *MessageProcessor) monitorWorkerPools() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for messageType, pool := range mp.workers {
				queueDepth := len(pool.jobs)
				queueCapacity := cap(pool.jobs)
				utilization := float64(queueDepth) / float64(queueCapacity) * 100

				log.Info().
					Str("message_type", messageType).
					Int("workers", pool.workerCount).
					Int("queue_depth", queueDepth).
					Int("queue_capacity", queueCapacity).
					Float64("utilization_pct", utilization).
					Msg("Worker pool metrics")

				if utilization > 80 {
					log.Warn().
						Str("message_type", messageType).
						Float64("utilization_pct", utilization).
						Msg("Worker pool utilization high - consider increasing workers")
				}
			}
		case <-mp.ctx.Done():
			return
		}
	}
}

func (mp *MessageProcessor) monitorQueueStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mp.logQueueStats()
		case <-mp.ctx.Done():
			return
		}
	}
}

func (mp *MessageProcessor) logQueueStats() {
	result, err := mp.sqsClient.GetQueueAttributes(mp.ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(mp.config.QueueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})

	if err != nil {
		log.Error().Err(err).Msg("Failed to fetch queue stats")
		return
	}

	available := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	inFlight := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]
	delayed := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)]

	log.Info().
		Str("available", available).
		Str("in_flight", inFlight).
		Str("delayed", delayed).
		Msg("SQS queue stats")
}

func (mp *MessageProcessor) pollSQS() {
	consecutiveFullCount := 0

	for {
		select {
		case <-mp.ctx.Done():
			return
		default:
			if consecutiveFullCount > 3 {
				log.Warn().Msg("Worker pools consistently full, throttling SQS polling for 5 seconds")
				time.Sleep(5 * time.Second)
				consecutiveFullCount = 0
			}

			result, err := mp.sqsClient.ReceiveMessage(mp.ctx, &sqs.ReceiveMessageInput{
				QueueUrl:              aws.String(mp.config.QueueURL),
				MaxNumberOfMessages:   10,
				WaitTimeSeconds:       20, // Long polling
				MessageAttributeNames: []string{"All"},
			})

			if err != nil {
				log.Error().Err(err).Msg("Failed to receive messages from SQS")
				time.Sleep(5 * time.Second)
				continue
			}

			if len(result.Messages) == 0 {
				consecutiveFullCount = 0
				continue
			}

			log.Debug().Int("count", len(result.Messages)).Msg("Received messages from SQS")

			anyPoolFull := false
			for _, sqsMsg := range result.Messages {
				wasFull := mp.processMessage(sqsMsg)
				if wasFull {
					anyPoolFull = true
				}
			}

			if anyPoolFull {
				consecutiveFullCount++
			} else {
				consecutiveFullCount = 0
			}
		}
	}
}

func (mp *MessageProcessor) processMessage(sqsMsg types.Message) bool {
	var msg Message
	if err := json.Unmarshal([]byte(*sqsMsg.Body), &msg); err != nil {
		log.Error().Err(err).Msg("Failed to parse message")
		mp.deleteMessage(sqsMsg)
		return false
	}

	// get correct worker pool, if one type is going to be heavy or used more than others, would make this a seperate dedicated processor for that type
	// or have thes funcs and fields on the stuct as to not pay the penalty for calc the hash every time to find the pool from the map
	pool, exists := mp.workers[msg.Type]
	if !exists {
		log.Warn().Str("message_type", msg.Type).Msg("No worker pool for message type")
		mp.deleteMessage(sqsMsg)
		return false
	}

	job := &WorkerJob{
		Message:       &msg,
		ReceiptHandle: sqsMsg.ReceiptHandle,
	}

	select {
	case pool.jobs <- job:
		// worker will delete after processing
		log.Debug().Str("message_id", msg.ID).Str("message_type", msg.Type).Msg("Message queued for processing")
		return false
	case <-mp.ctx.Done():
		return false
	case <-time.After(2 * time.Second):
		//pool is full/slow - extend visibility timeout and skip for now
		log.Warn().
			Str("message_type", msg.Type).
			Int("queue_depth", len(pool.jobs)).
			Int("queue_capacity", cap(pool.jobs)).
			Msg("Worker pool full")

		mp.extendVisibilityTimeout(sqsMsg, 60)
		// let SQS retry later
		return true
	}
}

func (mp *MessageProcessor) deleteMessage(sqsMsg types.Message) {
	mp.deleteMessageByReceiptHandle(sqsMsg.ReceiptHandle, sqsMsg.MessageId)
}

func (mp *MessageProcessor) deleteMessageByReceiptHandle(receiptHandle *string, messageID *string) {
	_, err := mp.sqsClient.DeleteMessage(mp.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(mp.config.QueueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		log.Error().Str("messageID", *messageID).Err(err).Msg("Failed to delete message from SQS")
	} else {
		log.Debug().Str("messageID", *messageID).Msg("Message deleted from SQS")
	}
}

func (mp *MessageProcessor) extendVisibilityTimeout(sqsMsg types.Message, seconds int32) {
	_, err := mp.sqsClient.ChangeMessageVisibility(mp.ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(mp.config.QueueURL),
		ReceiptHandle:     sqsMsg.ReceiptHandle,
		VisibilityTimeout: seconds,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to extend visibility timeout")
	} else {
		log.Debug().Int32("seconds", seconds).Msg("Extended message visibility timeout")
	}
}

func (mp *MessageProcessor) handleEmailMessage(ctx context.Context, msg *Message) bool {
	startTime := time.Now()
	el := log.With().Str("handler", "email").Str("message_id", msg.ID).Logger()

	defer func() {
		duration := time.Since(startTime)
		el.Debug().Dur("duration", duration).Msg("Email processing complete")
	}()

	recipient, ok := msg.Data["recipient"].(string)
	if !ok {
		el.Error().Msg("Invalid email message: missing recipient")
		return false
	}

	subject, ok := msg.Data["subject"].(string)
	if !ok {
		el.Error().Msg("Invalid email message: missing subject")
		return false
	}

	processingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// simulate email sending
	el.Debug().
		Str("recipient", recipient).
		Str("subject", subject).
		Msg("Sending email")

	select {
	case <-time.After(1 * time.Second):
		// Success
	case <-processingCtx.Done():
		el.Error().Msg("Email processing timed out")
		return false
	}

	err := mp.db.CreateEmailLog(processingCtx, CreateEmailLogParams{
		MessageID: msg.ID,
		Recipient: recipient,
		Subject:   subject,
		Status:    "sent",
		CreatedAt: time.Now(),
	})

	if err != nil {
		el.Error().Err(err).Msg("Failed to save email log")
		return false
	}

	if mp.quiet {
		el.Debug().Str("recipient", recipient).Msg("Email sent successfully")
	} else {
		el.Info().Str("recipient", recipient).Msg("Email sent successfully")
	}
	return true
}

func (mp *MessageProcessor) handleNotificationMessage(ctx context.Context, msg *Message) bool {
	startTime := time.Now()
	nl := log.With().Str("handler", "notification").Str("message_id", msg.ID).Logger()
	defer func() {
		duration := time.Since(startTime)
		nl.Debug().Dur("duration", duration).Msg("Notification processing complete")
	}()

	userID, ok := msg.Data["user_id"].(string)
	if !ok {
		nl.Error().Msg("Invalid notification message: missing user_id")
		return false
	}

	notificationType, ok := msg.Data["type"].(string)
	if !ok {
		nl.Error().Msg("Invalid notification message: missing type")
		return false
	}

	content, ok := msg.Data["content"].(string)
	if !ok {
		nl.Error().Msg("Invalid notification message: missing content")
		return false
	}

	processingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// simulate notification sending
	nl.Debug().
		Str("user_id", userID).
		Str("type", notificationType).
		Str("content", content).
		Msg("Sending notification")

	st := time.Duration(rand.Intn(5-1) + 1*int(time.Second))

	select {
	case <-time.After(st):
		// Success
	case <-processingCtx.Done():
		nl.Error().Msg("Notification processing timed out")
		return false
	}

	err := mp.db.CreateNotificationLog(processingCtx, CreateNotificationLogParams{
		MessageID: msg.ID,
		UserID:    userID,
		Type:      notificationType,
		Content:   content,
		Status:    "delivered",
		CreatedAt: time.Now(),
	})

	if err != nil {
		nl.Error().Err(err).Msg("Failed to save notification log")
		return false
	}

	if mp.quiet {
		nl.Debug().Str("user_id", userID).Str("type", notificationType).Msg("Notification sent successfully")
	} else {
		nl.Info().Str("user_id", userID).Str("type", notificationType).Msg("Notification sent successfully")
	}
	return true
}
