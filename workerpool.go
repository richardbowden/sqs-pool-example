package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
)

// manages workers for a specific message type
type WorkerPool struct {
	messageType string
	workerCount int
	jobs        chan *WorkerJob
	wg          sync.WaitGroup
	processor   *MessageProcessor
}

func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.jobs)
	wp.wg.Wait()
}

func (wp *WorkerPool) worker(ctx context.Context, workerID int) {
	defer wp.wg.Done()

	log.Debug().Str("message_type", wp.messageType).Int("worker_id", workerID).Msg("Worker started")

	for {
		select {
		case job, ok := <-wp.jobs:
			if !ok {
				log.Debug().Str("message_type", wp.messageType).Int("worker_id", workerID).Msg("Worker stopping")
				return
			}

			// recovery to prevent worker crashes
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Error().
							Str("message_type", wp.messageType).
							Int("worker_id", workerID).
							Interface("panic", r).
							Msg("Worker recovered from panic")
						// don't delete message on panic, let SQS retry
					}
				}()
				wp.handleMessage(ctx, job, workerID)
			}()

		case <-ctx.Done():
			return
		}
	}
}

func (wp *WorkerPool) handleMessage(ctx context.Context, job *WorkerJob, workerID int) {
	log.Debug().
		Int("worker_id", workerID).
		Str("message_type", wp.messageType).
		Str("message_id", job.Message.ID).
		Msg("Processing message")

	processed, err := wp.processor.dedupStore.IsProcessed(ctx, job.Message.ID)
	if err != nil {
		log.Error().Err(err).Str("message_id", job.Message.ID).Msg("Failed to check if message was processed")
		return
	}

	if processed {
		log.Info().Str("message_id", job.Message.ID).Msg("Duplicate message detected, skipping")
		wp.processor.deleteMessageByReceiptHandle(job.ReceiptHandle, &job.Message.ID)
		return
	}

	var success bool

	switch wp.messageType {
	case "email":
		success = wp.processor.handleEmailMessage(ctx, job.Message)
	case "notification":
		success = wp.processor.handleNotificationMessage(ctx, job.Message)
	default:
		log.Warn().Str("message_type", wp.messageType).Msg("Unknown message type")
		success = false
	}

	if success {
		if err := wp.processor.dedupStore.MarkProcessed(ctx, job.Message.ID, job.Message.Type); err != nil {
			log.Error().Err(err).Str("message_id", job.Message.ID).Msg("Failed to mark message as processed")
		}
		wp.processor.deleteMessageByReceiptHandle(job.ReceiptHandle, &job.Message.ID)
		log.Debug().Str("message_id", job.Message.ID).Msg("Message processed successfully")
	} else {
		log.Warn().Str("message_id", job.Message.ID).Msg("Message processing failed, will be retried by SQS")
	}
}
