package main

import (
	"context"
	"time"
)

// tracking processed messages
type DeduplicationStore interface {
	// checks if a message has already been processed
	IsProcessed(ctx context.Context, messageID string) (bool, error)

	// records that a message has been processed
	MarkProcessed(ctx context.Context, messageID, messageType string) error

	// removes old entries to prevent unbounded growth
	Cleanup(ctx context.Context, olderThan time.Duration) error

	// releases any resources, could be a noop if not required
	Close() error
}
