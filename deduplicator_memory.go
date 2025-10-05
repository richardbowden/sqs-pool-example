package main

import (
	"context"
	"sync"
	"time"
)

type processedMessage struct {
	messageType string
	processedAt time.Time
}

type InMemoryDeduplicationStore struct {
	mu        sync.RWMutex
	processed map[string]processedMessage
}

func NewInMemoryDeduplicationStore() *InMemoryDeduplicationStore {
	return &InMemoryDeduplicationStore{
		processed: make(map[string]processedMessage),
	}
}

func (m *InMemoryDeduplicationStore) IsProcessed(ctx context.Context, messageID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.processed[messageID]
	return exists, nil
}

func (m *InMemoryDeduplicationStore) MarkProcessed(ctx context.Context, messageID, messageType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.processed[messageID] = processedMessage{
		messageType: messageType,
		processedAt: time.Now(),
	}
	return nil
}

func (m *InMemoryDeduplicationStore) Cleanup(ctx context.Context, olderThan time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	for id, msg := range m.processed {
		if msg.processedAt.Before(cutoff) {
			delete(m.processed, id)
		}
	}
	return nil
}

func (m *InMemoryDeduplicationStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.processed = nil
	return nil
}
