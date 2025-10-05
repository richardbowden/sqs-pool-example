package main

import (
	"context"
	"database/sql"
	"time"
)

type PostgresDeduplicationStore struct {
	db *sql.DB
}

func NewPostgresDeduplicationStore(db *sql.DB) *PostgresDeduplicationStore {
	return &PostgresDeduplicationStore{db: db}
}

func (p *PostgresDeduplicationStore) IsProcessed(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	err := p.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM processed_messages WHERE message_id = $1)",
		messageID,
	).Scan(&exists)
	return exists, err
}

func (p *PostgresDeduplicationStore) MarkProcessed(ctx context.Context, messageID, messageType string) error {
	_, err := p.db.ExecContext(ctx,
		`INSERT INTO processed_messages (message_id, message_type, processed_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (message_id) DO NOTHING`,
		messageID, messageType, time.Now(),
	)
	return err
}

func (p *PostgresDeduplicationStore) Cleanup(ctx context.Context, olderThan time.Duration) error {
	_, err := p.db.ExecContext(ctx,
		"DELETE FROM processed_messages WHERE processed_at < $1",
		time.Now().Add(-olderThan),
	)
	return err
}

func (p *PostgresDeduplicationStore) Close() error {
	// DB connection is managed elsewhere, nothing to close here
	return nil
}
