package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

type DatabaseInterface interface {
	CreateEmailLog(ctx context.Context, params CreateEmailLogParams) error
	CreateNotificationLog(ctx context.Context, params CreateNotificationLogParams) error
	Close() error
}

type Database struct {
	db      *sql.DB
	queries *Queries
}

func NewDatabase(databaseURL string) (*Database, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &Database{
		db:      db,
		queries: New(db),
	}, nil
}

func (d *Database) Close() error {
	return d.db.Close()
}

func (d *Database) CreateEmailLog(ctx context.Context, params CreateEmailLogParams) error {
	return d.queries.CreateEmailLog(ctx, params)
}

func (d *Database) CreateNotificationLog(ctx context.Context, params CreateNotificationLogParams) error {
	return d.queries.CreateNotificationLog(ctx, params)
}

type Queries struct {
	db DBTX
}

func New(db DBTX) *Queries {
	return &Queries{db: db}
}

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

type CreateEmailLogParams struct {
	MessageID string
	Recipient string
	Subject   string
	Status    string
	CreatedAt time.Time
}

type CreateNotificationLogParams struct {
	MessageID string
	UserID    string
	Type      string
	Content   string
	Status    string
	CreatedAt time.Time
}

type EmailLog struct {
	ID        int64     `json:"id"`
	MessageID string    `json:"message_id"`
	Recipient string    `json:"recipient"`
	Subject   string    `json:"subject"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type NotificationLog struct {
	ID        int64     `json:"id"`
	MessageID string    `json:"message_id"`
	UserID    string    `json:"user_id"`
	Type      string    `json:"type"`
	Content   string    `json:"content"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

const createEmailLog = `-- name: CreateEmailLog :exec
INSERT INTO email_logs (message_id, recipient, subject, status, created_at)
VALUES ($1, $2, $3, $4, $5)
`

func (q *Queries) CreateEmailLog(ctx context.Context, arg CreateEmailLogParams) error {
	_, err := q.db.ExecContext(ctx, createEmailLog,
		arg.MessageID,
		arg.Recipient,
		arg.Subject,
		arg.Status,
		arg.CreatedAt,
	)
	return err
}

const createNotificationLog = `-- name: CreateNotificationLog :exec
INSERT INTO notification_logs (message_id, user_id, type, content, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
`

func (q *Queries) CreateNotificationLog(ctx context.Context, arg CreateNotificationLogParams) error {
	_, err := q.db.ExecContext(ctx, createNotificationLog,
		arg.MessageID,
		arg.UserID,
		arg.Type,
		arg.Content,
		arg.Status,
		arg.CreatedAt,
	)
	return err
}

func (d *Database) IsMessageProcessed(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	err := d.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM processed_messages WHERE message_id = $1)",
		messageID,
	).Scan(&exists)
	return exists, err
}

func (d *Database) MarkMessageProcessed(ctx context.Context, messageID, messageType string) error {
	_, err := d.db.ExecContext(ctx,
		"INSERT INTO processed_messages (message_id, message_type, processed_at) VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING",
		messageID, messageType, time.Now(),
	)
	return err
}

func (d *Database) CleanupOldProcessedMessages(ctx context.Context, olderThan time.Duration) error {
	_, err := d.db.ExecContext(ctx,
		"DELETE FROM processed_messages WHERE processed_at < $1",
		time.Now().Add(-olderThan),
	)
	return err
}
