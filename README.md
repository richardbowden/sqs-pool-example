# SQS Message Processor

Example Go app that processes AWS SQS messages using worker pools. Logs everything to Postgres and handles deduplication.

## What it does

The processor polls an SQS queue and routes messages to different worker pools based on message type. Each pool runs configurable workers that process messages concurrently. When workers get backed up, it extends the SQS visibility timeout so messages don't get redelivered while still processing. Processed messages get logged to Postgres, and a deduplication layer prevents the same message from being processed twice.

Right now it handles two message types: emails and notifications. Both are simulated - they just sleep for a bit and write to the database.

## Getting started

You'll need Go 1.25+, Postgres, and an SQS queue amd direnv

**Do this first**
Copy `envrc_example` to `.envrc` and fill in your details

[direnv](https://direnv.net/), it'll load automatically .envrc for you

Run the database setup script:

```bash
chmod +x local_db_setup.sh
./local_db_setup.sh
```

This creates the database, user, and tables.

use .envrc for easier setup

All commands to be run from the root of the project to ensure env vars are available.

## How to run it

**Build**
```bash
go build

or

go build -race (enables race detection)
```

**Start the processor**
```bash
./sqspoolexample start
```

or set the correct info for database and sqs at the cmd line

to see all options

```bash
./sqspoolexample start --help
```

```bash
./sqspoolexample start \
  --queue-url="https://sqs.us-east-1.amazonaws.com/123456789/my-queue" \
  --db-url="postgres://user:pass@localhost/dbname?sslmode=disable" \
  --email-workers=10 \
  --notification-workers=10
```

## Generating Some messages

check out the `loadtester` directory

## Message format

Send JSON messages to your SQS queue:

```json
{
  "id": "unique-message-id",
  "type": "email",
  "data": {
    "recipient": "user@example.com",
    "subject": "Hello World"
  },
  "metadata": {
    "priority": "high"
  }
}
```

Email messages need `recipient` and `subject`. Notification messages need `user_id`, `type`, and `content`.

## Code layout

```
main.go                    CLI setup
messageprocessor.go        Polls SQS, routes to worker pools
workerpool.go              Manages workers for each message type
database.go                Database queries and schema
deduplicator.go            Interface for tracking processed messages
deduplicator_postgres.go   Postgres-backed deduplication
deduplicator_memory.go     In-memory deduplication (testing only)
types.go                   Message structs
```

The flow is: SQS → MessageProcessor → WorkerPool → Handler → Database. Deduplication happens before the handler runs.

Worker pools have their own job channels. If a channel fills up, the processor extends the message visibility timeout and lets SQS retry later. This prevents message loss when the system is overloaded.

## Testing

```bash
go test ./...
```

Tests use mocks for SQS and the database. The test file shows examples of message parsing, routing, worker pool behavior, and context cancellation.
