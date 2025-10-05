# SQS Load Generator

A simple load testing tool for AWS SQS with a nice TUI dashboard. Sends a bunch of messages to your queue and shows you what's happening in real-time.

**Notes**

the load tester has been written to just work...

## What it does

- Sends messages to an SQS queue with configurable concurrency
- Shows progress, latency stats, and throughput in a terminal dashboard
- Supports different workload patterns (steady, burst, wave, time-of-day)
- Generates two types of test messages: emails and notifications

## Usage

make sure your .envrc file is set in the project root and loaded, this tool will use the env vars set to run
```bash
go build
./loadtester
```

### Environment Variables - Extra Options

**Required:**
- `SQS_QUEUE_URL` - Your SQS queue URL

**Optional:**
- `AWS_REGION` - AWS region (default: `us-east-1`)
- `LOAD_TEST_MESSAGES` - Number of messages to send (default: `1000`)
- `LOAD_TEST_CONCURRENCY` - Worker threads (default: `10`)
- `LOAD_TEST_EMAIL_RATIO` - Ratio of emails to notifications, 0-1 (default: `0.5`)
- `LOAD_TEST_TIMEOUT_SECONDS` - Timeout per message (default: `30`)
- `LOAD_TEST_PATTERN` - Workload pattern (default: `wave`)

### Workload Patterns

- `steady` - Constant rate
- `burst` - Periodic bursts of high volume
- `wave` - Sinusoidal traffic pattern
- `timeofday` - Simulates daily traffic (low at night, peak during day)

### Example

```bash
export SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
export AWS_REGION="us-west-2"
export LOAD_TEST_MESSAGES=5000
export LOAD_TEST_CONCURRENCY=20
export LOAD_TEST_PATTERN=burst
go run main.go
```

## What the messages look like

**Email:**
```json
{
  "id": "email-000123-abc123",
  "type": "email",
  "data": {
    "recipient": "user123@example.com",
    "subject": "Test Email"
  },
  "metadata": {
    "priority": "high"
  }
}
```

**Notification:**
```json
{
  "id": "notification-000456-def456",
  "type": "notification",
  "data": {
    "user_id": "user5432",
    "type": "push",
    "content": "You have a new message"
  },
  "metadata": {
    "platform": "mobile"
  }
}
```

## Controls

- `q` or `Ctrl+C` - Quit

## Notes

- The dashboard updates in real-time showing latency, throughput, success rate, etc.
- Failed messages are logged with error details
- All latencies are tracked (min/max/avg)
- The pattern visualization shows where you are in the workload