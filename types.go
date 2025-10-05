package main

// represents the structure of messages from SQS
type Message struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Data     map[string]any    `json:"data"`
	Metadata map[string]string `json:"metadata"`
}

// wraps a message with its SQS receipt handle for deletion after processing
type WorkerJob struct {
	Message       *Message
	ReceiptHandle *string
}
