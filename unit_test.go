package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// runs before all tests and configures the test environment
func TestMain(m *testing.M) {
	// we do not need logging during the tests
	zerolog.SetGlobalLevel(zerolog.Disabled)

	code := m.Run()

	os.Exit(code)
}

type MockDeduplicationStore struct {
	mock.Mock
}

func (m *MockDeduplicationStore) IsProcessed(ctx context.Context, messageID string) (bool, error) {
	args := m.Called(ctx, messageID)
	return args.Bool(0), args.Error(1)
}

func (m *MockDeduplicationStore) MarkProcessed(ctx context.Context, messageID, messageType string) error {
	args := m.Called(ctx, messageID, messageType)
	return args.Error(0)
}

func (m *MockDeduplicationStore) Cleanup(ctx context.Context, olderThan time.Duration) error {
	args := m.Called(ctx, olderThan)
	return args.Error(0)
}

func (m *MockDeduplicationStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockSQSClient struct {
	mock.Mock
}

func (m *MockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (m *MockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

func (m *MockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.ChangeMessageVisibilityOutput), args.Error(1)
}

func (m *MockSQSClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.GetQueueAttributesOutput), args.Error(1)
}

type MockDatabase struct {
	mock.Mock
}

func (m *MockDatabase) CreateEmailLog(ctx context.Context, params CreateEmailLogParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockDatabase) CreateNotificationLog(ctx context.Context, params CreateNotificationLogParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockDatabase) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestMessageParsing(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		expectErr bool
		expected  Message
	}{
		{
			name: "valid email message",
			body: `{"id":"email-001","type":"email","data":{"recipient":"test@example.com","subject":"Test"},"metadata":{"priority":"high"}}`,
			expected: Message{
				ID:   "email-001",
				Type: "email",
				Data: map[string]interface{}{
					"recipient": "test@example.com",
					"subject":   "Test",
				},
				Metadata: map[string]string{
					"priority": "high",
				},
			},
			expectErr: false,
		},
		{
			name: "valid notification message",
			body: `{"id":"notif-001","type":"notification","data":{"user_id":"user123","type":"push","content":"Hello"},"metadata":{}}`,
			expected: Message{
				ID:   "notif-001",
				Type: "notification",
				Data: map[string]interface{}{
					"user_id": "user123",
					"type":    "push",
					"content": "Hello",
				},
				Metadata: map[string]string{},
			},
			expectErr: false,
		},
		{
			name:      "invalid json",
			body:      `{invalid json}`,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var msg Message
			err := json.Unmarshal([]byte(tt.body), &msg)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.ID, msg.ID)
				assert.Equal(t, tt.expected.Type, msg.Type)
			}
		})
	}
}

func TestHandleEmailMessage(t *testing.T) {
	tests := []struct {
		name            string
		message         *Message
		dbError         error
		expectedSuccess bool
	}{
		{
			name: "successful email processing",
			message: &Message{
				ID:   "email-001",
				Type: "email",
				Data: map[string]interface{}{
					"recipient": "test@example.com",
					"subject":   "Test Subject",
				},
			},
			dbError:         nil,
			expectedSuccess: true,
		},
		{
			name: "missing recipient",
			message: &Message{
				ID:   "email-002",
				Type: "email",
				Data: map[string]interface{}{
					"subject": "Test Subject",
				},
			},
			expectedSuccess: false,
		},
		{
			name: "missing subject",
			message: &Message{
				ID:   "email-003",
				Type: "email",
				Data: map[string]interface{}{
					"recipient": "test@example.com",
				},
			},
			expectedSuccess: false,
		},
		{
			name: "database error",
			message: &Message{
				ID:   "email-004",
				Type: "email",
				Data: map[string]interface{}{
					"recipient": "test@example.com",
					"subject":   "Test Subject",
				},
			},
			dbError:         assert.AnError,
			expectedSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mockDB := new(MockDatabase)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			processor := &MessageProcessor{
				db:    mockDB,
				ctx:   ctx,
				quiet: false,
			}

			//if message has valid fields, DB will be called
			_, hasRecipient := tt.message.Data["recipient"]
			_, hasSubject := tt.message.Data["subject"]
			if hasRecipient && hasSubject {
				mockDB.On("CreateEmailLog", mock.Anything, mock.Anything).Return(tt.dbError)
			}

			success := processor.handleEmailMessage(ctx, tt.message)

			assert.Equal(t, tt.expectedSuccess, success)
			mockDB.AssertExpectations(t)
		})
	}
}

func TestHandleNotificationMessage(t *testing.T) {
	tests := []struct {
		name            string
		message         *Message
		dbError         error
		expectedSuccess bool
	}{
		{
			name: "successful notification processing",
			message: &Message{
				ID:   "notif-001",
				Type: "notification",
				Data: map[string]interface{}{
					"user_id": "user123",
					"type":    "push",
					"content": "Test notification",
				},
			},
			dbError:         nil,
			expectedSuccess: true,
		},
		{
			name: "missing user_id",
			message: &Message{
				ID:   "notif-002",
				Type: "notification",
				Data: map[string]interface{}{
					"type":    "push",
					"content": "Test notification",
				},
			},
			expectedSuccess: false,
		},
		{
			name: "database error",
			message: &Message{
				ID:   "notif-003",
				Type: "notification",
				Data: map[string]interface{}{
					"user_id": "user123",
					"type":    "push",
					"content": "Test notification",
				},
			},
			dbError:         assert.AnError,
			expectedSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mockDB := new(MockDatabase)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			processor := &MessageProcessor{
				db:    mockDB,
				ctx:   ctx,
				quiet: false,
			}

			//if message has valid fields, DB will be called
			_, hasUserID := tt.message.Data["user_id"]
			_, hasType := tt.message.Data["type"]
			_, hasContent := tt.message.Data["content"]
			if hasUserID && hasType && hasContent {
				mockDB.On("CreateNotificationLog", mock.Anything, mock.Anything).Return(tt.dbError)
			}

			success := processor.handleNotificationMessage(ctx, tt.message)

			assert.Equal(t, tt.expectedSuccess, success)
			mockDB.AssertExpectations(t)
		})
	}
}

func TestWorkerPool(t *testing.T) {
	mockDB := new(MockDatabase)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mockSQS := new(MockSQSClient)

	processor := &MessageProcessor{
		db:        mockDB,
		ctx:       ctx,
		sqsClient: mockSQS,
		config: ProcessorConfig{
			QueueURL: "test-queue",
		},
		quiet: false,
	}

	pool := &WorkerPool{
		messageType: "email",
		workerCount: 5,
		jobs:        make(chan *WorkerJob, 10),
		processor:   processor,
	}

	mockDB.On("CreateEmailLog", mock.Anything, mock.Anything).Return(nil)
	mockSQS.On("DeleteMessage", mock.Anything, mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)

	// Create dedup store mock
	mockDedup := &MockDeduplicationStore{}
	mockDedup.On("IsProcessed", mock.Anything, mock.Anything).Return(false, nil)
	mockDedup.On("MarkProcessed", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	processor.dedupStore = mockDedup

	pool.Start(ctx)

	receiptHandle := "test-receipt-handle"
	for i := 0; i < 5; i++ {
		job := &WorkerJob{
			Message: &Message{
				ID:   fmt.Sprintf("email-%03d", i),
				Type: "email",
				Data: map[string]interface{}{
					"recipient": "test@example.com",
					"subject":   "Test",
				},
			},
			ReceiptHandle: &receiptHandle,
		}
		pool.jobs <- job
	}

	// Wait for processing with proper synchronization
	time.Sleep(1 * time.Second)

	pool.Stop()

	mockDB.AssertNumberOfCalls(t, "CreateEmailLog", 5)
}

func TestProcessMessageRouting(t *testing.T) {
	tests := []struct {
		name          string
		messageType   string
		shouldRoute   bool
		expectDeleted bool
	}{
		{
			name:          "route to email worker",
			messageType:   "email",
			shouldRoute:   true,
			expectDeleted: false,
		},
		{
			name:          "route to notification worker",
			messageType:   "notification",
			shouldRoute:   true,
			expectDeleted: false,
		},
		{
			name:          "unknown message type",
			messageType:   "unknown",
			shouldRoute:   false,
			expectDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSQS := new(MockSQSClient)
			mockDB := new(MockDatabase)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			processor := &MessageProcessor{
				config: ProcessorConfig{
					QueueURL: "test-queue-url",
					WorkerCounts: map[string]int{
						"email":        2,
						"notification": 2,
					},
				},
				sqsClient: mockSQS,
				db:        mockDB,
				workers:   make(map[string]*WorkerPool),
				ctx:       ctx,
				cancel:    cancel,
				quiet:     false,
			}

			for messageType, workerCount := range processor.config.WorkerCounts {
				pool := &WorkerPool{
					messageType: messageType,
					workerCount: workerCount,
					jobs:        make(chan *WorkerJob, workerCount*10),
					processor:   processor,
				}
				processor.workers[messageType] = pool
			}

			msg := Message{
				ID:   "test-001",
				Type: tt.messageType,
				Data: map[string]interface{}{
					"recipient": "test@example.com",
					"subject":   "Test",
				},
			}
			body, _ := json.Marshal(msg)
			receiptHandle := "test-receipt"
			id := xid.New()
			sqsMsg := types.Message{
				Body:          aws.String(string(body)),
				ReceiptHandle: &receiptHandle,
				MessageId:     aws.String(id.String()),
			}

			if tt.expectDeleted {
				mockSQS.On("DeleteMessage", mock.Anything, mock.MatchedBy(func(input *sqs.DeleteMessageInput) bool {
					return *input.ReceiptHandle == receiptHandle
				})).Return(&sqs.DeleteMessageOutput{}, nil)
			}

			wasFull := processor.processMessage(sqsMsg)

			assert.False(t, wasFull)
			if tt.expectDeleted {
				mockSQS.AssertExpectations(t)
			}

			if tt.shouldRoute {
				pool := processor.workers[tt.messageType]
				assert.Equal(t, 1, len(pool.jobs))
			}
		})
	}
}

func TestContextCancellation(t *testing.T) {
	mockDB := new(MockDatabase)
	ctx, cancel := context.WithCancel(context.Background())

	processor := &MessageProcessor{
		db:     mockDB,
		ctx:    ctx,
		cancel: cancel,
		workers: map[string]*WorkerPool{
			"email": {
				messageType: "email",
				workerCount: 2,
				jobs:        make(chan *WorkerJob, 10),
			},
		},
		quiet: false,
	}

	cancel()

	receiptHandle := "test-receipt"
	msg := Message{
		ID:   "test-001",
		Type: "email",
		Data: map[string]interface{}{
			"recipient": "test@example.com",
			"subject":   "Test",
		},
	}
	body, _ := json.Marshal(msg)

	sqsMsg := types.Message{
		Body:          aws.String(string(body)),
		ReceiptHandle: &receiptHandle,
	}

	wasFull := processor.processMessage(sqsMsg)
	assert.False(t, wasFull)
}

func TestWorkerPoolFull(t *testing.T) {
	mockSQS := new(MockSQSClient)
	mockDB := new(MockDatabase)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := &MessageProcessor{
		config: ProcessorConfig{
			QueueURL: "test-queue-url",
		},
		sqsClient: mockSQS,
		db:        mockDB,
		workers:   make(map[string]*WorkerPool),
		ctx:       ctx,
		cancel:    cancel,
		quiet:     false,
	}

	pool := &WorkerPool{
		messageType: "email",
		workerCount: 1,
		jobs:        make(chan *WorkerJob, 1), // Very small buffer
		processor:   processor,
	}
	processor.workers["email"] = pool

	receiptHandle1 := "receipt-1"
	pool.jobs <- &WorkerJob{
		Message: &Message{
			ID:   "email-001",
			Type: "email",
			Data: map[string]interface{}{
				"recipient": "test@example.com",
				"subject":   "Test",
			},
		},
		ReceiptHandle: &receiptHandle1,
	}

	receiptHandle2 := "receipt-2"
	mockSQS.On("ChangeMessageVisibility", mock.Anything, mock.MatchedBy(func(input *sqs.ChangeMessageVisibilityInput) bool {
		return *input.ReceiptHandle == receiptHandle2
	})).Return(&sqs.ChangeMessageVisibilityOutput{}, nil)

	msg := Message{
		ID:   "email-002",
		Type: "email",
		Data: map[string]interface{}{
			"recipient": "test@example.com",
			"subject":   "Test",
		},
	}
	body, _ := json.Marshal(msg)

	sqsMsg := types.Message{
		Body:          aws.String(string(body)),
		ReceiptHandle: &receiptHandle2,
	}

	wasFull := processor.processMessage(sqsMsg)
	assert.True(t, wasFull, "Pool should be full")
	mockSQS.AssertExpectations(t)
}
