package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
)

type SQSClientInterface interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

type ProcessorConfig struct {
	QueueURL     string
	WorkerCounts map[string]int // message type -> worker count
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	app := &cli.App{
		Name:  "sqs-processor",
		Usage: "Process AWS SQS messages with configurable workers",
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Start the SQS message processor",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "queue-url",
						Usage:    "AWS SQS queue URL",
						Required: true,
						EnvVars:  []string{"SQS_QUEUE_URL"},
					},
					&cli.StringFlag{
						Name:    "db-url",
						Usage:   "Database connection URL",
						Value:   "postgres://user:password@localhost/dbname?sslmode=disable",
						EnvVars: []string{"DATABASE_URL"},
					},
					&cli.IntFlag{
						Name:  "email-workers",
						Usage: "Number of workers for email message processing",
						Value: 10,
					},
					&cli.IntFlag{
						Name:  "notification-workers",
						Usage: "Number of workers for notification message processing",
						Value: 10,
					},
					&cli.StringFlag{
						Name:    "log-level",
						Usage:   "Log level (debug, info, warn, error)",
						Value:   "info",
						EnvVars: []string{"LOG_LEVEL"},
					},
					&cli.BoolFlag{
						Name:    "quiet",
						Usage:   "Suppress successful message processing logs (only show metrics and stats)",
						Value:   false,
						EnvVars: []string{"QUIET"},
					},
					&cli.StringFlag{
						Name:    "dedup-type",
						Usage:   "Deduplication store type (postgres, memory, redis)",
						Value:   "postgres",
						EnvVars: []string{"DEDUP_TYPE"},
					},
				},
				Action: startProcessor,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err).Msg("Application failed")
	}
}

func startProcessor(c *cli.Context) error {
	switch c.String("log-level") {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// aws config
	awsCFG, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	db, err := NewDatabase(c.String("db-url"))
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	var dedupStore DeduplicationStore

	dedupType := c.String("dedup-type")
	switch dedupType {
	case "postgres":
		dedupStore = NewPostgresDeduplicationStore(db.db)
	case "memory":
		dedupStore = NewInMemoryDeduplicationStore()
	// case "redis":
	//     dedupStore, err = NewRedisDeduplicationStore(c.String("redis-addr"), 7*24*time.Hour)
	//     if err != nil {
	//         return fmt.Errorf("failed to create redis deduplication store: %w", err)
	//     }
	default:
		return fmt.Errorf("invalid dedup-type: %s", dedupType)
	}
	defer dedupStore.Close()

	processorConfig := ProcessorConfig{
		QueueURL: c.String("queue-url"),
		WorkerCounts: map[string]int{
			"email":        c.Int("email-workers"),
			"notification": c.Int("notification-workers"),
		},
	}

	processor, err := NewMessageProcessor(awsCFG, processorConfig, db, dedupStore, c.Bool("quiet"))
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}

	// shutdown setup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Info().Msg("Starting SQS message processor")
	go processor.Start()

	// wait for shutdown signal / ctrl-c or sigterm which is what docker sends
	<-sigChan
	log.Info().Msg("Shutting down...")
	processor.Stop()

	return nil
}
