package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/rs/xid"
)

var (
	queueURL         string
	region           string
	numberOfMessages int
	concurrency      int
	emailRatio       float64
	sendTimeout      time.Duration
	currentPattern   WorkloadPattern
)

func init() {
	queueURL = getEnv("SQS_QUEUE_URL", "")
	if queueURL == "" {
		fmt.Fprintf(os.Stderr, "ERROR: SQS_QUEUE_URL environment variable is required\n")
		os.Exit(1)
	}

	region = getEnv("AWS_REGION", "us-east-1")
	numberOfMessages = getEnvInt("LOAD_TEST_MESSAGES", 1000)
	concurrency = getEnvInt("LOAD_TEST_CONCURRENCY", 10)
	emailRatio = getEnvFloat("LOAD_TEST_EMAIL_RATIO", 0.5)
	sendTimeout = time.Duration(getEnvInt("LOAD_TEST_TIMEOUT_SECONDS", 30)) * time.Second

	patternStr := getEnv("LOAD_TEST_PATTERN", "wave")
	currentPattern = WorkloadPattern(patternStr)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

type WorkloadPattern string

const (
	PatternSteady    WorkloadPattern = "steady"
	PatternBurst     WorkloadPattern = "burst"
	PatternWave      WorkloadPattern = "wave"
	PatternTimeOfDay WorkloadPattern = "timeofday"
)

type MessageData struct {
	Recipient string `json:"recipient,omitempty"`
	Subject   string `json:"subject,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Type      string `json:"type,omitempty"`
	Content   string `json:"content,omitempty"`
}

type MessageMetadata struct {
	Priority string `json:"priority,omitempty"`
	Platform string `json:"platform,omitempty"`
}

type Message struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Data     MessageData     `json:"data"`
	Metadata MessageMetadata `json:"metadata"`
}

type Result struct {
	Success     bool
	Duration    time.Duration
	Index       int
	Error       string
	MessageType string
}

// UI Model
type model struct {
	spinner           spinner.Model
	progress          progress.Model
	totalMessages     int
	sentMessages      int
	successfulMsgs    int
	failedMsgs        int
	emailsSent        int
	notificationsSent int
	recentLogs        []logEntry
	errors            []string
	latencies         []time.Duration
	minLatency        time.Duration
	maxLatency        time.Duration
	avgLatency        time.Duration
	throughput        float64
	startTime         time.Time
	currentTime       time.Time
	isComplete        bool
	width             int
	height            int
	pattern           WorkloadPattern
	patternPhase      string
}

type logEntry struct {
	timestamp time.Time
	message   string
	msgType   string
	success   bool
}

type tickMsg time.Time
type resultMsg Result
type completeMsg struct{}

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("212")).
			Background(lipgloss.Color("235")).
			Padding(0, 1).
			MarginBottom(1)

	configBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("254")).
			Background(lipgloss.Color("236")).
			Padding(0, 1).
			MarginBottom(1)

	configLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("243"))

	configValueStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("117")).
				Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("42"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196"))

	emailStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("99"))

	notificationStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("220"))

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("241"))

	valueStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("111"))

	boxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("238")).
			Padding(1, 2).
			MarginBottom(1)

	patternStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("213")).
			Bold(true)
)

func initialModel() model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return model{
		spinner:       s,
		progress:      progress.New(progress.WithDefaultGradient()),
		totalMessages: numberOfMessages,
		recentLogs:    make([]logEntry, 0, 20),
		errors:        make([]string, 0),
		latencies:     make([]time.Duration, 0),
		startTime:     time.Now(),
		pattern:       currentPattern,
		patternPhase:  "Initializing",
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		tickCmd(),
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.progress.Width = msg.Width - 4
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}

	case tickMsg:
		m.currentTime = time.Time(msg)
		if !m.isComplete {
			return m, tickCmd()
		}
		return m, nil

	case resultMsg:
		m.sentMessages++
		m.latencies = append(m.latencies, msg.Duration)

		// update min/max/avg latency
		if len(m.latencies) == 1 {
			m.minLatency = msg.Duration
			m.maxLatency = msg.Duration
		} else {
			if msg.Duration < m.minLatency {
				m.minLatency = msg.Duration
			}
			if msg.Duration > m.maxLatency {
				m.maxLatency = msg.Duration
			}
		}

		var total time.Duration
		for _, d := range m.latencies {
			total += d
		}
		m.avgLatency = total / time.Duration(len(m.latencies))

		// calc throughput
		elapsed := time.Since(m.startTime).Seconds()
		if elapsed > 0 {
			m.throughput = float64(m.successfulMsgs) / elapsed
		}

		// pattern phase
		progress := float64(m.sentMessages) / float64(m.totalMessages)
		m.patternPhase = getPatternPhase(m.pattern, progress)

		if msg.Success {
			m.successfulMsgs++
			if msg.MessageType == "email" {
				m.emailsSent++
			} else {
				m.notificationsSent++
			}

			logMsg := fmt.Sprintf("Message %d sent (%v)", msg.Index, msg.Duration)
			m.recentLogs = append([]logEntry{{
				timestamp: time.Now(),
				message:   logMsg,
				msgType:   msg.MessageType,
				success:   true,
			}}, m.recentLogs...)
		} else {
			m.failedMsgs++
			logMsg := fmt.Sprintf("Message %d failed: %s", msg.Index, msg.Error)
			m.recentLogs = append([]logEntry{{
				timestamp: time.Now(),
				message:   logMsg,
				msgType:   msg.MessageType,
				success:   false,
			}}, m.recentLogs...)

			m.errors = append([]string{fmt.Sprintf("[%s] %s", msg.MessageType, msg.Error)}, m.errors...)
			if len(m.errors) > 5 {
				m.errors = m.errors[:5]
			}
		}

		if len(m.recentLogs) > 15 {
			m.recentLogs = m.recentLogs[:15]
		}

		return m, nil

	case completeMsg:
		m.isComplete = true
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	default:
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	var b strings.Builder

	title := titleStyle.Render("🚀 SQS Load Generator - Advanced Dashboard")
	b.WriteString(title + "\n")

	// Configuration info bar
	// configBar := m.renderConfigBar()
	// b.WriteString(configBar + "\n\n")

	// main progress bar
	progressPercent := float64(m.sentMessages) / float64(m.totalMessages)
	progressBar := m.progress.ViewAs(progressPercent)
	progressText := fmt.Sprintf("Progress: %d/%d messages (%.1f%%)",
		m.sentMessages, m.totalMessages, progressPercent*100)

	if !m.isComplete {
		progressText = m.spinner.View() + " " + progressText
	} else {
		progressText = "✓ " + progressText
	}

	b.WriteString(progressText + "\n")
	b.WriteString(progressBar + "\n\n")

	configPanel := m.renderConfigPanel()
	b.WriteString(configPanel + "\n")

	leftColumn := m.renderMetricsPanel()
	rightColumn := m.renderStatsPanel()

	columns := lipgloss.JoinHorizontal(
		lipgloss.Top,
		leftColumn,
		rightColumn,
	)
	b.WriteString(columns + "\n")

	patternViz := m.renderPatternVisualization()
	b.WriteString(patternViz + "\n")

	logPanel := m.renderLogPanel()
	b.WriteString(logPanel + "\n")

	if len(m.errors) > 0 {
		errorPanel := m.renderErrorPanel()
		b.WriteString(errorPanel + "\n")
	}

	if m.isComplete {
		b.WriteString(successStyle.Render("\n✓ Test Complete! Press 'q' to quit"))
	} else {
		b.WriteString(labelStyle.Render("\nPress 'q' to quit"))
	}

	return b.String()
}

func (m model) renderConfigPanel() string {

	displayQueueURL := queueURL
	if len(displayQueueURL) > 60 {
		displayQueueURL = "..." + displayQueueURL[len(displayQueueURL)-57:]
	}

	content := fmt.Sprintf(
		"%s\n"+
			"  %s %s\n"+
			"  %s %s\n"+
			"  %s %s\n"+
			"  %s %s\n"+
			"  %s %s\n"+
			"  %s %s",
		labelStyle.Render("Configuration:"),
		labelStyle.Render("Queue URL:"),
		configValueStyle.Render(displayQueueURL),
		labelStyle.Render("Region:"),
		configValueStyle.Render(region),
		labelStyle.Render("Workers:"),
		configValueStyle.Render(fmt.Sprintf("%d", concurrency)),
		labelStyle.Render("Pattern:"),
		configValueStyle.Render(string(currentPattern)),
		labelStyle.Render("Email Ratio:"),
		configValueStyle.Render(fmt.Sprintf("%.0f%%", emailRatio*100)),
		labelStyle.Render("Timeout:"),
		configValueStyle.Render(fmt.Sprintf("%ds", int(sendTimeout.Seconds()))),
	)

	return boxStyle.Width(84).Render(content)
}

// func (m model) renderConfigBar() string {
// 	// Truncate queue URL if too long
// 	displayQueueURL := queueURL
// 	if len(displayQueueURL) > 50 {
// 		displayQueueURL = "..." + displayQueueURL[len(displayQueueURL)-47:]
// 	}

// 	configItems := []string{
// 		fmt.Sprintf("%s %s", configLabelStyle.Render("Queue:"), configValueStyle.Render(displayQueueURL)),
// 		fmt.Sprintf("%s %s", configLabelStyle.Render("Region:"), configValueStyle.Render(region)),
// 		fmt.Sprintf("%s %s", configLabelStyle.Render("Workers:"), configValueStyle.Render(fmt.Sprintf("%d", concurrency))),
// 		fmt.Sprintf("%s %s", configLabelStyle.Render("Pattern:"), configValueStyle.Render(string(currentPattern))),
// 		fmt.Sprintf("%s %s", configLabelStyle.Render("Email Ratio:"), configValueStyle.Render(fmt.Sprintf("%.0f%%", emailRatio*100))),
// 		fmt.Sprintf("%s %s", configLabelStyle.Render("Timeout:"), configValueStyle.Render(fmt.Sprintf("%ds", int(sendTimeout.Seconds())))),
// 	}

// 	configLine := strings.Join(configItems, configLabelStyle.Render(" │ "))

// 	// Calculate padding to center or fill width
// 	contentWidth := lipgloss.Width(configLine)
// 	if m.width > contentWidth+4 {
// 		padding := (m.width - contentWidth - 4) / 2
// 		configLine = strings.Repeat(" ", padding) + configLine
// 	}

// 	return configBarStyle.Width(m.width).Render(configLine)
// }

func (m model) renderMetricsPanel() string {
	elapsed := m.currentTime.Sub(m.startTime)
	if elapsed == 0 {
		elapsed = time.Since(m.startTime)
	}

	content := fmt.Sprintf(
		"%s %s\n"+
			"%s %s\n"+
			"%s %s\n"+
			"%s %s\n\n"+
			"%s\n"+
			"  %s %s\n"+
			"  %s %s\n\n"+
			"%s %s\n"+
			"%s %s msg/s",
		labelStyle.Render("Total Sent:"),
		valueStyle.Render(fmt.Sprintf("%d", m.sentMessages)),
		labelStyle.Render("Successful:"),
		successStyle.Render(fmt.Sprintf("%d", m.successfulMsgs)),
		labelStyle.Render("Failed:"),
		errorStyle.Render(fmt.Sprintf("%d", m.failedMsgs)),
		labelStyle.Render("Success Rate:"),
		valueStyle.Render(fmt.Sprintf("%.1f%%", float64(m.successfulMsgs)/float64(max(m.sentMessages, 1))*100)),
		labelStyle.Render("Message Types:"),
		emailStyle.Render("📧 Emails:"),
		valueStyle.Render(fmt.Sprintf("%d (%.0f%%)", m.emailsSent, float64(m.emailsSent)/float64(max(m.successfulMsgs, 1))*100)),
		notificationStyle.Render("🔔 Notifications:"),
		valueStyle.Render(fmt.Sprintf("%d (%.0f%%)", m.notificationsSent, float64(m.notificationsSent)/float64(max(m.successfulMsgs, 1))*100)),
		labelStyle.Render("Elapsed:"),
		valueStyle.Render(elapsed.Round(time.Second).String()),
		labelStyle.Render("Throughput:"),
		valueStyle.Render(fmt.Sprintf("%.2f", m.throughput)),
	)

	return boxStyle.Width(40).Render(content)
}

func (m model) renderStatsPanel() string {
	var minStr, maxStr, avgStr string
	if len(m.latencies) > 0 {
		minStr = m.minLatency.Round(time.Millisecond).String()
		maxStr = m.maxLatency.Round(time.Millisecond).String()
		avgStr = m.avgLatency.Round(time.Millisecond).String()
	} else {
		minStr = "N/A"
		maxStr = "N/A"
		avgStr = "N/A"
	}

	// latency sparkline
	sparkline := m.renderLatencySparkline()

	content := fmt.Sprintf(
		"%s\n"+
			"%s %s\n"+
			"%s %s\n"+
			"%s %s\n\n"+
			"%s\n%s\n\n"+
			"%s\n"+
			"%s %s\n"+
			"%s %d",
		labelStyle.Render("Latency Statistics:"),
		labelStyle.Render("  Min:"),
		valueStyle.Render(minStr),
		labelStyle.Render("  Max:"),
		valueStyle.Render(maxStr),
		labelStyle.Render("  Avg:"),
		valueStyle.Render(avgStr),
		labelStyle.Render("Recent Latency Trend:"),
		sparkline,
		labelStyle.Render("Configuration:"),
		labelStyle.Render("  Concurrency:"),
		valueStyle.Render(fmt.Sprintf("%d workers", concurrency)),
		labelStyle.Render("  Total Messages:"),
		numberOfMessages,
	)

	return boxStyle.Width(40).Render(content)
}

func (m model) renderLatencySparkline() string {
	if len(m.latencies) == 0 {
		return labelStyle.Render("  No data yet...")
	}

	// last 30 latencies
	start := 0
	if len(m.latencies) > 30 {
		start = len(m.latencies) - 30
	}
	recent := m.latencies[start:]

	// min/max for scaling
	var min, max time.Duration
	min = recent[0]
	max = recent[0]
	for _, l := range recent {
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
	}

	// sparkline
	bars := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	var sparkline strings.Builder
	sparkline.WriteString("  ")

	for _, l := range recent {
		var normalized float64
		if max > min {
			normalized = float64(l-min) / float64(max-min)
		} else {
			normalized = 0.5
		}
		idx := int(normalized * float64(len(bars)-1))
		if idx >= len(bars) {
			idx = len(bars) - 1
		}
		if idx < 0 {
			idx = 0
		}
		sparkline.WriteRune(bars[idx])
	}

	return valueStyle.Render(sparkline.String())
}

func (m model) renderPatternVisualization() string {
	progress := float64(m.sentMessages) / float64(m.totalMessages)
	visualization := generatePatternVisualization(m.pattern, progress, 60)

	content := fmt.Sprintf(
		"%s %s\n"+
			"%s %s\n\n"+
			"%s",
		labelStyle.Render("Workload Pattern:"),
		patternStyle.Render(string(m.pattern)),
		labelStyle.Render("Phase:"),
		valueStyle.Render(m.patternPhase),
		visualization,
	)

	return boxStyle.Width(84).Render(content)
}

func (m model) renderLogPanel() string {
	var logs strings.Builder
	logs.WriteString(labelStyle.Render("Recent Activity:") + "\n\n")

	if len(m.recentLogs) == 0 {
		logs.WriteString(labelStyle.Render("  No activity yet..."))
	} else {
		for i, log := range m.recentLogs {
			if i >= 10 {
				break
			}

			var style lipgloss.Style
			var icon string
			if log.success {
				style = successStyle
				icon = "✓"
			} else {
				style = errorStyle
				icon = "✗"
			}

			var typeIcon string
			if log.msgType == "email" {
				typeIcon = emailStyle.Render("📧")
			} else {
				typeIcon = notificationStyle.Render("🔔")
			}

			timestamp := log.timestamp.Format("15:04:05.000")
			logs.WriteString(fmt.Sprintf("  %s %s %s %s\n",
				labelStyle.Render(timestamp),
				typeIcon,
				style.Render(icon),
				log.message,
			))
		}
	}

	return boxStyle.Width(84).Render(logs.String())
}

func (m model) renderErrorPanel() string {
	var errorList strings.Builder
	errorList.WriteString(errorStyle.Render("⚠ Recent Errors:") + "\n\n")

	for i, err := range m.errors {
		if i >= 5 {
			break
		}
		errorList.WriteString(fmt.Sprintf("  %s %s\n", errorStyle.Render("•"), err))
	}

	return boxStyle.Width(84).Render(errorList.String())
}

func getPatternPhase(pattern WorkloadPattern, progress float64) string {
	switch pattern {
	case PatternBurst:
		if progress < 0.3 || (progress > 0.5 && progress < 0.6) || (progress > 0.8 && progress < 0.9) {
			return "🔥 BURST - High Volume"
		}
		return "📊 Normal - Steady Flow"

	case PatternWave:
		sineValue := math.Sin(progress * 6 * math.Pi)
		if sineValue > 0.5 {
			return "📈 Peak - High Activity"
		} else if sineValue < -0.5 {
			return "📉 Valley - Low Activity"
		}
		return "〰️ Transitioning"

	case PatternTimeOfDay:
		hour := progress * 24
		switch {
		case hour < 6:
			return "🌙 Night - Low Traffic"
		case hour < 9:
			return "🌅 Morning - Ramping Up"
		case hour < 14:
			return "☀️ Peak Hours - Maximum Load"
		case hour < 20:
			return "🌆 Evening - Moderate Traffic"
		default:
			return "🌃 Night - Winding Down"
		}

	case PatternSteady:
		return "▶️ Steady - Constant Rate"

	default:
		return "Unknown"
	}
}

func generatePatternVisualization(pattern WorkloadPattern, currentProgress float64, width int) string {
	var viz strings.Builder

	bars := []rune{' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

	// gen visualization bars
	for i := 0; i < width; i++ {
		progress := float64(i) / float64(width)
		var intensity float64

		switch pattern {
		case PatternBurst:
			if progress < 0.3 || (progress > 0.5 && progress < 0.6) || (progress > 0.8 && progress < 0.9) {
				intensity = 0.9
			} else {
				intensity = 0.3
			}

		case PatternWave:
			sineValue := (1 + math.Sin(progress*6*math.Pi)) / 2
			intensity = sineValue

		case PatternTimeOfDay:
			hour := progress * 24
			switch {
			case hour < 6:
				intensity = 0.2
			case hour < 9:
				intensity = 0.5
			case hour < 14:
				intensity = 0.9
			case hour < 20:
				intensity = 0.6
			default:
				intensity = 0.3
			}

		case PatternSteady:
			intensity = 0.5
		}

		// bar character based on intensity
		idx := int(intensity * float64(len(bars)-1))
		if idx >= len(bars) {
			idx = len(bars) - 1
		}
		if idx < 0 {
			idx = 0
		}

		// highlight current position
		if math.Abs(progress-currentProgress) < 0.02 {
			viz.WriteString(successStyle.Render(string(bars[idx])))
		} else {
			viz.WriteString(labelStyle.Render(string(bars[idx])))
		}
	}

	return viz.String()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func generateEmail(rng *rand.Rand, index int) string {
	domains := []string{"example.com", "test.com", "demo.com", "loadtest.com"}
	domain := domains[rng.Intn(len(domains))]
	return fmt.Sprintf("user%d-%d@%s", index, time.Now().UnixNano(), domain)
}

func generateSubject(rng *rand.Rand) string {
	subjects := []string{
		"Test Email", "Load Test Message", "Performance Test",
		"Queue Test", "SQS Load Test", "Important Update",
		"Weekly Report", "Account Notification", "System Alert",
	}
	return subjects[rng.Intn(len(subjects))]
}

func generateUserID(rng *rand.Rand, index int) string {
	return fmt.Sprintf("user%d", 1000+rng.Intn(9000))
}

func generateNotificationType(rng *rand.Rand) string {
	types := []string{"push", "sms", "in-app", "email"}
	return types[rng.Intn(len(types))]
}

func generateNotificationContent(rng *rand.Rand) string {
	contents := []string{
		"You have a new message", "Your order has been shipped",
		"Someone liked your post", "Payment received",
		"New follower", "Task completed",
		"Alert: System update required", "Reminder: Meeting in 15 minutes",
		"New comment on your post", "Weekly summary available",
	}
	return contents[rng.Intn(len(contents))]
}

func generatePlatform(rng *rand.Rand) string {
	platforms := []string{"mobile", "web", "desktop", "tablet"}
	return platforms[rng.Intn(len(platforms))]
}

func getMessageDelay(pattern WorkloadPattern, index int, total int, rng *rand.Rand) time.Duration {
	switch pattern {
	case PatternSteady:
		return time.Duration(10+rng.Intn(5)) * time.Millisecond

	case PatternBurst:
		progress := float64(index) / float64(total)
		if progress < 0.3 || (progress > 0.5 && progress < 0.6) || (progress > 0.8 && progress < 0.9) {
			return time.Duration(rng.Intn(5)) * time.Millisecond
		}
		return time.Duration(50+rng.Intn(100)) * time.Millisecond

	case PatternWave:
		progress := float64(index) / float64(total)
		sineValue := (1 + math.Sin(progress*6*math.Pi)) / 2
		baseDelay := 5 + int(sineValue*195)
		return time.Duration(baseDelay+rng.Intn(10)) * time.Millisecond

	case PatternTimeOfDay:
		progress := float64(index) / float64(total)
		hour := progress * 24

		var baseDelay int
		switch {
		case hour < 6:
			baseDelay = 150
		case hour < 9:
			baseDelay = 80
		case hour < 12:
			baseDelay = 20
		case hour < 14:
			baseDelay = 5
		case hour < 17:
			baseDelay = 25
		case hour < 20:
			baseDelay = 60
		case hour < 22:
			baseDelay = 100
		default:
			baseDelay = 140
		}
		return time.Duration(baseDelay+rng.Intn(20)) * time.Millisecond

	default:
		return 10 * time.Millisecond
	}
}

func getEmailRatio(pattern WorkloadPattern, index int, total int) float32 {
	switch pattern {
	case PatternSteady:
		return float32(emailRatio)

	case PatternBurst:
		progress := float64(index) / float64(total)
		if progress < 0.3 || (progress > 0.5 && progress < 0.6) || (progress > 0.8 && progress < 0.9) {
			return 0.2
		}
		return 0.7

	case PatternWave:
		progress := float64(index) / float64(total)
		sineValue := (1 + math.Sin(progress*4*math.Pi)) / 2
		return float32(0.2 + sineValue*0.6)

	case PatternTimeOfDay:
		progress := float64(index) / float64(total)
		hour := progress * 24

		switch {
		case hour < 6:
			return 0.8
		case hour < 9:
			return 0.6
		case hour < 12:
			return 0.4
		case hour < 14:
			return 0.2
		case hour < 17:
			return 0.5
		case hour < 20:
			return 0.3
		case hour < 22:
			return 0.7
		default:
			return 0.8
		}

	default:
		return float32(emailRatio)
	}
}

func getMessageSize(pattern WorkloadPattern, index int, total int, rng *rand.Rand) int {
	switch pattern {
	case PatternSteady:
		return 1 + rng.Intn(2)

	case PatternBurst:
		progress := float64(index) / float64(total)
		if progress < 0.3 || (progress > 0.5 && progress < 0.6) || (progress > 0.8 && progress < 0.9) {
			return 1
		}
		return 2 + rng.Intn(3)

	case PatternWave:
		progress := float64(index) / float64(total)
		sineValue := (1 + math.Sin(progress*3*math.Pi)) / 2
		return 1 + int(sineValue*4)

	case PatternTimeOfDay:
		progress := float64(index) / float64(total)
		hour := progress * 24

		switch {
		case hour < 6:
			return 3 + rng.Intn(3)
		case hour < 9:
			return 2 + rng.Intn(2)
		case hour < 14:
			return 1 + rng.Intn(2)
		case hour < 20:
			return 2 + rng.Intn(2)
		default:
			return 3 + rng.Intn(2)
		}

	default:
		return 1
	}
}

func createEmailMessage(rng *rand.Rand, index int, sizeMultiplier int) Message {
	priority := "normal"
	if rng.Float32() > 0.5 {
		priority = "high"
	}

	subject := generateSubject(rng)
	if sizeMultiplier > 2 {
		subject = subject + " - Extended Report"
	}
	guid := xid.New()

	return Message{
		ID:   fmt.Sprintf("email-%06d-%s", index, guid.String()),
		Type: "email",
		Data: MessageData{
			Recipient: generateEmail(rng, index),
			Subject:   subject,
		},
		Metadata: MessageMetadata{
			Priority: priority,
		},
	}
}

func createNotificationMessage(rng *rand.Rand, index int, sizeMultiplier int) Message {
	content := generateNotificationContent(rng)
	if sizeMultiplier > 2 {
		content = content + " - Click for details and more information"
	}
	guid := xid.New()
	return Message{
		ID:   fmt.Sprintf("notification-%06d-%s", index, guid.String()),
		Type: "notification",
		Data: MessageData{
			UserID:  generateUserID(rng, index),
			Type:    generateNotificationType(rng),
			Content: content,
		},
		Metadata: MessageMetadata{
			Platform: generatePlatform(rng),
		},
	}
}

func sendMessage(ctx context.Context, client *sqs.Client, rng *rand.Rand, index int, totalMessages int) Result {
	currentEmailRatio := getEmailRatio(currentPattern, index, totalMessages)
	sizeMultiplier := getMessageSize(currentPattern, index, totalMessages, rng)

	var msg Message
	var messageType string
	if rng.Float32() < currentEmailRatio {
		msg = createEmailMessage(rng, index, sizeMultiplier)
		messageType = "email"
	} else {
		msg = createNotificationMessage(rng, index, sizeMultiplier)
		messageType = "notification"
	}

	messageBody, err := json.Marshal(msg)
	if err != nil {
		return Result{
			Success:     false,
			Duration:    0,
			Index:       index,
			Error:       fmt.Sprintf("JSON marshal error: %v", err),
			MessageType: messageType,
		}
	}

	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()

	startTime := time.Now()
	queueURLStr := queueURL
	messageBodyStr := string(messageBody)
	_, err = client.SendMessage(sendCtx, &sqs.SendMessageInput{
		QueueUrl:    &queueURLStr,
		MessageBody: &messageBodyStr,
	})
	duration := time.Since(startTime)

	if err != nil {
		return Result{
			Success:     false,
			Duration:    duration,
			Index:       index,
			Error:       err.Error(),
			MessageType: messageType,
		}
	}

	return Result{
		Success:     true,
		Duration:    duration,
		Index:       index,
		MessageType: messageType,
	}
}

func main() {
	// signal handling graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// aws
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Unable to load SDK config: %v\n", err)
		os.Exit(1)
	}

	client := sqs.NewFromConfig(cfg)

	p := tea.NewProgram(initialModel(), tea.WithAltScreen())

	// channel to send results to UI
	resultChan := make(chan Result, numberOfMessages)

	go func() {
		jobs := make(chan int, numberOfMessages)
		results := make(chan Result, numberOfMessages)

		var wg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

				for {
					select {
					case index, ok := <-jobs:
						if !ok {
							return
						}

						delay := getMessageDelay(currentPattern, index, numberOfMessages, rng)
						time.Sleep(delay)

						result := sendMessage(ctx, client, rng, index, numberOfMessages)
						results <- result
					case <-ctx.Done():
						return
					}
				}
			}(w)
		}

		// jobs
		go func() {
			for i := 1; i <= numberOfMessages; i++ {
				select {
				case jobs <- i:
				case <-ctx.Done():
					close(jobs)
					return
				}
			}
			close(jobs)
		}()

		// results
		go func() {
			wg.Wait()
			close(results)
		}()

		// forward results to UI
		for result := range results {
			resultChan <- result
		}
		close(resultChan)
	}()

	// forward results to UI
	go func() {
		for result := range resultChan {
			p.Send(resultMsg(result))
		}
		p.Send(completeMsg{})
	}()

	go func() {
		<-sigChan
		cancel()
		p.Quit()
	}()

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error running program: %v\n", err)
		os.Exit(1)
	}
}
