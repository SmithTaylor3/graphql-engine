package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Event represents an event in hdb_catalog.event_log
type Event struct {
	ID          string
	Payload     string
	Status      string // "pending", "processing", "delivered", "error"
	Tries       int
	LockedUntil time.Time
}

// InvocationLog represents an entry in hdb_catalog.event_invocation_logs
type InvocationLog struct {
	EventID      string
	StatusCode   int
	ResponseBody string
	Error        string
}

// Database interface for event operations
type Database interface {
	LockEvent(ctx context.Context, eventID string, duration time.Duration) (*Event, error)
	UpdateEventStatus(ctx context.Context, eventID string, status string, tries int) error
	InsertInvocationLog(ctx context.Context, log InvocationLog) error
}

// WebhookClient interface for sending HTTP requests
type WebhookClient interface {
	Send(ctx context.Context, payload string) (*http.Response, error)
}

// InMemDeliveredCache keeps track of successfully delivered events that failed to update in the DB
type InMemDeliveredCache struct {
	mu sync.RWMutex
	m  map[string]bool
}

func NewInMemDeliveredCache() *InMemDeliveredCache {
	return &InMemDeliveredCache{m: make(map[string]bool)}
}

func (c *InMemDeliveredCache) Add(eventID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[eventID] = true
}

func (c *InMemDeliveredCache) Contains(eventID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[eventID]
}

type EventProcessor struct {
	db             Database
	client         WebhookClient
	deliveredCache *InMemDeliveredCache
	httpTimeout    time.Duration
	dbTimeout      time.Duration
}

func NewEventProcessor(db Database, client WebhookClient, httpTimeout, dbTimeout time.Duration) *EventProcessor {
	return &EventProcessor{
		db:             db,
		client:         client,
		deliveredCache: NewInMemDeliveredCache(),
		httpTimeout:    httpTimeout,
		dbTimeout:      dbTimeout,
	}
}

// ProcessEvent processes a single event with resilience to DB timeouts
func (ep *EventProcessor) ProcessEvent(ctx context.Context, eventID string) error {
	// 0. Check if already successfully delivered in-memory (fallback for persistent DB failure)
	if ep.deliveredCache.Contains(eventID) {
		fmt.Printf("[Processor] Event %s already successfully delivered (in-memory fallback active). Skipping redelivery.\n", eventID)
		return nil
	}

	// 1. Lock Lifetime Management: Lock duration covers HTTP timeout + DB write timeout + retries
	lockDuration := ep.httpTimeout + (ep.dbTimeout * 6) // Sufficient buffer for HTTP timeout and DB retries
	lockCtx, cancel := context.WithTimeout(ctx, ep.dbTimeout)
	event, err := ep.db.LockEvent(lockCtx, eventID, lockDuration)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to lock event: %w", err)
	}

	// Double check in-memory cache after lock in case of race conditions
	if ep.deliveredCache.Contains(eventID) {
		return nil
	}

	// 2. Deliver HTTP payload
	httpCtx, cancel := context.WithTimeout(ctx, ep.httpTimeout)
	resp, err := ep.client.Send(httpCtx, event.Payload)
	cancel()

	isSuccess := false
	statusCode := 0
	var responseErr string

	if err == nil {
		statusCode = resp.StatusCode
		// 2xx and 3xx are considered successful deliveries
		if statusCode >= 200 && statusCode < 400 {
			isSuccess = true
		} else {
			responseErr = fmt.Sprintf("HTTP status code: %d", statusCode)
		}
	} else {
		responseErr = err.Error()
	}

	// 3. Two-Phase State Transition & Resilient DB Update Loop
	if isSuccess {
		// Once HTTP succeeds, we MUST NOT retry the HTTP request.
		// We retry ONLY the database status update.
		ep.deliveredCache.Add(eventID) // Mark in-memory immediately to prevent concurrent/future redelivery by this instance

		dbErr := ep.retryDBUpdate(ctx, eventID, "delivered", event.Tries+1, InvocationLog{
			EventID:    eventID,
			StatusCode: statusCode,
		})
		if dbErr != nil {
			// Persistent DB failure: log critical error, but event remains in deliveredCache
			fmt.Printf("[CRITICAL] Event %s delivered successfully but failed to update database: %v\n", eventID, dbErr)
			return fmt.Errorf("database update failed after successful delivery: %w", dbErr)
		}
	} else {
		// HTTP delivery failed, update status to error/retry
		dbErr := ep.retryDBUpdate(ctx, eventID, "error", event.Tries+1, InvocationLog{
			EventID:      eventID,
			StatusCode:   statusCode,
			Error:        responseErr,
		})
		if dbErr != nil {
			return fmt.Errorf("failed to update event status after failed delivery: %w", dbErr)
		}
	}

	return nil
}

func (ep *EventProcessor) retryDBUpdate(ctx context.Context, eventID string, status string, tries int, log InvocationLog) error {
	backoff := 50 * time.Millisecond
	maxBackoff := 500 * time.Millisecond
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		dbCtx, cancel := context.WithTimeout(ctx, ep.dbTimeout)
		err := ep.db.InsertInvocationLog(dbCtx, log)
		if err == nil {
			err = ep.db.UpdateEventStatus(dbCtx, eventID, status, tries)
		}
		cancel()

		if err == nil {
			return nil
		}

		fmt.Printf("[Processor] Database update failed (attempt %d/%d): %v. Retrying...\n", i+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	return errors.New("max database update retries exceeded")
}

// --- Mock Implementations for Verification ---

type MockDatabase struct {
	mu           sync.Mutex
	events       map[string]*Event
	failAttempts int
	currentFail  int
}

func NewMockDatabase() *MockDatabase {
	return &MockDatabase{
		events: map[string]*Event{
			"event-1": {ID: "event-1", Payload: `{"data": "test"}`, Status: "pending", Tries: 0},
			"event-2": {ID: "event-2", Payload: `{"data": "test"}`, Status: "pending", Tries: 0},
		},
	}
}

func (db *MockDatabase) LockEvent(ctx context.Context, eventID string, duration time.Duration) (*Event, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	event, exists := db.events[eventID]
	if !exists {
		return nil, errors.New("event not found")
	}
	event.LockedUntil = time.Now().Add(duration)
	event.Status = "processing"
	return event, nil
}

func (db *MockDatabase) UpdateEventStatus(ctx context.Context, eventID string, status string, tries int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.currentFail < db.failAttempts {
		db.currentFail++
		return errors.New("database timeout/connection failure")
	}

	event, exists := db.events[eventID]
	if !exists {
		return errors.New("event not found")
	}
	event.Status = status
	event.Tries = tries
	return nil
}

func (db *MockDatabase) InsertInvocationLog(ctx context.Context, log InvocationLog) error {
	// Simulating shared DB connection/timeout behavior
	if db.currentFail < db.failAttempts {
		return errors.New("database timeout/connection failure")
	}
	return nil
}

type MockWebhookClient struct {
	mu            sync.Mutex
	deliveryCount int
}

func (c *MockWebhookClient) Send(ctx context.Context, payload string) (*http.Response, error) {
	c.mu.Lock()
	c.deliveryCount++
	c.mu.Unlock()

	// Simulate successful HTTP delivery
	return &http.Response{
		StatusCode: 200,
	}, nil
}

func main() {
	fmt.Println("Starting Event Processor Verification...")

	// Case 1: Transient DB failure (succeeds on retry)
	fmt.Println("\n--- Case 1: Transient DB Failure ---")
	db1 := NewMockDatabase()
	db1.failAttempts = 2 // Fail first 2 DB updates, then succeed
	client1 := &MockWebhookClient{}
	processor1 := NewEventProcessor(db1, client1, 1*time.Second, 100*time.Millisecond)

	err := processor1.ProcessEvent(context.Background(), "event-1")
	if err != nil {
		fmt.Printf("ProcessEvent failed: %v\n", err)
	} else {
		fmt.Println("ProcessEvent succeeded!")
	}
	fmt.Printf("Webhook delivery count: %d (Expected: 1)\n", client1.deliveryCount)
	fmt.Printf("Final Event Status in DB: %s (Expected: delivered)\n", db1.events["event-1"].Status)

	// Case 2: Persistent DB failure (fails all retries, but prevents redelivery)
	fmt.Println("\n--- Case 2: Persistent DB Failure ---")
	db2 := NewMockDatabase()
	db2.failAttempts = 10 // Fail all 5 retries
	client2 := &MockWebhookClient{}
	processor2 := NewEventProcessor(db2, client2, 1*time.Second, 100*time.Millisecond)

	err = processor2.ProcessEvent(context.Background(), "event-2")
	if err != nil {
		fmt.Printf("ProcessEvent failed as expected: %v\n", err)
	}
	fmt.Printf("Webhook delivery count: %d (Expected: 1)\n", client2.deliveryCount)

	// Attempt to process again to simulate redelivery attempt
	fmt.Println("Attempting redelivery of event-2...")
	err = processor2.ProcessEvent(context.Background(), "event-2")
	if err != nil {
		fmt.Printf("ProcessEvent failed: %v\n", err)
	}
	fmt.Printf("Webhook delivery count after redelivery attempt: %d (Expected: 1 - no duplicate delivery)\n", client2.deliveryCount)
}
