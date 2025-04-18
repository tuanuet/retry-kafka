package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyDistributor manages key distribution for sub-programs
type KeyDistributor struct {
	redis             *redis.Client
	logger            Logger
	pattern           string
	processID         string
	processesKey      string // Prefix for process heartbeat keys (e.g., keydistributor:processes:)
	lastUpdateKey     string // Redis key to store last update timestamp
	rebalanceChannel  string // Redis channel for rebalance notifications
	ttl               time.Duration
	stabilityDuration time.Duration
	lastProcesses     []string   // Cache last known processes for comparison
	lastKeys          []string   // Cache last known keys for comparison
	lastKeysMu        sync.Mutex // Mutex for thread-safe access to lastKeys
}

// Config holds configuration for KeyDistributor
type Config struct {
	RedisClient       *redis.Client // Redis client
	Logger            Logger        // Logger for logging
	Pattern           string
	ProcessID         string
	Prefix            string        // Prefix for keys and channels
	TTL               time.Duration // TTL for process registration
	StabilityDuration time.Duration // Duration to consider system stable
}

// NewKeyDistributor creates a new KeyDistributor
func NewKeyDistributor(config Config) (*KeyDistributor, error) {
	if config.RedisClient == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}
	if config.Prefix == "" {
		return nil, fmt.Errorf("prefix must not be empty")
	}
	if config.Pattern == "" {
		return nil, fmt.Errorf("pattern must not be empty")
	}

	// Test Redis connection
	_, err := config.RedisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	kd := &KeyDistributor{
		redis:             config.RedisClient,
		logger:            config.Logger,
		pattern:           config.Pattern,
		processID:         config.ProcessID,
		processesKey:      config.Prefix + ":processes:",
		lastUpdateKey:     config.Prefix + ":last_update",
		rebalanceChannel:  config.Prefix + ":rebalance",
		ttl:               config.TTL,
		stabilityDuration: config.StabilityDuration,
		lastProcesses:     []string{},
		lastKeys:          []string{},
	}

	// Start periodic check for expired processes
	go kd.checkExpiredProcesses(context.Background())
	go kd.WatchNewKeys(context.Background())
	return kd, nil
}

// checkExpiredProcesses periodically checks for expired processes
func (kd *KeyDistributor) checkExpiredProcesses(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current active processes using KEYS
			processKeys, err := kd.redis.Keys(ctx, kd.processesKey+"*").Result()
			if err != nil {
				continue
			}
			var currentProcesses []string
			for _, key := range processKeys {
				if strings.HasPrefix(key, kd.processesKey) {
					processID := strings.TrimPrefix(key, kd.processesKey)
					currentProcesses = append(currentProcesses, processID)
				}
			}

			// Compare with last known processes
			sort.Strings(currentProcesses)
			sort.Strings(kd.lastProcesses)
			if !kd.equalSlices(currentProcesses, kd.lastProcesses) {
				// Update last update timestamp
				err = kd.redis.Set(ctx, kd.lastUpdateKey, time.Now().Unix(), 0).Err()
				if err != nil {
					continue
				}

				// Publish rebalance event
				err = kd.redis.Publish(ctx, kd.rebalanceChannel, "rebalance").Err()
				if err != nil {
					continue
				}

				// Update last known processes
				kd.lastProcesses = currentProcesses
			}
		}
	}
}

// Register registers the sub-program with the system
func (kd *KeyDistributor) Register(ctx context.Context) error {
	// Set TTL for process heartbeat
	if kd.ttl > 0 {
		err := kd.redis.SetEx(ctx, kd.processesKey+kd.processID, "active", kd.ttl).Err()
		if err != nil {
			return fmt.Errorf("failed to set TTL for process %s: %v", kd.processID, err)
		}
	}

	// Update last update timestamp
	err := kd.redis.Set(ctx, kd.lastUpdateKey, time.Now().Unix(), 0).Err()
	if err != nil {
		return fmt.Errorf("failed to update last update timestamp: %v", err)
	}

	// Publish rebalance event
	err = kd.redis.Publish(ctx, kd.rebalanceChannel, "rebalance").Err()
	if err != nil {
		return fmt.Errorf("failed to publish rebalance event: %v", err)
	}

	return nil
}

func (kd *KeyDistributor) ConsumeRebalance(ctx context.Context) chan struct{} {
	signal := make(chan struct{})
	pubsub := kd.redis.Subscribe(ctx, kd.rebalanceChannel)

	go func() {
		for {
			select {
			case <-ctx.Done():
				pubsub.Close()
				close(signal)
				return
			default:
				msg, err := pubsub.ReceiveMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return // Context canceled
					}
					continue
				}
				if msg.Payload == "rebalance" {
					signal <- struct{}{}
				}
			}
		}
	}()

	return signal
}

// Deregister removes the sub-program from the system
func (kd *KeyDistributor) Deregister(ctx context.Context) error {
	err := kd.redis.Del(ctx, kd.processesKey+kd.processID).Err()
	if err != nil {
		return fmt.Errorf("failed to delete TTL key for process %s: %v", kd.processID, err)
	}

	// Update last update timestamp
	err = kd.redis.Set(ctx, kd.lastUpdateKey, time.Now().Unix(), 0).Err()
	if err != nil {
		return fmt.Errorf("failed to update last update timestamp: %v", err)
	}

	// Publish rebalance event
	err = kd.redis.Publish(ctx, kd.rebalanceChannel, "rebalance").Err()
	if err != nil {
		return fmt.Errorf("failed to publish rebalance event: %v", err)
	}

	return nil
}

// KeepAlive refreshes the TTL for the process
func (kd *KeyDistributor) KeepAlive(ctx context.Context) error {
	if kd.ttl > 0 {
		return kd.redis.SetEx(ctx, kd.processesKey+kd.processID, "active", kd.ttl).Err()
	}
	return nil
}

// IsStable checks if the system is stable (no process changes for stabilityDuration)
func (kd *KeyDistributor) IsStable(ctx context.Context) (bool, error) {
	// Get last update timestamp
	lastUpdateStr, err := kd.redis.Get(ctx, kd.lastUpdateKey).Result()
	if err == redis.Nil {
		return false, nil // No update timestamp yet
	}
	if err != nil {
		return false, fmt.Errorf("failed to get last update timestamp: %v", err)
	}

	lastUpdate, err := strconv.ParseInt(lastUpdateStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid last update timestamp: %v", err)
	}

	// Check if enough time has passed since last update
	if time.Since(time.Unix(lastUpdate, 0)) < kd.stabilityDuration {
		return false, nil
	}

	return true, nil
}

// WatchNewKeys monitors for new keys matching the pattern and publishes rebalance events
func (kd *KeyDistributor) WatchNewKeys(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current keys
			currentKeys, err := kd.GetKeys(ctx, kd.pattern)
			if err != nil {
				continue
			}

			// Compare with last known keys
			kd.lastKeysMu.Lock()
			sort.Strings(currentKeys)
			if !kd.equalSlices(currentKeys, kd.lastKeys) {
				// Update last update timestamp
				err = kd.redis.Set(ctx, kd.lastUpdateKey, time.Now().Unix(), 0).Err()
				if err != nil {
				}

				// Publish rebalance event
				err = kd.redis.Publish(ctx, kd.rebalanceChannel, "rebalance").Err()
				if err != nil {
				}

				// Update last known keys
				kd.lastKeys = currentKeys
			}
			kd.lastKeysMu.Unlock()
		}
	}
}

// equalSlices compares two sorted string slices
func (kd *KeyDistributor) equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// GetMyKeys returns the keys assigned to this sub-program using round-robin
func (kd *KeyDistributor) GetMyKeys(ctx context.Context) ([]string, error) {

	// Get all keys from Redis
	keys, err := kd.GetKeys(ctx, kd.pattern)
	if err != nil {
		return nil, err
	}

	// Get all active processes using KEYS
	processKeys, err := kd.redis.Keys(ctx, kd.processesKey+"*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get process keys: %v", err)
	}
	var processes []string
	for _, key := range processKeys {
		// Extract processID from key (remove processesKey prefix)
		if strings.HasPrefix(key, kd.processesKey) {
			processID := strings.TrimPrefix(key, kd.processesKey)
			processes = append(processes, processID)
		}
	}
	if len(processes) == 0 {
		return nil, nil // No processes, no keys assigned
	}

	// Sort keys and processes for consistent assignment
	sort.Strings(keys)
	sort.Strings(processes)

	// Find index of this process
	var processIndex int
	found := false
	for i, proc := range processes {
		if proc == kd.processID {
			processIndex = i
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}

	// Assign keys using round-robin
	var myKeys []string
	for i, key := range keys {
		if i%len(processes) == processIndex {
			myKeys = append(myKeys, key)
		}
	}
	return myKeys, nil
}

// GetKeys retrieves all keys from Redis matching the pattern using SCAN
func (kd *KeyDistributor) GetKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		result, nextCursor, err := kd.redis.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan keys with pattern %s: %v", pattern, err)
		}
		keys = append(keys, result...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	return keys, nil
}

// Close closes the Redis connection
func (kd *KeyDistributor) Close() error {
	kd.Deregister(context.Background())
	return kd.redis.Close()
}
