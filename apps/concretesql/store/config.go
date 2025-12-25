package store

import "time"

// Config holds configuration parameters for the PageStore and LockManager.
type Config struct {
	// PageSize is the default page size for new databases.
	// Default: 4096
	PageSize int

	// MaxTxBytes is the maximum size of a single FDB transaction in bytes.
	// Writes larger than this will be split into multiple transactions.
	// Default: 9MB (allowing some headroom for metadata)
	MaxTxBytes int

	// LockLeaseDuration is the TTL for distributed locks.
	// If a client crashes, the lock will be released after this duration.
	// Default: 5s
	LockLeaseDuration time.Duration

	// LockHeartbeatInterval is how often we refresh the lock lease.
	// Should be significantly less than LockLeaseDuration (e.g., 1/2 or 1/3).
	// Default: 2s
	LockHeartbeatInterval time.Duration

	// LockPollInterval is how often we retry acquiring a lock when blocked.
	// Default: 50ms
	LockPollInterval time.Duration
}

// DefaultConfig returns the recommended default configuration.
func DefaultConfig() Config {
	return Config{
		PageSize:              4096,
		MaxTxBytes:            9 * 1024 * 1024,
		LockLeaseDuration:     5 * time.Second,
		LockHeartbeatInterval: 2 * time.Second,
		LockPollInterval:      50 * time.Millisecond,
	}
}
