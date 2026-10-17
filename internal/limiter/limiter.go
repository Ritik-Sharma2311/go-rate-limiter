// Package limiter implements a distributed token bucket backed by Redis.
package limiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token_bucket.lua
var tokenBucketLua string

var tokenBucket = redis.NewScript(tokenBucketLua)

// Config describes one bucket shape shared by every key.
type Config struct {
	// Capacity is the burst size: the most tokens a bucket can hold.
	Capacity int
	// RefillPerSec is how many tokens are added per second.
	RefillPerSec float64
	// Prefix namespaces keys in Redis. Defaults to "rl:".
	Prefix string
}

// Result is the outcome of one Allow call.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

// Limiter decides whether a request for key may proceed.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}

// Redis is a Limiter whose state lives in Redis, so every process pointing at
// the same Redis enforces one global limit.
type Redis struct {
	client redis.Scripter
	cfg    Config
	ttl    time.Duration
}

// NewRedis validates cfg and returns a Limiter using client.
func NewRedis(client redis.Scripter, cfg Config) (*Redis, error) {
	if cfg.Capacity < 1 {
		return nil, errors.New("limiter: capacity must be at least 1")
	}
	if cfg.RefillPerSec <= 0 {
		return nil, errors.New("limiter: refill rate must be positive")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "rl:"
	}
	// Once idle for a full refill period a bucket is indistinguishable from a
	// fresh one, so the key can expire. Double it as a safety margin.
	full := time.Duration(float64(cfg.Capacity) / cfg.RefillPerSec * float64(time.Second))
	ttl := 2 * full
	if ttl < time.Second {
		ttl = time.Second
	}
	return &Redis{client: client, cfg: cfg, ttl: ttl}, nil
}

// Allow takes one token from key's bucket.
func (r *Redis) Allow(ctx context.Context, key string) (Result, error) {
	res, err := tokenBucket.Run(ctx, r.client, []string{r.cfg.Prefix + key},
		r.cfg.Capacity, r.cfg.RefillPerSec, 1, r.ttl.Milliseconds()).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("limiter: %w", err)
	}
	if len(res) != 3 {
		return Result{}, fmt.Errorf("limiter: unexpected script reply %v", res)
	}
	return Result{
		Allowed:    res[0] == 1,
		Limit:      r.cfg.Capacity,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}

// RetryAfterSeconds rounds d up to whole seconds, as the Retry-After header requires.
func RetryAfterSeconds(d time.Duration) int {
	return int(math.Ceil(d.Seconds()))
}
