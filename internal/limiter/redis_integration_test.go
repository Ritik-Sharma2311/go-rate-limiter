package limiter

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Runs the same exactness check against a real Redis. Skipped unless
// REDIS_ADDR is set, e.g. REDIS_ADDR=localhost:6379 go test ./...
func TestRealRedisDistributedLimitIsExact(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: addr})
	defer admin.Close()
	if err := admin.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis not reachable: %v", err)
	}
	cfg := Config{Capacity: 50, RefillPerSec: 0.1, Prefix: "rltest:"}
	admin.Del(ctx, cfg.Prefix+"shared")

	const replicas, requests = 4, 400
	limiters := make([]*Redis, replicas)
	for i := range limiters {
		c := redis.NewClient(&redis.Options{Addr: addr})
		defer c.Close()
		l, err := NewRedis(c, cfg)
		if err != nil {
			t.Fatal(err)
		}
		limiters[i] = l
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := limiters[i%replicas].Allow(ctx, "shared")
			if err != nil {
				t.Error(err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	admin.Del(ctx, cfg.Prefix+"shared")

	if got := allowed.Load(); got != 50 {
		t.Fatalf("allowed %d, want 50", got)
	}
}
