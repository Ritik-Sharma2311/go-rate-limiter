package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestBurstThenDeny(t *testing.T) {
	mr := miniredis.RunT(t)
	l, err := NewRedis(newClient(t, mr.Addr()), Config{Capacity: 5, RefillPerSec: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		res, err := l.Allow(ctx, "u1")
		if err != nil || !res.Allowed {
			t.Fatalf("call %d: allowed=%v err=%v", i, res.Allowed, err)
		}
		if want := 4 - i; res.Remaining != want {
			t.Fatalf("call %d: remaining %d, want %d", i, res.Remaining, want)
		}
	}
	res, err := l.Allow(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("6th call should be denied")
	}
	if res.RetryAfter <= 0 || res.RetryAfter > time.Second {
		t.Fatalf("retry-after %v, want within (0, 1s]", res.RetryAfter)
	}
}

func TestRefillUsesServerClock(t *testing.T) {
	mr := miniredis.RunT(t)
	base := time.Unix(1_700_000_000, 0)
	mr.SetTime(base)
	l, _ := NewRedis(newClient(t, mr.Addr()), Config{Capacity: 5, RefillPerSec: 1})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		l.Allow(ctx, "u1")
	}
	if res, _ := l.Allow(ctx, "u1"); res.Allowed {
		t.Fatal("bucket should be empty")
	}

	mr.SetTime(base.Add(2 * time.Second)) // two tokens refill
	for i := 0; i < 2; i++ {
		if res, _ := l.Allow(ctx, "u1"); !res.Allowed {
			t.Fatalf("call %d after refill should be allowed", i)
		}
	}
	if res, _ := l.Allow(ctx, "u1"); res.Allowed {
		t.Fatal("only two tokens should have refilled")
	}
}

func TestRefillCapsAtCapacity(t *testing.T) {
	mr := miniredis.RunT(t)
	base := time.Unix(1_700_000_000, 0)
	mr.SetTime(base)
	l, _ := NewRedis(newClient(t, mr.Addr()), Config{Capacity: 3, RefillPerSec: 10})
	ctx := context.Background()

	l.Allow(ctx, "u1")
	mr.SetTime(base.Add(time.Hour))
	res, _ := l.Allow(ctx, "u1")
	if !res.Allowed || res.Remaining != 2 {
		t.Fatalf("want allowed with 2 remaining (capped at 3), got %+v", res)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	mr := miniredis.RunT(t)
	l, _ := NewRedis(newClient(t, mr.Addr()), Config{Capacity: 1, RefillPerSec: 0.01})
	ctx := context.Background()

	if res, _ := l.Allow(ctx, "a"); !res.Allowed {
		t.Fatal("a first call")
	}
	if res, _ := l.Allow(ctx, "a"); res.Allowed {
		t.Fatal("a second call should be denied")
	}
	if res, _ := l.Allow(ctx, "b"); !res.Allowed {
		t.Fatal("b must have its own bucket")
	}
}

func TestBucketKeyExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	l, _ := NewRedis(newClient(t, mr.Addr()), Config{Capacity: 10, RefillPerSec: 10})
	l.Allow(context.Background(), "u1")

	if ttl := mr.TTL("rl:u1"); ttl <= 0 {
		t.Fatalf("bucket should carry a TTL, got %v", ttl)
	}
	mr.FastForward(time.Minute)
	if mr.Exists("rl:u1") {
		t.Fatal("idle bucket should have expired")
	}
}

// Several limiter instances (one per simulated gateway replica, each with its
// own connection) share one Redis and must enforce a single global limit.
func TestDistributedLimitIsExact(t *testing.T) {
	const (
		replicas = 4
		burst    = 50
		requests = 400
	)
	mr := miniredis.RunT(t)
	cfg := Config{Capacity: burst, RefillPerSec: 0.1}

	limiters := make([]*Redis, replicas)
	for i := range limiters {
		l, err := NewRedis(newClient(t, mr.Addr()), cfg)
		if err != nil {
			t.Fatal(err)
		}
		limiters[i] = l
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	ctx := context.Background()
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

	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed %d of %d, want exactly %d", got, requests, burst)
	}
}

func TestNewRedisValidatesConfig(t *testing.T) {
	if _, err := NewRedis(nil, Config{Capacity: 0, RefillPerSec: 1}); err == nil {
		t.Fatal("zero capacity should fail")
	}
	if _, err := NewRedis(nil, Config{Capacity: 1, RefillPerSec: 0}); err == nil {
		t.Fatal("zero rate should fail")
	}
}
