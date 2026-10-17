package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Ritik-Sharma2311/go-rate-limiter/internal/limiter"
)

var ok = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

func newLimiter(t *testing.T, mr *miniredis.Miniredis, burst int) limiter.Limiter {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	l, err := limiter.NewRedis(c, limiter.Config{Capacity: burst, RefillPerSec: 0.01})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func do(h http.Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:5555"
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHeadersAnd429(t *testing.T) {
	mr := miniredis.RunT(t)
	h := RateLimit(newLimiter(t, mr, 2), ClientKey, ok)

	rec := do(h, "")
	if rec.Code != http.StatusOK || rec.Header().Get("X-RateLimit-Limit") != "2" || rec.Header().Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first: code %d headers %v", rec.Code, rec.Header())
	}
	do(h, "")
	rec = do(h, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("remaining %q, want 0", rec.Header().Get("X-RateLimit-Remaining"))
	}
}

func TestClientKeyPrefersAPIKeyOverIP(t *testing.T) {
	mr := miniredis.RunT(t)
	h := RateLimit(newLimiter(t, mr, 1), ClientKey, ok)

	if do(h, "alice").Code != http.StatusOK {
		t.Fatal("alice first call")
	}
	if do(h, "alice").Code != http.StatusTooManyRequests {
		t.Fatal("alice second call should be limited")
	}
	if do(h, "bob").Code != http.StatusOK {
		t.Fatal("bob has his own bucket even from the same IP")
	}
}

func TestFailsOpenWhenRedisDown(t *testing.T) {
	mr := miniredis.RunT(t)
	l := newLimiter(t, mr, 1)
	mr.Close()

	h := RateLimit(l, ClientKey, ok)
	for i := 0; i < 3; i++ {
		rec := do(h, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("call %d: want pass-through, got %d", i, rec.Code)
		}
		if rec.Header().Get("X-RateLimit-Status") != "bypassed" {
			t.Fatal("bypass should be flagged in a header")
		}
	}
}

// Three gateway handlers in front of one Redis must admit exactly `burst`
// requests out of a concurrent flood, regardless of which replica gets each.
func TestReplicasShareOneGlobalLimit(t *testing.T) {
	const (
		burst    = 50
		requests = 400
		replicas = 3
	)
	mr := miniredis.RunT(t)

	handlers := make([]http.Handler, replicas)
	for i := range handlers {
		handlers[i] = RateLimit(newLimiter(t, mr, burst), ClientKey, ok)
	}

	var allowed, limited atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch do(handlers[i%replicas], "tenant-1").Code {
			case http.StatusOK:
				allowed.Add(1)
			case http.StatusTooManyRequests:
				limited.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if allowed.Load() != burst || limited.Load() != requests-burst {
		t.Fatalf("allowed=%d limited=%d, want %d/%d", allowed.Load(), limited.Load(), burst, requests-burst)
	}
}
