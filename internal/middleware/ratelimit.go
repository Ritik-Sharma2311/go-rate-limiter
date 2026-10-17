// Package middleware adapts a limiter.Limiter to net/http.
package middleware

import (
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/Ritik-Sharma2311/go-rate-limiter/internal/limiter"
)

// KeyFunc picks the bucket key for a request.
type KeyFunc func(*http.Request) string

// ClientKey uses the X-API-Key header when present, else the peer IP.
// It deliberately ignores X-Forwarded-For, which clients can forge.
func ClientKey(r *http.Request) string {
	if k := r.Header.Get("X-API-Key"); k != "" {
		return "key:" + k
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

// RateLimit rejects requests over the limit with 429. If the limiter itself
// fails (for example Redis is down) the request is let through: a broken rate
// limiter should not take the whole API down with it.
func RateLimit(l limiter.Limiter, keyFn KeyFunc, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := l.Allow(r.Context(), keyFn(r))
		if err != nil {
			log.Printf("rate limiter unavailable, failing open: %v", err)
			w.Header().Set("X-RateLimit-Status", "bypassed")
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))

		if !res.Allowed {
			w.Header().Set("Retry-After", strconv.Itoa(limiter.RetryAfterSeconds(res.RetryAfter)))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
