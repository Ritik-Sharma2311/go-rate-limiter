// Command gateway is a reverse proxy that rate limits clients with a Redis
// backed token bucket shared across all gateway replicas.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Ritik-Sharma2311/go-rate-limiter/internal/limiter"
	"github.com/Ritik-Sharma2311/go-rate-limiter/internal/middleware"
)

func main() {
	listen := flag.String("listen", listenDefault(), "listen address (defaults to :$PORT when PORT is set)")
	upstream := flag.String("upstream", env("UPSTREAM_URL", "http://localhost:9000"), "upstream base URL; \"demo\" serves a built-in JSON reply")
	redisAddr := flag.String("redis", env("REDIS_ADDR", "localhost:6379"), "redis address, or a redis:// URL (also read from REDIS_URL)")
	burst := flag.Int("burst", 50, "bucket capacity (burst size)")
	rate := flag.Float64("rate", 10, "tokens refilled per second")
	flag.Parse()

	opts := &redis.Options{Addr: *redisAddr}
	if u := os.Getenv("REDIS_URL"); u != "" {
		*redisAddr = u
	}
	if strings.HasPrefix(*redisAddr, "redis://") || strings.HasPrefix(*redisAddr, "rediss://") {
		parsed, err := redis.ParseURL(*redisAddr)
		if err != nil {
			log.Fatalf("bad redis URL: %v", err)
		}
		opts = parsed
	}
	opts.DialTimeout = 200 * time.Millisecond
	opts.ReadTimeout = 200 * time.Millisecond
	opts.MaxRetries = -1 // a slow Redis must not stall requests; fail open instead
	rdb := redis.NewClient(opts)
	lim, err := limiter.NewRedis(rdb, limiter.Config{Capacity: *burst, RefillPerSec: *rate})
	if err != nil {
		log.Fatal(err)
	}

	var next http.Handler
	if *upstream == "demo" {
		next = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}` + "\n"))
		})
	} else {
		target, err := url.Parse(*upstream)
		if err != nil {
			log.Fatalf("bad -upstream: %v", err)
		}
		next = httputil.NewSingleHostReverseProxy(target)
	}
	limited := middleware.RateLimit(lim, middleware.ClientKey, next)
	// The health check bypasses the limiter and Redis, so uptime pings are cheap.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/", limited)
	handler := http.Handler(mux)
	srv := &http.Server{Addr: *listen, Handler: handler}

	go func() {
		log.Printf("gateway on %s -> %s (burst=%d rate=%.1f/s redis=%s)", *listen, *upstream, *burst, *rate, *redisAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = rdb.Close()
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func listenDefault() string {
	if p := os.Getenv("PORT"); p != "" {
		return ":" + p
	}
	return ":8080"
}
