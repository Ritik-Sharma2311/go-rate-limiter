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
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Ritik-Sharma2311/go-rate-limiter/internal/limiter"
	"github.com/Ritik-Sharma2311/go-rate-limiter/internal/middleware"
)

func main() {
	listen := flag.String("listen", ":8080", "listen address")
	upstream := flag.String("upstream", "http://localhost:9000", "upstream base URL")
	redisAddr := flag.String("redis", "localhost:6379", "redis address")
	burst := flag.Int("burst", 50, "bucket capacity (burst size)")
	rate := flag.Float64("rate", 10, "tokens refilled per second")
	flag.Parse()

	target, err := url.Parse(*upstream)
	if err != nil {
		log.Fatalf("bad -upstream: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:        *redisAddr,
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
		MaxRetries:  -1, // a slow Redis must not stall requests; fail open instead
	})
	lim, err := limiter.NewRedis(rdb, limiter.Config{Capacity: *burst, RefillPerSec: *rate})
	if err != nil {
		log.Fatal(err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	handler := middleware.RateLimit(lim, middleware.ClientKey, proxy)
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
