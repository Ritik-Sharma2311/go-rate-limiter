# go-rate-limiter

A distributed rate limiter for Go APIs. It implements a token bucket as a single atomic Redis Lua script and ships an `http.Handler` middleware and a small reverse-proxy gateway built on it. Any number of gateway replicas pointed at the same Redis enforce one global limit per client.

## Why a Lua script

A token bucket is read-modify-write: read the tokens, add the refill, subtract one, write back. Done from the application that is a race, and two replicas can both spend the last token. Redis runs a Lua script atomically, so the whole decision is a single step no matter how many gateways are calling.

The script also takes the time from Redis (`TIME`) instead of the calling server. Replicas with drifting clocks would otherwise disagree about how many tokens have refilled.

```
client ──> gateway A ──┐
client ──> gateway B ──┼──> Redis (Lua token bucket, one key per client) 
client ──> gateway C ──┘
              │
              └── allowed ──> upstream
```

## Behaviour

- **Per-client buckets**: keyed by `X-API-Key` when present, else the peer IP. `X-Forwarded-For` is ignored on purpose because clients can forge it.
- **Burst and refill**: `-burst` is bucket capacity, `-rate` is tokens added per second.
- **Response headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining` on every response, plus `Retry-After` (whole seconds) on `429`.
- **Fail open**: if Redis is unreachable or slow, requests pass through and carry `X-RateLimit-Status: bypassed`. The limiter protects the API, it should not be the thing that takes it down. Redis timeouts are 200 ms with no retries so a sick Redis cannot stall traffic.
- **Key expiry**: every bucket gets a TTL of twice its full-refill time, so idle clients do not accumulate in Redis.

## Use as a library

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
lim, _ := limiter.NewRedis(rdb, limiter.Config{Capacity: 50, RefillPerSec: 10})
handler := middleware.RateLimit(lim, middleware.ClientKey, yourHandler)
```

## Run the gateway

```
go run ./cmd/gateway -upstream http://localhost:9000 -redis localhost:6379 -burst 50 -rate 10
```

Or with two replicas, a Redis and a sample upstream:

```
docker compose up --build
```

Then send requests alternately to `localhost:8081` and `localhost:8082`. After the burst, both start returning `429` together because they draw from the same bucket.

## Tests

```
make test          # vet + race detector, uses an in-process Redis (miniredis)
make test-redis    # same, plus the integration test against a real Redis
```

What is covered:

- burst then deny, refill from the server clock, refill capped at capacity, independent keys, key expiry
- 4 limiter clients and 3 gateway handlers sharing one Redis, 400 concurrent requests against a burst of 50: exactly 50 allowed, 350 limited
- headers and `Retry-After`, API-key vs IP keying, fail-open when Redis is down

CI runs the suite twice over: against miniredis, and against a real Redis service container.

## Limits

One Redis is a single point of coordination; Redis Cluster would need keys hashed to one slot per client (they are, since each client is one key). The bucket is token based, so it allows bursts by design; for strictly smooth output use a leaky bucket instead.

## Deploy (free tier)

`render.yaml` deploys the gateway and a Redis (Key Value) instance on Render. `UPSTREAM_URL=demo` makes the gateway answer with a small JSON body itself, so no second service is needed. Settings can also come from `PORT`, `REDIS_URL` and `UPSTREAM_URL`.

`/healthz` bypasses the limiter and Redis, so an uptime monitor (for example UptimeRobot, 5 minute interval) can keep the service awake cheaply. Free hosts sleep when idle, so the first request after a quiet period can take 30-50 seconds.

Behind a hosted proxy every client shares the proxy's IP, so send an `X-API-Key` header to get your own bucket (the limiter deliberately ignores `X-Forwarded-For`, which clients can forge).

## Postman

Import `postman/rate-limiter.postman_collection.json`, set `base_url`, then run **Burst request** in the Collection Runner (60+ iterations, no delay) to watch 200 turn into 429. Change `api_key` for a fresh bucket.
