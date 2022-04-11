package lockx

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/go-redis/redis/v8"
	"io"
	"sync"
	"time"
)

// RedisClient is a minimal client interface
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

//NewRedisLockerFactory 新建 redis 分布式锁工厂
func NewRedisLockerFactory(client RedisClient) LockerFactory {
	return &RedisLockerFactory{client: client}
}

//RedisLockerFactory 分布式工厂锁实现
type RedisLockerFactory struct {
	client RedisClient
	tmp    []byte
	mux    sync.Mutex
}

// Mutex tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (rlf *RedisLockerFactory) Mutex(ctx context.Context, options ...Option) (Locker, error) {
	// meta
	meta := &LockerMeta{
		retryStrategy: NoRetry(),
	}
	for _, opt := range options {
		opt(meta)
	}
	// Create a random token
	token, err := rlf.randomToken()
	if err != nil {
		return nil, err
	}

	return &RedisLocker{
		ctx:    ctx,
		client: rlf.client,
		meta:   meta,
		value:  token,
	}, nil

}

func (rlf *RedisLockerFactory) randomToken() (string, error) {
	rlf.mux.Lock()
	defer rlf.mux.Unlock()

	if len(rlf.tmp) == 0 {
		rlf.tmp = make([]byte, 16)
	}

	if _, err := io.ReadFull(rand.Reader, rlf.tmp); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(rlf.tmp), nil
}

// --------------------------------------------------

const (
	luaRefresh = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`
	luaRelease = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
	luaPTTL    = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")
)

//RedisLocker 分布式锁实现
type RedisLocker struct {
	client RedisClient
	value  string
	meta   *LockerMeta
	ctx    context.Context
}

//Lock 加锁
func (rl *RedisLocker) Lock() error {
	value := rl.value
	retry := rl.meta.retryStrategy

	var timer *time.Timer
	for deadline := time.Now().Add(rl.meta.ttl); time.Now().Before(deadline); {

		ok, err := rl.obtain(rl.meta.key, value, rl.meta.ttl)
		if err != nil {
			return err
		} else if ok {
			// 加锁成功
			return nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			break
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-rl.ctx.Done():
			return rl.ctx.Err()
		case <-timer.C:
		}
	}

	return ErrNotObtained
}

func (rl *RedisLocker) obtain(key, value string, ttl time.Duration) (bool, error) {
	return rl.client.SetNX(rl.ctx, key, value, ttl).Result()
}

//Unlock 解锁
func (rl *RedisLocker) Unlock() error {
	res, err := rl.client.Eval(rl.ctx, luaRelease, []string{rl.meta.key}, rl.value).Result()
	if err != nil {
		return err
	}

	if i, ok := res.(int64); !ok || i != 1 {
		return ErrLockNotHeld
	}
	return nil
}
