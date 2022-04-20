package lockx

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/threeq/lockx/timingwheel"
	"io"
	"log"
	"sync"
	"time"
)

// RedisClient is a minimal client interface
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

//NewRedisLockerFactory 新建 redis 分布式锁工厂
func NewRedisLockerFactory(client RedisClient) *RedisLockerFactory {
	return &RedisLockerFactory{
		client:   client,
		mux:      sync.Mutex{},
		tmpCache: make(map[string]*redisL2Locker, 1000),
		tw:       timingwheel.NewTimingWheel(1*time.Second, 3600),
	}
}

//RedisLockerFactory 分布式工厂锁实现
type RedisLockerFactory struct {
	client   RedisClient
	tmp      []byte
	mux      sync.Mutex
	tmpCache map[string]*redisL2Locker
	tw       *timingwheel.TimingWheel
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

	rlf.mux.Lock()
	defer rlf.mux.Unlock()

	// Create a random token
	token, err := rlf.randomToken()
	if err != nil {
		return nil, err
	}

	return &redisLocker{
		ctx:    ctx,
		client: rlf.client,
		meta:   meta,
		value:  token,
		tw:     rlf.tw,
	}, nil

}

// MutexL2 获取针对相同的 key 优化的二级锁
func (rlf *RedisLockerFactory) MutexL2(ctx context.Context, options ...Option) (*redisL2Locker, error) {
	// meta
	meta := &LockerMeta{
		retryStrategy: NoRetry(),
	}
	for _, opt := range options {
		opt(meta)
	}

	rlf.mux.Lock()
	defer rlf.mux.Unlock()

	if l, ok := rlf.tmpCache[meta.key]; ok {
		return l, nil
	}

	// Create a random token
	token, err := rlf.randomToken()
	if err != nil {
		return nil, err
	}

	l := &redisL2Locker{
		mux: sync.Mutex{},
		redisLocker: redisLocker{
			ctx:    ctx,
			client: rlf.client,
			meta:   meta,
			value:  token,
			tw:     rlf.tw,
		},
	}

	// 根据数量定期释放
	if len(rlf.tmpCache) >= 1*10000 {
		rlf.tmpCache = make(map[string]*redisL2Locker, 1000)
	}

	rlf.tmpCache[meta.key] = l

	return l, nil

}

// RWMutex tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (rlf *RedisLockerFactory) RWMutex(ctx context.Context, options ...Option) (RWLocker, error) {
	return nil, nil
}

func (rlf *RedisLockerFactory) randomToken() (string, error) {
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
	luaRefresh = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 2 end`
	luaRelease = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
	luaPTTL    = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")

	// ErrNotSupport is returned when trying to release an inactive lock.
	ErrNotSupport = errors.New("redislock: redisL2Locker.Unlock() Method 不需调用。释放锁请使用 redisL2Locker.Lock() 返回的 UnlockHandler。")
)

//redisLocker 分布式锁实现
type redisLocker struct {
	client  RedisClient
	value   string
	meta    *LockerMeta
	ctx     context.Context
	tw      *timingwheel.TimingWheel
	locking chan struct{}
}

//Lock 加锁
func (rl *redisLocker) Lock() error {
	value := rl.value
	retry := rl.meta.retryStrategy

	var timer <-chan struct{}
	for deadline := time.Now().Add(rl.meta.ttl); time.Now().Before(deadline); {

		ok, err := rl.obtain(rl.meta.key, value, rl.meta.ttl)
		if err != nil {
			return err
		} else if ok {
			// 加锁成功
			rl.startMonitor()
			return nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			break
		}

		timer = rl.tw.After(backoff)

		select {
		case <-rl.ctx.Done():
			return rl.ctx.Err()
		case <-timer:
		}
	}

	return ErrNotObtained
}

func (rl *redisLocker) obtain(key, value string, ttl time.Duration) (bool, error) {
	return rl.client.SetNX(rl.ctx, key, value, ttl).Result()
}

//Unlock 解锁
func (rl *redisLocker) Unlock() error {
	rl.stopMonitor()
	res, err := rl.client.Eval(rl.ctx, luaRelease, []string{rl.meta.key}, rl.value).Result()
	if err != nil {
		return err
	}

	if i, ok := res.(int64); !ok || i != 1 {
		return ErrLockNotHeld
	}
	return nil
}

func formatMs(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		return 1
	}
	return int64(dur / time.Millisecond)
}

func (rl *redisLocker) startMonitor() {
	rl.locking = make(chan struct{})
	go func() {
		for {
			next := rl.tw.After(rl.meta.ttl / 2)
			select {
			case <-rl.locking:
				return
			case <-next:
				println("luaRefresh")
				res, err := rl.client.Eval(rl.ctx, luaRefresh, []string{rl.meta.key}, rl.value, formatMs(rl.meta.ttl)).Result()
				if err != nil {
					log.Printf("[Warning] redis lock lua-refresh key <%s> execute error: %s.\n", rl.meta.key, err.Error())
				} else {
					if ret, ok := res.(int64); ok {
						switch ret {
						case 0:
							log.Printf("[Warning] redis lock lua-refresh key <%s> not exists or not pexpire ttl.\n", rl.meta.key)
						case 2:
							log.Printf("[Warning] redis lock lua-refresh key <%s> invalid and has been obtained by someone else.\n", rl.meta.key)
							return
						}
					}
				}
			}
		}
	}()
}

func (rl *redisLocker) stopMonitor() {
	println("stopMonitor ================")
	if rl.locking != nil {
		close(rl.locking)
		rl.locking = nil
	}
}

//redisL2Locker 分布式锁实现，两级锁
type redisL2Locker struct {
	redisLocker
	mux sync.Mutex
}

//Lock 加锁
func (rl2 *redisL2Locker) Lock() error {
	rl2.mux.Lock()
	err := rl2.redisLocker.Lock()
	if err != nil {
		rl2.mux.Unlock()
	}
	return err
}

//Unlock 释放锁
func (rl2 *redisL2Locker) Unlock() error {
	defer rl2.mux.Unlock()
	return rl2.redisLocker.Unlock()
}
