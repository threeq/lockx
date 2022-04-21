package lockx

import (
	"context"
	"time"
)

// Locker 锁接口
type Locker interface {
	Lock() error
	Unlock() error
}

// RWLocker 读写锁接口
type RWLocker interface {
	Locker
	RLock() error
	RUnlock() error
	RLocker() Locker
}

// LockerFactory 锁创建工厂实现接口
type LockerFactory interface {
	Mutex(context.Context, ...Option) (Locker, error)
	MutexL2(ctx context.Context, options ...Option) (Locker, error)
	RWMutex(context.Context, ...Option) (RWLocker, error)
}

// --------------------------------------------------------------------

//LockerMeta 锁配置元数据
type LockerMeta struct {
	key           string
	ttl           time.Duration
	retryStrategy RetryStrategy
}

//Option 锁配置元数据设置
type Option func(*LockerMeta)

//Key 锁 id
func Key(id string) Option {
	return func(meta *LockerMeta) {
		meta.key = id
	}
}

//TTL 锁过期时间
func TTL(ttl time.Duration) Option {
	return func(meta *LockerMeta) {
		meta.ttl = ttl
	}
}

//Retry 锁重试次数
func Retry(retry RetryStrategy) Option {
	return func(meta *LockerMeta) {
		meta.retryStrategy = retry
	}
}

// --------------------------------------------------------------------

// RetryStrategy allows to customise the lock retry strategy.
type RetryStrategy interface {
	// NextBackoff returns the next backoff duration.
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

//NextBackoff returns the next backoff duration.
func (r linearBackoff) NextBackoff() time.Duration {
	return time.Duration(r)
}

// LinearBackoff allows retries regularly with customized intervals
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

// NoRetry acquire the lock only once.
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

type limitedRetry struct {
	s RetryStrategy

	cnt, max int
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: max}
}

//NextBackoff returns the next backoff duration.
func (r *limitedRetry) NextBackoff() time.Duration {
	if r.cnt >= r.max {
		return 0
	}
	r.cnt++
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint

	min, max time.Duration
}

// ExponentialBackoff strategy is an optimization strategy with a retry time of 2**n milliseconds (n means number of times).
// You can set a minimum and maximum value, the recommended minimum value is not less than 16ms.
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	r.cnt++

	ms := 2 << 25
	if r.cnt < 25 {
		ms = 2 << r.cnt
	}

	if d := time.Duration(ms) * time.Millisecond; d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}

var _factory LockerFactory

func init() {
	_factory = NewLocalLockerFactory()
}

func Init(factory LockerFactory) {
	_factory = factory
}

func Mutex(ctx context.Context, opts ...Option) (Locker, error) {
	return _factory.Mutex(ctx, opts...)
}
func MutexL2(ctx context.Context, opts ...Option) (Locker, error) {
	return _factory.MutexL2(ctx, opts...)
}
func RWMutex(ctx context.Context, opts ...Option) (RWLocker, error) {
	return _factory.RWMutex(ctx, opts...)
}
