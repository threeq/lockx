package lockx

import (
	"context"
	"sync"
	"time"
)

type Cmder interface {
	Name() string
	FullName() string
	Args() []interface{}
	String() string
	Err() error
}

// RedisClient is a minimal client interface
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) Cmder
	Eval(ctx context.Context, ript string, keys []string, args ...interface{}) Cmder
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) Cmder
	ScriptExists(ctx context.Context, scripts ...string) Cmder
	ScriptLoad(ctx context.Context, script string) Cmder
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

func (r *RedisLockerFactory) Mutex(ctx context.Context, option ...Option) (Locker, error) {
	panic("implement me")
}

//RedisLocker 分布式锁实现
type RedisLocker struct {
	client RedisClient
	value  string
	meta   *LockerMeta
	ctx    context.Context
}
