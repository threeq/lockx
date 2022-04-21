package lockx

import (
	"context"
	"sync"
)

type lockerCount struct {
	mux   sync.Locker
	count int
}

// LocalLockerFactory 本地锁工厂
type LocalLockerFactory struct {
	mutex   sync.Mutex
	mutexes map[string]*lockerCount
}

var once sync.Once
var localLockerFactory LockerFactory

//NewLocalLockerFactory 获取本地所工厂，单例模式
func NewLocalLockerFactory() LockerFactory {
	once.Do(func() {
		localLockerFactory = &LocalLockerFactory{
			mutexes: make(map[string]*lockerCount),
		}
	})
	return localLockerFactory
}

// Mutex 获取普通锁
func (llf *LocalLockerFactory) Mutex(ctx context.Context, options ...Option) (Locker, error) {
	llf.mutex.Lock()
	defer llf.mutex.Unlock()

	meta := llf.lockerMeta(ctx, &sync.Mutex{}, options...)

	return &localLocker{
		key:     meta.key,
		mtx:     llf.mutexes[meta.key].mux,
		factory: llf,
	}, nil
}

// MutexL2 本地锁没有二级锁概念
func (llf *LocalLockerFactory) MutexL2(ctx context.Context, options ...Option) (Locker, error) {
	return llf.Mutex(ctx, options...)
}

// RWMutex 获取读写锁
func (llf *LocalLockerFactory) RWMutex(ctx context.Context, options ...Option) (RWLocker, error) {
	llf.mutex.Lock()
	defer llf.mutex.Unlock()

	meta := llf.lockerMeta(ctx, &sync.RWMutex{}, options...)

	return &localRWLocker{
		localLocker{
			key:     meta.key,
			mtx:     llf.mutexes[meta.key].mux,
			factory: llf,
		},
	}, nil
}

func (llf *LocalLockerFactory) lockerMeta(ctx context.Context, mtx sync.Locker, options ...Option) *LockerMeta {
	meta := new(LockerMeta)
	meta.retryStrategy = NoRetry()
	for _, opt := range options {
		opt(meta)
	}

	if _, ok := llf.mutexes[meta.key]; !ok {
		llf.mutexes[meta.key] = &lockerCount{mtx, 1}
	} else {
		llf.mutexes[meta.key].count++
	}

	return meta
}

//Del 删除一个锁
func (llf *LocalLockerFactory) Del(key string) {
	llf.mutex.Lock()
	defer llf.mutex.Unlock()

	if lc, ok := llf.mutexes[key]; ok {
		lc.count--
		if lc.count <= 0 {
			delete(llf.mutexes, key)
		}
	}
}

// localLocker 本地锁包装
type localLocker struct {
	factory *LocalLockerFactory
	key     string
	mtx     sync.Locker
}

//Lock 加锁
func (ll *localLocker) Lock() error {
	ll.mtx.Lock()
	return nil
}

//Unlock 解锁
func (ll *localLocker) Unlock() error {
	if ll.factory != nil {
		ll.factory.Del(ll.key)
	}
	ll.mtx.Unlock()
	return nil
}

// localLocker 本地锁包装
type localRWLocker struct {
	localLocker
}

func (l *localRWLocker) RLock() error {
	l.mtx.(*sync.RWMutex).RLock()
	return nil
}

func (l *localRWLocker) RUnlock() error {
	l.mtx.(*sync.RWMutex).RUnlock()
	return nil
}

func (l *localRWLocker) RLocker() Locker {
	return &localLocker{
		mtx: l.mtx.(*sync.RWMutex).RLocker(),
	}
}
