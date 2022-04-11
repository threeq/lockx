package lockx

import (
	"context"
	"sync"
)

type lockerCount struct {
	mux   *sync.Mutex
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

// Mutex 生产一个新锁
func (llf *LocalLockerFactory) Mutex(_ context.Context, options ...Option) (Locker, error) {
	llf.mutex.Lock()
	defer llf.mutex.Unlock()

	meta := new(LockerMeta)
	meta.retryStrategy = NoRetry()
	for _, opt := range options {
		opt(meta)
	}

	if _, ok := llf.mutexes[meta.key]; !ok {
		llf.mutexes[meta.key] = &lockerCount{&sync.Mutex{}, 1}
	} else {
		llf.mutexes[meta.key].count++
	}

	return &LocalLocker{
		key:     meta.key,
		mtx:     llf.mutexes[meta.key].mux,
		factory: llf,
	}, nil
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

// LocalLocker 本地锁
type LocalLocker struct {
	factory *LocalLockerFactory
	key     string
	mtx     *sync.Mutex
}

//Lock 加锁
func (ll *LocalLocker) Lock() error {
	ll.mtx.Lock()
	return nil
}

//Unlock 解锁
func (ll *LocalLocker) Unlock() error {
	ll.factory.Del(ll.key)
	ll.mtx.Unlock()
	return nil
}
