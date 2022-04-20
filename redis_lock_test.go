package lockx_test

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/threeq/lockx"
	"log"
	"testing"
	"time"
)

var mr *miniredis.Miniredis
var lf lockx.LockerFactory
var redisLF *lockx.RedisLockerFactory
var client *redis.Client

func init() {
	//var err error
	//mr, err = miniredis.Run()
	//if err != nil {
	//	panic(any(err))
	//}

	addr := "127.0.0.1:6379"
	//addr := mr.Addr()
	fmt.Println("memory redis addr: ", addr)
	client = redis.NewClient(&redis.Options{
		Addr: addr,
		//Password: "!13#c%b^*a",
	})
	redisLF = lockx.NewRedisLockerFactory(client)
	lf = redisLF
}

func TestRedisLocker_All(t *testing.T) {

	mtx, err := lf.Mutex(context.Background(), lockx.Key("redis-lock"), lockx.TTL(10*time.Second))

	assert.Nil(t, err)

	log.Println(1, "加锁")
	err = mtx.Lock()
	assert.Nil(t, err)
	log.Println(1, "加锁完成")

	s, err := client.Get(context.Background(), "redis-lock").Result()
	assert.Nil(t, err)
	assert.NotEmpty(t, s)
	log.Println(1, s)

	go func() {
		innerMtx, err := lf.Mutex(context.Background(),
			lockx.Key("redis-lock"),
			lockx.TTL(20*time.Second),
			lockx.Retry(lockx.LimitRetry(lockx.LinearBackoff(1*time.Second), 10)),
		)
		assert.Nil(t, err)

		log.Println(2, "加锁")
		err = innerMtx.Lock()
		if err != nil {
			log.Println(2, err)
		}
		log.Println(2, "加锁完成")
		s, err := client.Get(context.Background(), "redis-lock").Result()
		assert.Nil(t, err)
		assert.NotEmpty(t, s)
		log.Println(2, s)

		time.Sleep(15 * time.Second)

		log.Println(2, "释放锁")
		err = innerMtx.Unlock()
		assert.Nil(t, err)
		log.Println(2, "释放锁完成")

		s, err = client.Get(context.Background(), "redis-lock").Result()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, redis.Nil)
		assert.Empty(t, s)
	}()

	time.Sleep(8 * time.Second)
	log.Println(1, "释放锁")
	err = mtx.Unlock()
	assert.Nil(t, err)
	log.Println(1, "释放锁完成")

	time.Sleep(30 * time.Second)

	s, err = client.Get(context.Background(), "redis-lock").Result()
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, redis.Nil)
	assert.Empty(t, s)
}

func TestOptimizeRedisLocker_Lock(t *testing.T) {
	defer mr.Close()

	_, err := redisLF.MutexL2(context.Background(), lockx.Key("redis-lock"), lockx.TTL(3*time.Second))

	assert.Nil(t, err)

}
