package lockx_test

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/threeq/lockx"
	"testing"
	"time"
)

var mr *miniredis.Miniredis
var lf lockx.LockerFactory

func init() {
	var err error
	mr, err = miniredis.Run()
	if err != nil {
		panic(any(err))
	}

	//addr := "10.66.173.3:6379"
	addr := mr.Addr()
	fmt.Println("memory redis addr: ", addr)
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		//Password: "!13#c%b^*a",
	})
	lf = lockx.NewRedisLockerFactory(client)

}

func TestRedisLocker_All(t *testing.T) {
	defer mr.Close()

	mtx, err := lf.Mutex(context.Background(), lockx.Key("redis-lock"), lockx.TTL(3*time.Second))

	assert.Nil(t, err)

	err = mtx.Lock()
	assert.Nil(t, err)

	err = mtx.Unlock()
	assert.Nil(t, err)
}
