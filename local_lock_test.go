package lockx_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/threeq/lockx"
	"testing"
)

func TestLocalLocker_Lock(t *testing.T) {
	factory := lockx.NewLocalLockerFactory()
	mtx, err := factory.Mutex(context.Background(), lockx.Key("local-lock"))
	assert.Nil(t, err)

	err = mtx.Lock()
	assert.Nil(t, err)

	err = mtx.Unlock()
	assert.Nil(t, err)

}
