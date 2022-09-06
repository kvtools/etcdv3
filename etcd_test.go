package etcdv3

import (
	"context"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testsuite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 60 * time.Second

const client = "localhost:4001"

func makeEtcdV3Client(t *testing.T) store.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config := &Config{
		ConnectionTimeout: 3 * time.Second,
		Username:          "test",
		Password:          "very-secure",
	}

	kv, err := New(ctx, []string{client}, config)
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestEtcdV3Register(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kv, err := valkeyrie.NewStore(ctx, StoreName, []string{client}, nil)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	assert.IsTypef(t, kv, new(Store), "Error registering and initializing etcd with v3 client")
}

func TestEtcdV3Store(t *testing.T) {
	kv := makeEtcdV3Client(t)
	lockKV := makeEtcdV3Client(t)
	ttlKV := makeEtcdV3Client(t)

	t.Cleanup(func() {
		testsuite.RunCleanup(t, kv)
	})

	testsuite.RunTestCommon(t, kv)
	testsuite.RunTestAtomic(t, kv)
	testsuite.RunTestWatch(t, kv)
	testsuite.RunTestLock(t, kv)
	testsuite.RunTestLockTTL(t, kv, lockKV)
	testsuite.RunTestListLock(t, kv)
	testsuite.RunTestTTL(t, kv, ttlKV)
}

func TestEtcdV3KeepAlive(t *testing.T) {
	kv := makeEtcdV3Client(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	t.Cleanup(func() {
		_ = kv.Delete(ctx, "foo")
	})

	err := kv.Put(ctx, "foo", []byte("bar"), &store.WriteOptions{
		TTL: 1 * time.Second,
	})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// The key should be gone because we didn't use KeepAlive.
	pair, err := kv.Get(ctx, "foo", nil)
	assert.Error(t, err, store.ErrKeyNotFound)
	assert.Nil(t, pair)

	// Put the key but now with a KeepAlive.
	err = kv.Put(ctx, "foo", []byte("bar"), &store.WriteOptions{
		TTL:       1 * time.Second,
		KeepAlive: true,
	})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// We should still be able to get the key after the TTL expires.
	pair, err = kv.Get(ctx, "foo", nil)
	require.NoError(t, err)
	assert.Equal(t, pair.Value, []byte("bar"))
}
