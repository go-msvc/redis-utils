package redis_test

import (
	"testing"

	"github.com/go-msvc/redis-utils"
)

func TestKV1(t *testing.T) {
	c := redis.KeyValueConfig{
		Config: redis.Config{
			Standalone: &redis.StandaloneConfig{
				Address: "localhost",
				Port:    6379,
			},
		},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("invalid redis config: %+v", err)
	}
	kvs, err := c.Create()
	if err != nil {
		t.Fatalf("failed to create kv store: %+v", err)
	}

	testData := map[string]interface{}{
		"a": 1,
		"b": "two",
		"c": false,
		"d": true,
		"f": 1.2,
	}

	//set all test data
	for n, v := range testData {
		if err := kvs.Set(n, v); err != nil {
			t.Fatalf("failed to set \"%s\"=(%T)%+v: %+v", n, v, v, err)
		}
	}

	//get all test data
	for n, expValue := range testData {
		gotValue, err := kvs.Get(n)
		if err != nil {
			t.Fatalf("failed to get \"%s\": %+v", n, err)
		}
		if gotValue != expValue {
			t.Fatalf("\"%s\"->(%T)%+v != (%T)%+v", n, gotValue, gotValue, expValue, expValue)
		}
	}
}
