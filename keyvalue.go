package redis

import (
	"context"
	"time"

	"github.com/go-msvc/errors"
	"github.com/go-msvc/utils/keyvalue"
)

type KeyValueConfig struct {
	Config
}

func (c *KeyValueConfig) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	return nil
} //KeyValueConfig.Validate()

func (c *KeyValueConfig) Create() (keyvalue.Store, error) {
	redisClient, err := c.Config.Create()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create redis client")
	}
	return kvstore{
		redisClient: redisClient,
	}, nil
}

//implements ms.KeyValueStore using redis backend
type kvstore struct {
	redisClient Client
}

func (kvs kvstore) Get(key string) (interface{}, error) {
	var value interface{}
	if err := kvs.redisClient.GetJson(key, &value); err != nil {
		return nil, errors.Wrapf(err, "cannot get key(%s)", key)
	}
	log.Debugf("got key(%s)=(%T)%+c", key, value, value)
	return value, nil
}

func (kvs kvstore) GetTmpl(key string, tmpl interface{}) (interface{}, error) {
	return nil, errors.Errorf("NYI")
}

func (kvs kvstore) Set(key string, value interface{}) error {
	if value == nil {
		return errors.Errorf("cannot set %s=nil (use delete if necessary)", key)
	}
	if err := kvs.redisClient.SetJson(key, value, 0, false, false, false, false, nil); err != nil {
		return errors.Wrapf(err, "failed to set")
	}
	return nil
}

func (kvs kvstore) CtxGet(ctx context.Context, key string) (interface{}, error) {
	return nil, errors.Errorf("NYI")
}

func (kvs kvstore) CtxGetTmpl(ctx context.Context, key string, tmpl interface{}) (interface{}, error) {
	return nil, errors.Errorf("NYI")
}

func (kvs kvstore) CtxSet(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return errors.Errorf("NYI")
}

func (kvs kvstore) CtxDel(ctx context.Context, key string) error {
	return errors.Errorf("NYI")
}

// newRedis creates a new Redis Comms handler
// func newRedis(config redisConfig, opts *options) (handler, error) {

// 	var r redisHandler

// 	if err := r.init(config, opts); err != nil {
// 		return nil, errors.Wrapf(err,
// 			"Failed to init Redis client")
// 	} // if failed to init

// 	return &r, nil

// } // newRedis()

// // init initialises the Redis Comms handler
// func (r *redisHandler) init(config redisConfig, opts *options) error {

// 	const method = "redisHandler.init"

// 	if r == nil || opts == nil {
// 		return errors.Errorf("Invalid parameters %p.%s ()",
// 			r,
// 			method)
// 	} // if invalid params

// 	if err := validate.Validate(
// 		&config); err != nil {
// 		return errors.Wrapf(err,
// 			"Config invalid")
// 	} // if invalid

// 	var err error

// 	if r.redis, err = redis.NewClient(
// 		config.Client); err != nil {

// 		return errors.Wrapf(err,
// 			"Failed to create Redis client")

// 	} // if failed to init

// 	r.subscriptions = make(map[string]redisSub)
// 	r.replySub = "QR:" + uuid.NewString()

// 	return nil

// } // redisHandler.init()
