package redis

import (
	"encoding/json"
	"math"
	"time"

	"github.com/go-msvc/errors"
)

type Client interface {
	Connector
	BLPop(timeout time.Duration, keys ...string) (string, []byte, error)
	BRPop(timeout time.Duration, keys ...string) (string, []byte, error)
	Decr(key string) (int64, error)
	DecrBy(key string, decrement int64) (int64, error)
	Del(keys ...string) (int64, error)
	Eval(script string, keys []string, args []interface{}) (interface{}, error)
	Exists(keys ...string) (int64, error)
	Expire(key string, expire time.Duration, option ExpireOption) (bool, error)
	Get(key string) ([]byte, error)
	GetDel(key string) ([]byte, int64, error)
	GetJson(key string, value interface{}) error
	Incr(key string) (int64, error)
	IncrBy(key string, increment int64) (int64, error)
	IncrByAndExpire(key string, increment int64, e time.Duration, o ExpireOption) (int64, bool, error)
	LLen(key string) (int64, error)
	LPop(key string, count int64) ([][]byte, error)
	LPush(key string, values ...[]byte) (int64, error)
	LPushAndExpire(key string, expire time.Duration, values ...[]byte) (int64, bool, error)
	LPushX(key string, values ...[]byte) (int64, error)
	LRem(key string, count int, element string) (int64, error)
	PExpire(key string, expire time.Duration, option ExpireOption) (bool, error)
	Ping(message string) (string, error)
	PubSubChannels(pattern string) ([]string, error)
	PubSubNumPat() (int64, error)
	PubSubNumSub(channels ...string) ([]NumSub, error)
	Publish(channel string, message string) (int64, error)
	RPop(key string, count int64) ([][]byte, error)
	RPush(key string, values ...[]byte) (int64, error)
	RPushAndExpire(key string, expire time.Duration, values ...[]byte) (int64, bool, error)
	RPushX(key string, values ...[]byte) (int64, error)
	Set(key string, value []byte, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool, get bool) ([]byte, error)
	SetGet(key string, value []byte, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool) ([]byte, bool, error)
	SetJson(key string, value interface{}, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool, get bool, oldValue interface{}) error
	ZCard(key string) (int64, error)
	ZPopMax(key string, count int64) ([]SortedSetElem, error)
	ZPopMin(key string, count int64) ([]SortedSetElem, error)
	ZRem(key string, members ...string) (int64, error)
	ZRemRangeByScore(key string, min string, max string) (int64, error)
}

type client struct {
	Connector
}

// ExpireOption represents the options that can be used by the EXPIRE and
// PEXPIRE commands
type ExpireOption string

type NumSub struct {
	Channel string
	NumSub  int64
} // NumSub

type SortedSetElem struct {
	Member string
	Score  string
} // SortedSetElem

// BLPop performs blocking list pop
func (c client) BLPop(timeout time.Duration, keys ...string) (string, []byte, error) {
	if len(keys) <= 0 {
		return "", nil, errors.Errorf("missing keys")
	}
	key, value, err := c.bPop("BLPOP", timeout, keys...)
	if err != nil {
		return "", nil, errors.Wrapf(err, "BLPOP failed")
	}
	return key, value, nil
} //commands.BLPop()

// BRPop performs blocking list pop
func (c client) BRPop(timeout time.Duration, keys ...string) (string, []byte, error) {
	if len(keys) <= 0 {
		return "", nil, errors.Errorf("missing keys")
	}
	key, value, err := c.bPop("BRPOP", timeout, keys...)
	if err != nil {
		return "", nil, errors.Wrapf(err, "BRPOP failed")
	}
	return key, value, nil
} //commands.BRPop()

// Decr decrements the number stored at key by one
func (c client) Decr(key string) (int64, error) {
	resp, err := c.Do("DECR", key)
	if err != nil {
		return -1, errors.Wrapf(err, "DECR failed")
	}
	return resp.(int64), nil
} //commands.Decr()

//DecrBy decrements the number stored at key by decrement
func (c client) DecrBy(key string, decrement int64) (int64, error) {
	resp, err := c.Do("DECRBY", key, decrement)
	if err != nil {
		return -1, errors.Wrapf(err, "DECRBY failed")
	}
	return resp.(int64), nil
} //commands.DecrBy()

// Del deletes the keys from redis, and returns the number of keys deleted
func (c client) Del(keys ...string) (int64, error) {
	if len(keys) <= 0 {
		return 0, errors.Errorf("missing keys")
	}
	var err error
	var n interface{}
	if len(keys) == 1 {
		n, err = c.Do("DEL", keys[0])
	} else {
		args := make([]interface{}, len(keys))
		for i, k := range keys {
			args[i] = k
		} // for each key
		n, err = c.Do("DEL", args...)
	} //if args
	if err != nil {
		return 0, errors.Wrapf(err, "DEL(%v) failed", keys)
	} //if failed to get key
	numDel, _ := n.(int64)
	return numDel, nil
} //commands.Del()

// Eval invokes the execution of a server-side Lua script
func (c client) Eval(script string, keys []string, args []interface{}) (interface{}, error) {
	var a = make([]interface{}, 2, 1+1+len(keys)+len(args))
	a[0] = script
	a[1] = len(keys)
	for _, key := range keys {
		a = append(a, key)
	}
	for _, arg := range args {
		a = append(a, arg)
	}
	ret, err := c.Do("EVAL", a...)
	if err != nil {
		return nil, errors.Wrapf(err, "EVAL failed")
	}
	return ret, nil
} //commands.Eval()

// Exists returns the number of keys that exist from those specified as
// arguments
func (c client) Exists(keys ...string) (int64, error) {
	if len(keys) <= 0 {
		return 0, nil
	}
	var n interface{}
	var err error
	if len(keys) == 1 {
		n, err = c.Do("EXISTS", keys[0])
	} else {
		var args = make([]interface{}, len(keys))
		for i, key := range keys {
			args[i] = key
		} // for each key
		n, err = c.Do("EXISTS", args...)
	} //if more values
	if err != nil {
		return -1, errors.Wrapf(err, "EXISTS failed")
	} //if failed
	return n.(int64), nil
} //commands.Exists()

// Expire sets a timeout on key. expire is rounded up to the nearest second
func (c client) Expire(key string, expire time.Duration, option ExpireOption) (bool, error) {
	set, err := c.expire("EXPIRE", key, int64(math.Ceil(expire.Seconds())), option)
	if err != nil {
		return false, errors.Wrapf(err, "EXPIRE failed")
	}
	return set, nil
} //commands.Expire()

//Get a value from Redis
func (c client) Get(key string) ([]byte, error) {
	value, err := c.Do("GET", key)
	if err != nil {
		return nil, errors.Wrapf(err, "GET failed")
	}
	if value == nil {
		return nil, nil
	}
	return value.([]byte), nil
} //commands.Get()

// GetDel gets and deletes a key from Redis atomically
func (c client) GetDel(key string) ([]byte, int64, error) {
	commands := make([]command, 0, 2)
	commands = append(commands, command{
		Command: "GET",
		Args:    []interface{}{key},
	})
	commands = append(commands, command{
		Command: "DEL",
		Args:    []interface{}{key},
	})
	reply, err := c.Multi(key, commands)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "MULTI failed")
	}
	if len(reply) != len(commands) {
		return nil, 0, errors.Errorf("Unexpected number of responses %d. Expected %d", len(reply), len(commands))
	}
	return reply[0].([]byte), reply[1].(int64), nil
} //commands.GetDel()

// GetJson gets a JSON value from Redis, unmarshalled into value
func (c client) GetJson(key string, value interface{}) error {
	b, err := c.Get(key)
	if err != nil {
		return errors.Wrapf(err, "failed to get key %s", key)
	}
	if b == nil {
		return ErrKeyNotFound
	}
	if err := json.Unmarshal(b, value); err != nil {
		return errors.Wrapf(err, "Failed to unmarshal JSON %s", b)
	}
	return nil
} //commands.GetJson()

// Incr increments the number stored at key by one
func (c client) Incr(key string) (int64, error) {
	resp, err := c.Do("INCR", key)
	if err != nil {
		return -1, errors.Wrapf(err, "INCR failed")
	}
	return resp.(int64), nil
} //commands.Incr()

// IncrBy increments the number stored at key by increment
func (c client) IncrBy(key string, increment int64) (int64, error) {
	resp, err := c.Do("INCRBY", key, increment)
	if err != nil {
		return -1, errors.Wrapf(err, "INCRBY failed")
	}
	return resp.(int64), nil
} //commands.IncrBy()

// IncrByAndExpire increments the number stored at key by increment, and
// returns the value a key.
func (c client) IncrByAndExpire(key string, increment int64, e time.Duration, o ExpireOption) (int64, bool, error) {
	option := string(o)
	commands := make([]command, 0, 2)
	commands = append(commands, command{
		Command: "INCRBY",
		Args:    []interface{}{key, increment},
	}) // incrby
	expire := e.Milliseconds()
	if len(option) <= 0 {
		commands = append(commands, command{
			Command: "PEXPIRE",
			Args:    []interface{}{key, expire},
		})
	} else {
		commands = append(commands, command{
			Command: "PEXPIRE",
			Args:    []interface{}{key, expire, option},
		})
	} // if option
	reply, err := c.Multi(key, commands)
	if err != nil {
		return -1, false, errors.Wrapf(err, "MULTI failed")
	}
	if len(reply) != len(commands) {
		return -1, false, errors.Errorf("Unexpected number of responses %d. Expected %d", len(reply), len(commands))
	}
	return reply[0].(int64), reply[1].(int64) != 0, nil
} //commands.IncrByAndExpire()

// LLen returns the length of the list stored at key. If key does not exist, it
// is interpreted as an empty list and 0 is returned. An error is returned when
// the value stored at key is not a list.
func (c client) LLen(key string) (int64, error) {
	listLen, err := c.Do("LLEN", key)
	if err != nil {
		return -1, errors.Wrapf(err, "LLEN failed")
	}
	return listLen.(int64), nil
} //commands.LLen()

// LPop performs a left list pop
func (c client) LPop(key string, count int64) ([][]byte, error) {
	values, err := c.pop("LPOP", key, count)
	if err != nil {
		return nil, errors.Wrapf(err, "LPOP failed")
	}
	return values, nil
} //commands.LPop()

// LPush inserts all the specified values at the head of the list stored at
// key. Returns the list length.
func (c client) LPush(key string, values ...[]byte) (int64, error) {
	if len(values) <= 0 {
		return -1, errors.Errorf("missing values")
	}
	listLen, err := c.push("LPUSH", key, values...)
	if err != nil {
		return -1, errors.Wrapf(err, "LPUSH failed")
	}
	return listLen, nil
} //commands.LPush()

// LPushAndExpire inserts all the specified values at the tail of the list stored at
// key. Returns the list length.
func (c client) LPushAndExpire(key string, expire time.Duration, values ...[]byte) (int64, bool, error) {
	if len(values) <= 0 {
		return -1, false, errors.Errorf("missing values")
	}
	listLen, expirySet, err := c.pushAndExpire("LPUSH", key, expire, values...)
	if err != nil {
		return -1, false, errors.Wrapf(err, "LPUSH failed")
	}
	return listLen, expirySet, nil
} //commands.LPushAndExpire()

// LPushX inserts all the specified values at the head of the list stored at
// key only if the list exists. Returns the list length.
func (c client) LPushX(key string, values ...[]byte) (int64, error) {
	if len(values) <= 0 {
		return -1, errors.Errorf("missing values")
	}
	listLen, err := c.push("LPUSHX", key, values...)
	if err != nil {
		return -1, errors.Wrapf(err, "LPUSHX failed")
	}
	return listLen, nil
} //commands.LPushX()

// LRem Removes the first count occurrences of elements equal to element from
// the list stored at key. The count argument influences the operation in the
// following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
func (c client) LRem(key string, count int, element string) (int64, error) {
	if len(key) <= 0 {
		return -1, errors.Errorf("missing keys")
	}
	numElements, err := c.Do("LREM", key, count, element)
	if err != nil {
		return -1, errors.Wrapf(err, "LREM failed")
	}
	return numElements.(int64), nil
} //commands.LRem()

// PExpire sets a timeout on key
func (c client) PExpire(key string, expire time.Duration, option ExpireOption) (bool, error) {
	set, err := c.expire("PEXPIRE", key, expire.Milliseconds(), option)
	if err != nil {
		return false, errors.Wrapf(err, "PEXPIRE failed")
	}
	return set, nil
} //commands.PExpire()

// Ping sends a redis ping command
func (c client) Ping(message string) (string, error) {
	res, err := c.Do("PING", message)
	if err != nil {
		return "", errors.Wrapf(err, "PING failed")
	}
	var b []byte
	if res != nil {
		b, _ = res.([]byte)
	}
	return string(b), nil
} //commands.Ping()

func (c client) PubSubChannels(pattern string) ([]string, error) {
	resp, err := c.Do("PUBSUB", "CHANNELS", pattern)
	if err != nil {
		return nil, errors.Wrapf(err, "PUBSUB CHANNELS failed")
	}
	channels, _ := resp.([]interface{})
	retChannels := make([]string, 0, len(channels))
	for _, ch := range channels {
		b, _ := ch.([]byte)
		retChannels = append(retChannels, string(b))
	}
	return retChannels, nil
} //commands.PubSubChannels()

func (c client) PubSubNumPat() (int64, error) {
	n, err := c.Do("PUBSUB", "NUMPAT")
	if err != nil {
		return -1, errors.Wrapf(err, "PUBSUB NUMPAT failed")
	}
	num, _ := n.(int64)
	return num, nil
} //commands.PubSubNumPat()

func (c client) PubSubNumSub(channels ...string) ([]NumSub, error) {
	resp, err := c.Do("PUBSUB", "NUMSUB", channels)
	if err != nil {
		return nil, errors.Wrapf(err, "PUBSUB NUMSUB failed")
	}
	numSubs, _ := resp.([]interface{})
	if len(numSubs)%2 != 0 {
		return nil, errors.Errorf("Expected size %d to be even",
			len(numSubs))
	}
	retNumSubs := make([]NumSub, len(numSubs)/2)
	for i := 0; i < len(numSubs); i += 2 {
		var n NumSub
		b, _ := numSubs[i].([]byte)
		n.Channel = string(b)
		n.NumSub, _ = numSubs[i+1].(int64)
		retNumSubs[i/2] = n
	} // for each
	return retNumSubs, nil
} //commands.PubSubNumSub()

// Publish a message
func (c client) Publish(channel string, message string) (int64, error) {
	n, err := c.Do("PUBLISH", channel, message)
	if err != nil {
		return -1, errors.Wrapf(err, "PUBLISH failed")
	}
	numRecv, _ := n.(int64)
	return numRecv, nil
} //commands.Publish()

// RPop performs a right list pop
func (c client) RPop(key string, count int64) ([][]byte, error) {
	values, err := c.pop("RPOP", key, count)
	if err != nil {
		return nil, errors.Wrapf(err, "RPOP failed")
	}
	return values, nil
} //commands.RPop()

// RPush inserts all the specified values at the tail of the list stored at
// key. Returns the list length.
func (c client) RPush(key string, values ...[]byte) (int64, error) {
	if len(values) <= 0 {
		return -1, errors.Errorf("missing values")
	}
	listLen, err := c.push("RPUSH", key, values...)
	if err != nil {
		return -1, errors.Wrapf(err, "RPUSH failed")
	}
	return listLen, nil
} //commands.RPush()

// RPushAndExpire inserts all the specified values at the tail of the list stored at
// key. Returns the list length.
func (c client) RPushAndExpire(key string, expire time.Duration, values ...[]byte) (int64, bool, error) {
	if len(values) <= 0 {
		return -1, false, errors.Errorf("missing values")
	}
	listLen, expirySet, err := c.pushAndExpire("RPUSH", key, expire, values...)
	if err != nil {
		return -1, false, errors.Wrapf(err, "RPUSH failed")
	}
	return listLen, expirySet, nil
} //commands.RPushAndExpire()

// RPushX inserts all the specified values at the tail of the list stored at
// key only if the list exists. Returns the list length.
func (c client) RPushX(key string, values ...[]byte) (int64, error) {
	if len(values) <= 0 {
		return -1, errors.Errorf("missing values")
	}
	listLen, err := c.push("RPUSHX", key, values...)
	if err != nil {
		return -1, errors.Wrapf(err, "RPUSHX failed")
	}
	return listLen, nil
} //commands.RPushX()

// Set a value withing Redis
func (c client) Set(key string, value []byte, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool, get bool) ([]byte, error) {
	if onlySetIfNotExists && onlySetIfExists {
		return nil, errors.Errorf("invalid parameters %v,%v", onlySetIfNotExists, onlySetIfExists)
	}
	args := setArgs(key, value, expiry, keepTtl, onlySetIfNotExists, onlySetIfExists, get)
	retValue, err := c.Do("SET", args...)
	if err != nil {
		return nil, errors.Wrapf(err, "SET failed")
	}
	if get && retValue != nil {
		return retValue.([]byte), nil
	} else {
		if retValue != nil {
			s, _ := retValue.(string)
			return []byte(s), nil
		}
		return nil, nil
	}
} //commands.Set()

// SetGet tries to set the value in redis, and returns the current value
func (c client) SetGet(key string, value []byte, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool) ([]byte, bool, error) {

	if onlySetIfNotExists && onlySetIfExists {
		return nil, false, errors.Errorf("Invalid parameters %v,%v", onlySetIfNotExists, onlySetIfExists)
	}
	commands := make([]command, 0, 3)
	commands = append(commands, command{
		Command: "SET",
		Args: setArgs(
			key,
			value,
			0, //expiry,
			keepTtl,
			onlySetIfNotExists,
			onlySetIfExists,
			false),
	}) // set
	if !keepTtl && expiry > 0 {
		commands = append(commands, command{
			Command: "PEXPIRE",
			Args:    []interface{}{key, expiry.Milliseconds()},
		})
	} // if expiry

	commands = append(commands, command{
		Command: "GET",
		Args:    []interface{}{key},
	}) // get

	reply, err := c.Multi(key, commands)
	if err != nil {
		return nil, false, errors.Wrapf(err, "MULTI SETGET failed")
	}
	if len(reply) != len(commands) {
		return nil, false, errors.Errorf("Unexpected number of responses %d. Expected %d", len(reply), len(commands))
	}
	return reply[len(commands)-1].([]byte), reply[0] != nil, nil
} //commands.SetGet()

// SetJson sets a value within Redis, formatted as JSON
func (c client) SetJson(key string, value interface{}, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool, get bool, oldValue interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal JSON")
	}
	old, err := c.Set(
		key,
		b,
		expiry,
		keepTtl,
		onlySetIfNotExists,
		onlySetIfExists,
		get)
	if err != nil {
		return errors.Wrapf(err, "failed to set key %s", key)
	}
	if get && old != nil && oldValue != nil {
		if err := json.Unmarshal(old, oldValue); err != nil {
			return errors.Wrapf(err, "failed to unmarshal JSON")
		}
	}
	return nil
} //commands.SetJson()

// ZCard returns the sorted set cardinality (number of elements) of the sorted
// set stored at key
func (c client) ZCard(key string) (int64, error) {
	n, err := c.Do("ZCARD", key)
	if err != nil {
		return -1, errors.Wrapf(err, "ZCARD failed")
	}
	card, _ := n.(int64)
	return card, nil
} //commands.ZCard()

// ZPopMax removes and returns up to count members with the highest scores in
// the sorted set stored at key
func (c client) ZPopMax(key string, count int64) ([]SortedSetElem, error) {
	values, err := c.zPop("ZPOPMAX", key, count)
	if err != nil {
		return nil, errors.Wrapf(err, "ZPOPMAX failed")
	}
	return values, nil
} //commands.ZPopMax()

// ZPopMin removes and returns up to count members with the lowest scores in
// the sorted set stored at key
func (c client) ZPopMin(key string, count int64) ([]SortedSetElem, error) {
	values, err := c.zPop("ZPOPMIN", key, count)
	if err != nil {
		return nil, errors.Wrapf(err, "ZPOPMIN failed")
	}
	return values, nil
} //commands.ZPopMin()

// ZRem Removes the specified members from the sorted set stored at key.
// Non-existing members are ignored
func (c client) ZRem(key string, members ...string) (int64, error) {
	if len(members) <= 0 {
		return 0, errors.Errorf("missing members")
	}
	var err error
	var n interface{}
	if len(members) == 1 {
		n, err = c.Do("ZREM", key, members[0])
	} else {
		args := make([]interface{}, len(members)+1)
		args[0] = key

		for i, m := range members {
			args[i+1] = m
		}
		n, err = c.Do("ZREM", args...)
	} // if args
	if err != nil {
		return 0, errors.Wrapf(err, "failed to remove members at key %s from Redis", key)
	}
	numRem, _ := n.(int64)
	return numRem, nil
} //commands.ZRem()

// ZRemRangeByScore removes all elements in the sorted set stored at key with
// a score between min and max (inclusive).
func (c client) ZRemRangeByScore(key string, min string, max string) (int64, error) {
	n, err := c.Do("ZREMRANGEBYSCORE", key, min, max)
	if err != nil {
		return 0, errors.Wrapf(err, "ZREMRANGEBYSCORE(%s,%v,%v) failed", key, min, max)
	}
	numRem, _ := n.(int64)
	return numRem, nil
} //commands.ZRemRangeByScore()

func (c client) bPop(command string, timeout time.Duration, keys ...string) (string, []byte, error) {
	var multi interface{}
	var err error
	ttlSeconds := math.Ceil(timeout.Seconds())
	if len(keys) == 1 {
		multi, err = c.Do(command, keys[0], ttlSeconds)
	} else {
		var args = make([]interface{}, len(keys)+1)
		for i, key := range keys {
			args[i] = key
		}
		args[len(keys)] = ttlSeconds
		multi, err = c.Do(command, args...)
	}
	if err != nil {
		return "", nil, errors.Wrapf(err, "%s failed", command)
	}
	if multi == nil {
		return "", nil, nil
	}
	m, _ := multi.([]interface{})
	key := m[0].([]byte)
	value := m[1].([]byte)
	return string(key), value, nil
} //commands.bPop()

func (c client) expire(cmd string, key string, expire int64, o ExpireOption) (bool, error) {
	option := string(o)
	var resp interface{}
	var err error
	if len(option) <= 0 {
		resp, err = c.Do(cmd, key, expire)
	} else {
		resp, err = c.Do(cmd, key, expire, option)
	}
	if err != nil {
		return false, errors.Wrapf(err, "expire(%s,%s) failed", cmd, key)
	}
	return resp.(int64) != 0, nil
} //commands.expire()

func (c client) pop(cmd string, key string, count int64) ([][]byte, error) {
	if count <= 0 {
		count = 1
	}
	values, err := c.Do(cmd, key, count)
	if err != nil {
		return nil, errors.Wrapf(err, "pop(%s,%s,%v) failed", cmd, key, count)
	}
	if values == nil {
		return nil, nil
	}
	vs := values.([]interface{})
	var bValues = make([][]byte, len(vs))
	for i, v := range values.([]interface{}) {
		bValues[i] = v.([]byte)
	}
	return bValues, nil
} //commands.pop()

func (c client) push(command string, key string, values ...[]byte) (int64, error) {
	var n interface{}
	var err error
	if len(values) == 1 {
		n, err = c.Do(command, key, values[0])
	} else {
		var args = make([]interface{}, len(values)+1)
		args[0] = key
		for i, value := range values {
			args[i+1] = value
		}
		n, err = c.Do(command, args...)
	}
	if err != nil {
		return -1, errors.Wrapf(err, "%s failed for key %s", command, key)
	}
	listLen, _ := n.(int64)
	return listLen, nil
} //commands.push()

func (c client) pushAndExpire(cmd string, key string, expire time.Duration, values ...[]byte) (int64, bool, error) {
	commands := make([]command, 0, 2)
	if len(values) == 1 {
		commands = append(commands, command{
			Command: cmd,
			Args:    []interface{}{key, values[0]},
		})
	} else {
		var args = make([]interface{}, len(values)+1)
		args[0] = key
		for i, value := range values {
			args[i+1] = value
		}
		commands = append(commands, command{
			Command: cmd,
			Args:    args,
		})
	}
	commands = append(commands, command{
		Command: "PEXPIRE",
		Args:    []interface{}{key, expire.Milliseconds()},
	})
	reply, err := c.Multi(key, commands)
	if err != nil {
		return -1, false, errors.Wrapf(err, "pushAndExpire(%s,%s) failed", cmd, key)
	}
	if len(reply) != len(commands) {
		return -1, false, errors.Errorf("Unexpected number of responses %d. Expected %d", len(reply), len(commands))
	}
	return reply[0].(int64), reply[1].(int64) != 0, nil
} //commands.pushAndExpire()

func (c client) zPop(cmd string, key string, count int64) ([]SortedSetElem, error) {
	if count <= 0 {
		count = 1
	}
	values, err := c.Do(cmd, key, count)
	if err != nil {
		return nil, errors.Wrapf(err, "ZPOP(%s,%s) failed", cmd, key)
	}
	if values == nil {
		return nil, nil
	}
	vs := values.([]interface{})
	if len(vs)%2 != 0 {
		return nil, errors.Errorf("Unexpected number of returned values %d", len(vs))
	} // if not % 2
	var sValues = make([]SortedSetElem, len(vs)/2)
	for i := 0; i < len(vs); i += 2 {
		sValues[i/2] = SortedSetElem{
			Member: string(vs[i].([]byte)),
			Score:  string(vs[i+1].([]byte)),
		}
	}
	return sValues, nil
} //commands.zPop()

func setArgs(key string, value []byte, expiry time.Duration, keepTtl bool, onlySetIfNotExists bool, onlySetIfExists bool, get bool) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, key)
	args = append(args, value)

	if keepTtl {
		args = append(args, "KEEPTTL")
	} else if expiry > 0 {
		args = append(args, "PX", expiry.Milliseconds())
	}

	if onlySetIfNotExists {
		args = append(args, "NX")
	} else if onlySetIfExists {
		args = append(args, "XX")
	}

	if get {
		args = append(args, "GET")
	}

	return args
} //setArgs()
