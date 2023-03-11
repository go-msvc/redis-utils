package redis

import (
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	"github.com/go-msvc/errors"
	"github.com/go-msvc/logger"
	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/trace"
)

var log = logger.New().WithLevel(logger.LevelDebug)

//different types of REDIS connectors all implement this interface
//(standalone, cluster or ha_shard)
type Connector interface {
	Do(commandName string, args ...interface{}) (interface{}, error)
	Multi(key string, cmd []command) ([]interface{}, error)
}

func (c Config) Create() (Client, error) {
	//metricsInit(redisConfig.Name)
	var connector Connector
	var err error
	if c.Standalone != nil {
		connector, err = c.Standalone.Create()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create standalone connector")
		}
	}
	if c.Cluster != nil {
		connector, err = c.Cluster.Create()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create cluster connector")
		}
	}
	if c.Sharded != nil {
		connector, err = c.Sharded.Create()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create hashard connector")
		}
	}
	if connector == nil {
		return nil, errors.Errorf("no connection to REDIS")
	}
	client := client{
		Connector: connector,
	}
	//ping redis to catch connectivity errors at start up
	if _, err := client.Ping("ping"); err != nil {
		return nil, errors.Wrapf(err, "failed to ping Redis server")
	}
	return client, nil
} //Config.NewClient()

type command struct {
	Command string
	Args    []interface{}
}

// type NumSub struct {
// 	Channel string
// 	NumSub  int64
// } // NumSub

// type SortedSetElem struct {
// 	Member string
// 	Score  string
// } // SortedSetElem

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
) // var

var (
	defaultClient   *Client
	defaultClientMu sync.Mutex
) // var

func hash(key []byte, hashSize uint16) uint16 {
	// Use the radix ClusterSlot method so that we can take advantage of the
	// standard Redis key hashtag convention.
	return radix.ClusterSlot(key) % hashSize
} // hash ()

func newPool(network string, address string, config commonConfig, failOk bool) (*radix.Pool, error) {
	log.Debugf("creating pool %s", address)
	p, err := radix.NewPool(
		network,
		address,
		config.NumKeepAlive,
		radix.PoolWithTrace(
			trace.PoolTrace{
				ConnCreated: func(created trace.PoolConnCreated) {
					log.Debugf("Conn created %+v", created)
				},
				ConnClosed: func(closed trace.PoolConnClosed) {
					log.Debugf("Conn closed %+v", closed)
				},
				DoCompleted: func(completed trace.PoolDoCompleted) {
					log.Debugf("Completed %+v", completed)
				},
				InitCompleted: func(completed trace.PoolInitCompleted) {
					log.Debugf("Init completed %+v", completed)
				},
			}),
		radix.PoolPingInterval(config.PoolPingInterval.Duration()),
		radix.PoolConnFunc(func(network, addr string) (radix.Conn, error) {
			log.Debugf("new Redis pool %s:%s", network, addr)
			options := []radix.DialOpt{
				radix.DialConnectTimeout(config.ConnTimeout.Duration()),
				radix.DialReadTimeout(config.ReadTimeout.Duration()),
				radix.DialWriteTimeout(config.WriteTimeout.Duration()),
			}
			if config.Database > 0 {
				options = append(options, radix.DialSelectDB(config.Database))
			}
			if len(config.Username) > 0 {
				options = append(options, radix.DialAuthUser(config.Username, config.Password.StringPlain()))
			} else if len(config.Password.StringPlain()) > 0 {
				options = append(options, radix.DialAuthPass(config.Password.StringPlain()))
			}
			if config.UseTls {
				tlsConfig := tls.Config{
					InsecureSkipVerify: config.TLSSkipVerify,
				}
				options = append(options, radix.DialUseTLS(&tlsConfig))
			}
			conn, err := radix.Dial(network, addr, options...)
			if err != nil {
				// radix does not print the error, and just continues,
				// so print any errors here
				log.Errorf("Error dialling REDIS(network:%s, addr:%s): %+v", network, addr, err)
			}
			return conn, err
		}),
	)
	if err != nil {
		if failOk {
			log.Errorf("proceed after failed to create pool: %+v", err)
		} else {
			return nil, errors.Wrapf(err, "failed to create pool")
		}
	} //if failed to create
	return p, nil
} //newPool()

// // Perform a single command against Redis
// func do(client radix.Client, numRes int, commandName string, args ...interface{}) (
// 	interface{}, error) {

// 	if client == nil {
// 		return nil, errors.Errorf("client not initialised")
// 	} // if client not init

// 	startTime := time.Now()
// 	defer func() {
// 		metricsCountCommand(commandName, time.Since(startTime))
// 	}()

// 	logger.Tracef("Attempting Redis command %s with args %v. "+
// 		"Num expected responses %d",
// 		commandName,
// 		args,
// 		numRes)

// 	var reply interface{}
// 	var mn radix.MaybeNil

// 	if numRes > 1 {
// 		resp := make([]interface{}, numRes, numRes)
// 		tuple := make(radix.Tuple, numRes, numRes)
// 		for i := 0; i < numRes; i++ {
// 			tuple[i] = &radix.MaybeNil{
// 				Rcv: &resp[i],
// 			}
// 		}
// 		mn.Rcv = &tuple
// 		reply = resp
// 	} else {
// 		mn.Rcv = &reply
// 	}

// 	if len(args) <= 0 {

// 		err := client.Do(radix.Cmd(
// 			&mn,
// 			commandName))

// 		if err != nil {
// 			return nil, errors.Wrapf(err,
// 				"Failed to execute Redis command %s",
// 				commandName)
// 		} // if redis failed

// 	} else {

// 		switch strings.ToUpper(commandName) {

// 		case "EVAL":

// 			if len(args) < 2 {
// 				return nil, errors.Errorf("Num args %d unexpected",
// 					len(args))
// 			} // if unexpected

// 			script, ok := args[0].(string)
// 			if !ok {
// 				return nil, errors.Errorf("Failed to get script from %T (%v)",
// 					args[0], args[0])
// 			} // if not key

// 			logger.Tracef("Script: %s",
// 				script)

// 			evalKeys, err := keys("EVAL", args...)
// 			if err != nil {
// 				return nil, errors.Wrapf(err,
// 					"Failed to get keys")
// 			} // if failed

// 			numKey := len(evalKeys)
// 			evalArgs := args[numKey+2:]

// 			logger.Debugf("EVAL args %+v",
// 				evalArgs)

// 			evalScript := radix.NewEvalScript(
// 				numKey,
// 				script)

// 			err = client.Do(evalScript.FlatCmd(
// 				&mn,
// 				evalKeys,
// 				evalArgs...))

// 			if err != nil {
// 				return nil, errors.Wrapf(err,
// 					"Failed to execute Redis command %s",
// 					commandName)
// 			} // if redis failed

// 		default:

// 			key, ok := args[0].(string)
// 			if !ok {
// 				return nil, errors.Errorf("Failed to get key from %T (%v)",
// 					args[0],
// 					args[0])
// 			} // if not key

// 			logger.Tracef("Key %s",
// 				key)

// 			err := client.Do(radix.FlatCmd(
// 				&mn,
// 				commandName,
// 				key,
// 				args[1:]...))

// 			if err != nil {
// 				return nil, errors.Wrapf(err,
// 					"Failed to execute Redis command %s",
// 					commandName)
// 			} // if redis failed

// 		} // switch

// 	} // if args

// 	logger.Tracef("Got Redis reply %T: %v",
// 		reply,
// 		reply)

// 	if mn.Nil {
// 		return nil, nil
// 	} // if nil

// 	return reply, nil

// } // do()

// // Perform a transaction against Redis
// func multi(client radix.Client, key string, cmd []command) ([]interface{}, error) {

// 	if client == nil {
// 		return nil, errors.Errorf("client not initialised")
// 	} // if client not init

// 	if len(cmd) <= 0 {
// 		return nil, errors.Errorf("No commands supplied")
// 	} // if no commands

// 	var reply interface{}

// 	err := client.Do(radix.WithConn(key, func(conn radix.Conn) error {

// 		var err error

// 		if _, err = do(conn, 1, "MULTI"); err != nil {
// 			return errors.Wrapf(err, "Failed to execute Redis command MULTI")
// 		} // if failed to perform

// 		defer func() {
// 			if err != nil {
// 				if _, err = do(conn, 1, "DISCARD"); err != nil {
// 					logger.Errorf("%+v", errors.Wrapf(err,
// 						"Failed to execute Redis command DISCARD"))
// 				} // if failed to perform
// 			} // if error
// 		}() // defer

// 		for i, cmd := range cmd {
// 			if _, err = do(conn, 1, cmd.Command, cmd.Args...); err != nil {
// 				return errors.Wrapf(err,
// 					"Failed to execute Redis command[%d] \"%s\"",
// 					i, cmd.Command)
// 			} // if failed to perform
// 		} // for each cmd

// 		if reply, err = do(conn, len(cmd), "EXEC"); err != nil {
// 			return errors.Wrapf(err,
// 				"Failed to execute Redis command EXEC")
// 		} // if failed to perform

// 		return nil

// 	}))

// 	if err != nil {
// 		return nil, errors.Wrapf(err,
// 			"Failed to perform multi command")
// 	} // if failed to perform

// 	if _, ok := reply.([]interface{}); !ok {
// 		return nil, errors.Errorf("Unexpected type %T",
// 			reply)
// 	} // if unexpected

// 	return reply.([]interface{}), nil

// } // multi()

// // Get the keys for a command
// func keys(cmd string, args ...interface{}) ([]string, error) {

// 	if len(args) <= 0 {
// 		return nil, errors.Errorf(
// 			"No args supplied")
// 	} // if no args

// 	switch strings.ToUpper(cmd) {

// 	case "EVAL":

// 		if len(args) < 2 {
// 			return nil, errors.Errorf("Num args %d unexpected",
// 				len(args))
// 		} // if unexpected

// 		numKey, ok := args[1].(int)
// 		if !ok {
// 			return nil, errors.Errorf("Failed to get num keys from %T (%v)",
// 				args[1], args[1])
// 		} // if not key

// 		logger.Tracef("Num key: %d",
// 			numKey)

// 		if len(args) < numKey+2 {
// 			return nil, errors.Errorf("Num args %d unexpected",
// 				len(args))
// 		} // if unexpected

// 		evalKeys := make([]string, numKey)
// 		for i, k := range args[2 : 2+numKey] {
// 			key, ok := k.(string)
// 			if !ok {
// 				return nil, errors.Errorf("Failed to get key from %T (%v)",
// 					k, k)
// 			} // if not key
// 			evalKeys[i] = key
// 		} // for each

// 		logger.Debugf("EVAL keys %+v",
// 			evalKeys)

// 		return evalKeys, nil

// 	default:

// 		key, _ := args[0].(string)
// 		return []string{key}, nil

// 	} // switch

// } // keys()

// Perform a command against a redis client, which could be a pool of connections
func do(client radix.Client, numRes int, commandName string, args ...interface{}) (interface{}, error) {
	if client == nil {
		return nil, errors.Errorf("missing REDIS client")
	}
	// startTime := time.Now()
	// defer func() {
	// 	metricsCountCommand(commandName, time.Since(startTime))
	// }()
	var reply interface{}
	var mn radix.MaybeNil
	if numRes > 1 {
		resp := make([]interface{}, numRes, numRes)
		tuple := make(radix.Tuple, numRes, numRes)
		for i := 0; i < numRes; i++ {
			tuple[i] = &radix.MaybeNil{
				Rcv: &resp[i],
			}
		}
		mn.Rcv = &tuple
		reply = resp
	} else {
		mn.Rcv = &reply
	}
	if len(args) <= 0 {
		err := client.Do(radix.Cmd(&mn, commandName))
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to execute Redis command %s", commandName)
		}
	} else {
		switch strings.ToUpper(commandName) {
		case "EVAL":
			if len(args) < 2 {
				return nil, errors.Errorf("Num args %d unexpected", len(args))
			}
			script, ok := args[0].(string)
			if !ok {
				return nil, errors.Errorf("Failed to get script from %T (%v)", args[0], args[0])
			}
			log.Debugf("Script: %s", script)
			evalKeys, err := keys("EVAL", args...)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to get keys")
			}
			numKey := len(evalKeys)
			evalArgs := args[numKey+2:]
			log.Debugf("EVAL args %+v", evalArgs)
			evalScript := radix.NewEvalScript(numKey, script)
			err = client.Do(evalScript.FlatCmd(&mn, evalKeys, evalArgs...))
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to execute Redis command %s", commandName)
			}
		default:
			key, ok := args[0].(string)
			if !ok {
				return nil, errors.Errorf("Failed to get key from %T (%v)", args[0], args[0])
			}
			err := client.Do(radix.FlatCmd(&mn, commandName, key, args[1:]...))
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to execute Redis command %s", commandName)
			}
		} // switch
	} // if args
	log.Debugf("Got Redis reply %T: %v", reply, reply)
	if mn.Nil {
		return nil, nil
	}
	return reply, nil
} // do()

// Perform a transaction against Redis
func multi(client radix.Client, key string, cmd []command) ([]interface{}, error) {
	if client == nil {
		return nil, errors.Errorf("client not initialised")
	}
	if len(cmd) <= 0 {
		return nil, errors.Errorf("missing commands")
	}
	var reply interface{}
	err := client.Do(radix.WithConn(key, func(conn radix.Conn) error {
		var err error
		if _, err = do(conn, 1, "MULTI"); err != nil {
			return errors.Wrapf(err, "Failed to execute Redis command MULTI")
		}
		defer func() {
			if err != nil {
				if _, err = do(conn, 1, "DISCARD"); err != nil {
					log.Errorf("%+v", errors.Wrapf(err, "Failed to execute Redis command DISCARD"))
				}
			}
		}()

		for i, cmd := range cmd {
			if _, err = do(conn, 1, cmd.Command, cmd.Args...); err != nil {
				return errors.Wrapf(err, "Failed to execute Redis command[%d] \"%s\"", i, cmd.Command)
			}
		}
		if reply, err = do(conn, len(cmd), "EXEC"); err != nil {
			return errors.Wrapf(err, "Failed to execute Redis command EXEC")
		}
		return nil
	}))

	if err != nil {
		return nil, errors.Wrapf(err, "Failed to perform multi command")
	}
	if _, ok := reply.([]interface{}); !ok {
		return nil, errors.Errorf("Unexpected type %T", reply)
	}
	return reply.([]interface{}), nil
} //multi()

// Get the keys for a command
func keys(cmd string, args ...interface{}) ([]string, error) {
	if len(args) <= 0 {
		return nil, errors.Errorf("No args supplied")
	}
	switch strings.ToUpper(cmd) {
	case "EVAL":
		if len(args) < 2 {
			return nil, errors.Errorf("num args %d < 2", len(args))
		}
		numKey, ok := args[1].(int)
		if !ok {
			return nil, errors.Errorf("Failed to get num keys from %T (%v)", args[1], args[1])
		}
		log.Debugf("Num key: %d", numKey)
		if len(args) < numKey+2 {
			return nil, errors.Errorf("Num args %d unexpected", len(args))
		}
		evalKeys := make([]string, numKey)
		for i, k := range args[2 : 2+numKey] {
			key, ok := k.(string)
			if !ok {
				return nil, errors.Errorf("Failed to get key from %T (%v)", k, k)
			}
			evalKeys[i] = key
		} //for each
		log.Debugf("EVAL keys %+v", evalKeys)
		return evalKeys, nil
	default:
		key, _ := args[0].(string)
		return []string{key}, nil
	} //switch
} //keys()
