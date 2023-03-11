package redis

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-msvc/errors"
	"github.com/go-msvc/nats-utils/datatype"
	"github.com/mediocregopher/radix/v3"
)

type ShardedConfig struct {
	HealthInterval datatype.Duration `json:"health_interval" doc:"Duration between health checks"`
	Shards         []ShardConfig     `json:"shards" doc:"Optional: If not specified all servers are used and shards divided among them. List of shards containing the servers to be used for each shard, the sequence of the shard will be used to determine which shard needs to be for the received hashed key."`
	Servers        []ServerConfig    `json:"servers" doc:"List of redis server StandaloneConfig items with name to identify for the shards lists."`
}

func (c *ShardedConfig) Validate() error {
	if c.HealthInterval <= 0 {
		return errors.Errorf("invalid health_interval:\"%s\"", c.HealthInterval)
	}
	if len(c.Servers) == 0 {
		return errors.Errorf("missing servers")
	}
	for i, server := range c.Servers {
		if err := server.Validate(); err != nil {
			return errors.Wrapf(err, "invalid servers[%d]", i)
		}
	}
	for i, shard := range c.Shards {
		if err := shard.Validate(); err != nil {
			return errors.Wrapf(err, "invalid shard[%d]", i)
		}
	}
	return nil
} //ShardedConfig.Validate()

type ServerConfig struct {
	Name string `json:"name" doc:"Name for server that is used in the shards list."`
	StandaloneConfig
}

func (c ServerConfig) Validate() error {
	if c.Name == "" {
		return errors.Errorf("missing name")
	}
	return nil
} //ServerConfig.Validate()

type ShardConfig struct {
	Servers []string `json:"servers" doc:"List of server names in order of use for this shard, if the first server is not reachable the second server will be used etc. Note that data is not replicated to other servers so this KVStore is used for short lived transactional data only. This option will not suffer under the split brain scenario when one of two entire sites goes down."`
}

func (c ShardConfig) Validate() error {
	if len(c.Servers) == 0 {
		return errors.Errorf("missing servers")
	}
	for i, s := range c.Servers {
		if s == "" {
			return errors.Errorf("empty name for server[%d]", i)
		}
	}
	return nil
}

type PubSubConfig struct {
	Address    string `json:"address"`
	AbortAfter int    `json:"abort_after"`
}

func (redisConfig *PubSubConfig) Validate() error {
	if redisConfig.Address == "" {
		return errors.Errorf("missing address")
	}
	return nil
} //PubSubConfig.Validate()

func (c ShardedConfig) Create() (Connector, error) {
	newConnector := &shardedConnector{
		config:            c,
		connectorByServer: map[string]*haShardStandaloneConnector{},
	}
	for _, serverConfig := range c.Servers {
		standaloneClient := haShardStandaloneConnector{
			parent:       newConnector,
			serverConfig: serverConfig,
			address:      fmt.Sprintf("%s:%d", serverConfig.Address, serverConfig.Port),
		}
		if err := standaloneClient.initPool(true); err != nil {
			return nil, errors.Wrapf(err, "Failed to create connection to redis %s", standaloneClient.address)
		}
		newConnector.connectorByServer[serverConfig.Name] = &standaloneClient
	}
	return newConnector, nil
} //ShardedConfig.Create()

type shardedConnector struct {
	config            ShardedConfig
	connectorByServer map[string]*haShardStandaloneConnector
}

func (c *shardedConnector) Do(commandName string, args ...interface{}) (interface{}, error) {
	if strings.ToUpper(commandName) == "PING" {
		log.Debugf("pinging %d servers...", len(c.connectorByServer))
		for serverName, connectorToServer := range c.connectorByServer {
			_, err := connectorToServer.Do(commandName, args...)
			if err != nil {
				return nil, errors.Wrapf(err, "server(%s) ping failed", serverName)
			}
			log.Debugf("server(%s) replied to ping", serverName)
		}
		return "OK", nil
	} //if ping

	ks, err := keys(commandName, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get keys")
	}
	if len(ks) <= 0 {
		return nil, errors.Errorf("no keys found")
	}
	reply, err := c.selectServer(
		ks[0],
		func(sa *haShardStandaloneConnector) (interface{}, error) {
			if reply, err := sa.Do(commandName, args...); err != nil {
				return nil, errors.Wrapf(err, "failed to execute Redis command")
			} else {
				return reply, nil
			}
		})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute Redis command")
	}
	return reply, nil
} //shardedConnector.Do()

func (c *shardedConnector) Multi(key string, cmd []command) ([]interface{}, error) {
	reply, err := c.selectServer(
		key,
		func(sa *haShardStandaloneConnector) (interface{}, error) {
			if reply, err := sa.Multi(key, cmd); err != nil {
				return nil, errors.Wrapf(err, "failed to execute Redis command")
			} else {
				return reply, nil
			}
		})

	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute Redis command")
	}
	return reply.([]interface{}), nil
} //shardedConnector.Multi()

type haShardStandaloneConnector struct {
	parent       *shardedConnector
	address      string
	serverConfig ServerConfig
	healthy      int32
	healthOnce   sync.Once
	mutex        sync.Mutex
	pool         *radix.Pool
}

func (c *haShardStandaloneConnector) initPool(failOk bool) error {
	if c.pool == nil {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.pool == nil {
			defer c.healthOnce.Do(func() {
				go c.health()
			})
			p, err := newPool("tcp", c.address, c.serverConfig.commonConfig, failOk)
			if err != nil {
				return errors.Wrapf(err,
					"Failed to create new pool for %s",
					c.address)
			}
			c.pool = p
		} //if not pool
	} //if not pool
	return nil
} //haShardStandaloneConnector.initPool()

//health performs period health checks on the pool
func (c *haShardStandaloneConnector) health() {
	for {
		time.Sleep(c.parent.config.HealthInterval.Duration())
		log.Debugf("Performing health check for %s (%s:%d)",
			c.serverConfig.Name,
			c.serverConfig.Address,
			c.serverConfig.Port)

		cli := client{
			Connector: c,
		}
		if _, err := cli.Ping(c.serverConfig.Name); err != nil {
			log.Errorf("%+v", errors.Wrapf(err,
				"Failed to ping server %s (%s:%d)",
				c.serverConfig.Name,
				c.serverConfig.Address,
				c.serverConfig.Port))
			atomic.StoreInt32(&c.healthy, 0)
		} else {
			log.Debugf("Ping reply for %s (%s:%d). Server up.",
				c.serverConfig.Name,
				c.serverConfig.Address,
				c.serverConfig.Port)
			atomic.StoreInt32(&c.healthy, 1)
		}
	} //forever
} //haShardStandaloneConnector.health()

func (c *haShardStandaloneConnector) Do(commandName string, args ...interface{}) (interface{}, error) {
	if err := c.initPool(false); err != nil {
		return nil, errors.Wrapf(err,
			"Failed to initialise pool")
	} // if failed to init

	reply, err := do(c.pool, 1, commandName, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute Redis command")
	}
	return reply, nil
} //haShardStandaloneConnector.Do()

func (c *haShardStandaloneConnector) Multi(key string, cmd []command) ([]interface{}, error) {
	if err := c.initPool(false); err != nil {
		return nil, errors.Wrapf(err, "failed to initialise pool")
	}
	reply, err := multi(c.pool, key, cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute Redis multi command")
	}
	return reply, nil
} //haShardStandaloneConnector.Multi()

// selectServer selects a server to use
func (c *shardedConnector) selectServer(
	shardKey string,
	f func(sa *haShardStandaloneConnector) (interface{}, error),
) (
	interface{},
	error,
) {
	log.Debugf("Shard key %s", shardKey)

	// There are 2 approaches for sharding
	//
	// 1) Create a shard per server:
	//   - this is done if no Shards are configured.
	//   - all other servers act as backups
	//   - if one server is down, simply try the next one in the list
	//   - to ensure that the same server is not always used in the event of a site failure (a few servers),
	// 		server with an even index will incur selection of the next server in the list with index + 1,
	//      server with an odd index will incur selection of the next server in the list with index - 1
	//
	// 2) Specific shards are configured, each with its own list of servers
	//   - The first server in the list is always used first, if down, the next one in the list is used.

	if len(c.config.Shards) == 0 {

		// if no shards are defined, we assume nr shards = nr of
		// servers defined
		nrServers := len(c.config.Servers)
		keyInst := hash([]byte(shardKey), uint16(nrServers))
		keyInstEven := keyInst % 2
		nextServerInst := int(keyInst)

		for i := 0; i < nrServers; i++ {

			serverName := c.config.Servers[nextServerInst].Name
			sa := c.connectorByServer[serverName]
			if atomic.LoadInt32(&sa.healthy) != 0 {
				log.Debugf("Server chosen to use is %s", serverName)
				reply, err := f(sa)
				if err != nil {
					return nil, errors.Wrapf(err, "Redis command failed for server %s", serverName)
				}
				log.Debugf("Got success for server %s", serverName)
				return reply, nil
			} //if healthy

			log.Debugf("Server %s not healthy", serverName)

			// even first keyInst we choose next server in slice, if odd we
			// choose the previous one. this ensures that the same next server
			// is not chosen if a site goes down (depending on the server list
			// config)
			if keyInstEven == 0 {
				nextServerInst += 1
			} else {
				nextServerInst -= 1
			}

			if nextServerInst >= nrServers {
				nextServerInst = 0
			}

			if nextServerInst < 0 {
				nextServerInst = nrServers - 1
			}

		} // for servers

		return nil, errors.Errorf("Failed to find an active connection to "+
			"any of the configured Redis servers (total=%d)",
			nrServers)
	} else {
		// get nr shards and check config to get server to send to
		keyInst := hash([]byte(shardKey), uint16(len(c.config.Shards)))
		shard := c.config.Shards[keyInst]

		// Check if server is up, starting from server 0
		for _, serverName := range shard.Servers {
			sa := c.connectorByServer[serverName]
			log.Debugf("Shard chosen is idx=[%v] server chosen is \"%s\" (shard = %v)", keyInst, serverName, shard)
			if atomic.LoadInt32(&sa.healthy) != 0 {
				log.Debugf("Shard idx=[%v] server chosen to use is %s",
					keyInst,
					serverName)
				reply, err := f(sa)
				if err != nil {
					return nil, errors.Wrapf(err, "redis command failed for server %s", serverName)
				} else {
					log.Debugf("success for shard idx=[%v] server %s", keyInst, serverName)
					return reply, nil
				}
			} //if healthy
			log.Debugf("server %s not healthy", serverName)
		} // for servers
		return nil, errors.Errorf("failed to find an active connection to "+
			"the shard (index=%d) to any of the configured Redis "+
			"servers (total=%d)", int(keyInst), len(shard.Servers))
	}
} //haShardConnector.selectServer()
