package redis

import (
	"github.com/go-msvc/errors"
	"github.com/go-msvc/nats-utils/datatype"
)

type Config struct {
	Standalone *StandaloneConfig `json:"standalone" doc:"Single standalone REDIS instance"`
	Cluster    *ClusterConfig    `json:"cluster" doc:"Configured REDIS cluster"`
	Sharded    *ShardedConfig    `json:"sharded" doc:"High Availability REDIS Shards. Sharding will be done either configured server (if no shards are configured) or as configured shards with list of servers in order of use."`
} //Config

func (c *Config) Validate() error {
	count := 0
	if c.Standalone != nil {
		count++
		if err := c.Standalone.Validate(); err != nil {
			return errors.Wrapf(err, "invalid standalone")
		}
	}
	if c.Cluster != nil {
		count++
		if err := c.Cluster.Validate(); err != nil {
			return errors.Wrapf(err, "invalid cluster")
		}
	}
	if c.Sharded != nil {
		count++
		if err := c.Sharded.Validate(); err != nil {
			return errors.Wrapf(err, "invalid ha_shard")
		}
	}
	if count != 1 {
		return errors.Errorf("required exactly one of standalone|cluster|ha_shard (has %d)", count)
	}
	return nil
} //Config.Validate()

type commonConfig struct {
	Database         int               `json:"database" doc:"The Redis database to use"`
	ConnTimeout      datatype.Duration `json:"connect_timeout" doc:"Connect timeout"`
	ReadTimeout      datatype.Duration `json:"read_timeout" doc:"Read timeout"`
	WriteTimeout     datatype.Duration `json:"write_timeout" doc:"Write timeout"`
	NumKeepAlive     int               `json:"num_keep_alive" doc:"The number of connections to keep alive"`
	AliveTime        datatype.Duration `json:"keep_alive"`
	PoolPingInterval datatype.Duration `json:"pool_ping_interval" doc:"Pool ping interval"`
	Username         string            `json:"username" doc:"Redis username"`
	Password         datatype.EncStr   `json:"password" doc:"Redis password"`
	UseTls           bool              `json:"use_tls" doc:"Set to true to connect to Redis using TLS"`
	TLSSkipVerify    bool              `json:"tls_skip_verify" doc:"Set to true to skip certificate validation"`
}

func (c commonConfig) Validate() error {
	if c.ConnTimeout.Duration() <= 0 {
		return errors.Errorf("invalid connect_timeout:\"%s\"", c.ConnTimeout)
	}
	// if redisConfig.ReadTimeout.Duration() <= 0 {
	// 	return errors.Errorf("invalid read_timeout:\"%s\"", redisConfig.ReadTimeout)
	// }
	// if redisConfig.WriteTimeout.Duration() <= 0 {
	// 	return errors.Errorf("invalid write_timeout:\"%s\"", redisConfig.WriteTimeout)
	// }
	if c.NumKeepAlive <= 0 {
		return errors.Errorf("invalid num_keep_alive:%d", c.NumKeepAlive)
	}
	if c.AliveTime.Duration() <= 0 {
		return errors.Errorf("invalid keep_alive:\"%s\"", c.AliveTime)
	}
	return nil
} //commonConfig.Validate()
