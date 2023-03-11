package redis

import (
	"fmt"

	"github.com/go-msvc/errors"
	"github.com/mediocregopher/radix/v3"
)

type StandaloneConfig struct {
	Address string `json:"address" doc:"Redis server address"`
	Port    int    `json:"port" doc:"Redis server port"`
	commonConfig
}

func (c *StandaloneConfig) Validate() error {
	if len(c.Address) <= 0 {
		return errors.Errorf("invalid address:\"%s\"", c.Address)
	}
	if c.Port <= 0 {
		return errors.Errorf("invalid port:%d", c.Port)
	}
	return nil
} //StandaloneConfig.Validate()

func (c StandaloneConfig) Create() (Connector, error) {
	address := fmt.Sprintf("%s:%d", c.Address, c.Port)
	pool, err := newPool("tcp", address, c.commonConfig, false)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create pool")
	}
	return standaloneConnector{
		pool: pool,
	}, nil
} //StandaloneConfig.Create()

type standaloneConnector struct {
	pool *radix.Pool
}

func (c standaloneConnector) Do(commandName string, args ...interface{}) (interface{}, error) {
	reply, err := do(c.pool, 1, commandName, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "command failed")
	}
	return reply, nil
} //standalone.Do()

func (c standaloneConnector) Multi(key string, cmd []command) ([]interface{}, error) {
	reply, err := multi(c.pool, key, cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "multi failed")
	}
	return reply, err
} //standalone.Multi()
