package redis

import (
	"github.com/go-msvc/errors"
	"github.com/mediocregopher/radix/v3"
)

type ClusterConfig struct {
	StartNodes           []string `json:"start_nodes" doc:"The Redis Cluster server addresses"`
	InitAllowUnavailable bool     `json:"init_allow_unavailable"`
	commonConfig
}

func (c *ClusterConfig) Validate() error {
	if len(c.StartNodes) <= 0 {
		return errors.Errorf("missing start_nodes")
	}
	for i, s := range c.StartNodes {
		if len(s) <= 0 {
			return errors.Errorf("invalid start_nodes[%d]:\"%s\"", i, s)
		}
	}
	if err := c.commonConfig.Validate(); err != nil {
		return err
	}
	return nil
} //ClusterConfig.Validate()

func (c ClusterConfig) Create() (Connector, error) {
	cluster, err := radix.NewCluster(
		c.StartNodes,
		radix.ClusterOnInitAllowUnavailable(c.InitAllowUnavailable),
		radix.ClusterPoolFunc(func(network, addr string) (radix.Client, error) {
			return newPool(network, addr, c.commonConfig, false)
		}),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create redis cluster client")
	}
	return clusterConnector{cluster: cluster}, nil
} //ClusterConfig.Create()

type clusterConnector struct {
	cluster *radix.Cluster
}

func (c clusterConnector) Do(commandName string, args ...interface{}) (interface{}, error) {
	reply, err := do(c.cluster, 1, commandName, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "command failed")
	}
	return reply, nil
} //clusterConnector.Do()

func (c clusterConnector) Multi(key string, cmd []command) ([]interface{}, error) {
	reply, err := multi(c.cluster, key, cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "multi failed")
	}
	return reply, err
} //clusterConnector.Multi()
