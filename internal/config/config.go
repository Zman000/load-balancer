package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Server struct {
	ListenAddress  string `yaml:"listen_address"`
	MaxConnections int    `yaml:"max_connections"`
}
type LoadBalancer struct {
	Algorithm         string `yaml:"algorithm"`
	ConnectionTimeout string `yaml:"connection_timeout"`
}
type HealthCheck struct {
	Enabled  bool   `yaml:"enabled"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`
}
type RateDecider struct {
	RequestsPerSecond int `yaml:"requests_per_sec"`
	BurstCapacity     int `yaml:"burst_cap"`
}
type CircuitBreakerConfig struct {
	MaxFailures int    `yaml:"max_failures"`
	Cooldown    string `yaml:"cooldown"`
}
type RetryConfig struct {
	MaxRetries     int    `yaml:"max_retries"`
	InitialBackoff string `yaml:"initial_backoff"`
}
type Config struct {
	Server         Server               `yaml:"server"`
	LoadBalancer   LoadBalancer         `yaml:"load_balancer"`
	HealthCheck    HealthCheck          `yaml:"health_check"`
	RateDecider    RateDecider          `yaml:"rate_limit"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	Retry          RetryConfig          `yaml:"retry"`
	Backends       []string             `yaml:"backends"`
	IdleTimeout    string               `yaml:"idle_timeout"`
}

func Load(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	var config Config

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return Config{}, err
	}

	return config, nil
}

func (c *Config) Validate() error {
	if c.LoadBalancer.Algorithm != "round_robin" && c.LoadBalancer.Algorithm != "least_connections" {
		return fmt.Errorf("invalid algorithm %q: must be 'round_robin' or 'least_connections'", c.LoadBalancer.Algorithm)
	}
	if c.Server.ListenAddress == "" {
		return fmt.Errorf("listen address cannot be empty")
	}
	if c.Server.MaxConnections <= 0 {
		return fmt.Errorf("server max_connections must be greater than 0")
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends available")
	}
	return nil
}
