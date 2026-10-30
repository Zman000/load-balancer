package config

import (
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
type PrintStatus struct {
	Enabled bool `yaml:"enabled"`
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
	PrintStatus    PrintStatus          `yaml:"print_status"`
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
