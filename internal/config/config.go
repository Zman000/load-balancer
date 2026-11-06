package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Server struct {
	ListenAddress  string `yaml:"listen_address"`
	MaxConnections int    `yaml:"max_connections"`
}
type LoadBalancer struct {
	Algorithm         string        `yaml:"algorithm"`
	ConnectionTimeout string        `yaml:"connection_timeout"`
	ParsedConnTimeout time.Duration `yaml:"-"`
}
type HealthCheck struct {
	Enabled        bool          `yaml:"enabled"`
	Timeout        string        `yaml:"timeout"`
	Interval       string        `yaml:"interval"`
	ParsedTimeout  time.Duration `yaml:"-"`
	ParsedInterval time.Duration `yaml:"-"`
}
type RateDecider struct {
	RequestsPerSecond int `yaml:"requests_per_sec"`
	BurstCapacity     int `yaml:"burst_cap"`
}
type CircuitBreakerConfig struct {
	MaxFailures    int           `yaml:"max_failures"`
	Cooldown       string        `yaml:"cooldown"`
	ParsedCooldown time.Duration `yaml:"-"`
}
type RetryConfig struct {
	MaxRetries     int           `yaml:"max_retries"`
	InitialBackoff string        `yaml:"initial_backoff"`
	ParsedBackoff  time.Duration `yaml:"-"`
}
type Config struct {
	Server            Server               `yaml:"server"`
	LoadBalancer      LoadBalancer         `yaml:"load_balancer"`
	HealthCheck       HealthCheck          `yaml:"health_check"`
	RateDecider       RateDecider          `yaml:"rate_limit"`
	CircuitBreaker    CircuitBreakerConfig `yaml:"circuit_breaker"`
	Retry             RetryConfig          `yaml:"retry"`
	Backends          []string             `yaml:"backends"`
	IdleTimeout       string               `yaml:"idle_timeout"`
	ParsedIdleTimeout time.Duration        `yaml:"-"`
}

func Load(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	var config Config

	// KnownFields(true) rejects any YAML key that doesn't map to a struct
	// field. Without this, a typo like "requests_per_second" (instead of
	// "requests_per_sec") silently yields a zero value — the rate limiter
	// would allow everything through.
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	err = decoder.Decode(&config)
	if err != nil {
		return Config{}, fmt.Errorf("config parse error: %w", err)
	}

	return config, nil
}

// parseDuration is a helper that returns a clear error naming the field.
func parseDuration(name, raw string) (time.Duration, error) {
	if raw == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, raw, err)
	}
	return d, nil
}

func (c *Config) Validate() error {
	// --- structural checks ---
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

	// --- numeric range checks ---
	if c.RateDecider.RequestsPerSecond <= 0 {
		return fmt.Errorf("rate_limit.requests_per_sec must be > 0, got %d", c.RateDecider.RequestsPerSecond)
	}
	if c.RateDecider.BurstCapacity <= 0 {
		return fmt.Errorf("rate_limit.burst_cap must be > 0, got %d", c.RateDecider.BurstCapacity)
	}
	if c.CircuitBreaker.MaxFailures < 1 {
		return fmt.Errorf("circuit_breaker.max_failures must be >= 1, got %d", c.CircuitBreaker.MaxFailures)
	}
	if c.Retry.MaxRetries < 0 {
		return fmt.Errorf("retry.max_retries must be >= 0, got %d", c.Retry.MaxRetries)
	}

	// --- parse all durations once, with clear error messages ---
	var err error

	c.LoadBalancer.ParsedConnTimeout, err = parseDuration("load_balancer.connection_timeout", c.LoadBalancer.ConnectionTimeout)
	if err != nil {
		return err
	}

	c.CircuitBreaker.ParsedCooldown, err = parseDuration("circuit_breaker.cooldown", c.CircuitBreaker.Cooldown)
	if err != nil {
		return err
	}

	c.Retry.ParsedBackoff, err = parseDuration("retry.initial_backoff", c.Retry.InitialBackoff)
	if err != nil {
		return err
	}
	// initial_backoff below 5ns causes rand.Int63n(0) panic in the retry
	// jitter calculation inside a goroutine, crashing the whole process.
	if c.Retry.ParsedBackoff < time.Millisecond {
		return fmt.Errorf("retry.initial_backoff must be >= 1ms to avoid jitter panic, got %s", c.Retry.InitialBackoff)
	}

	// idle_timeout defaults to 5m if not set (non-fatal).
	if c.IdleTimeout != "" {
		c.ParsedIdleTimeout, err = parseDuration("idle_timeout", c.IdleTimeout)
		if err != nil {
			return err
		}
	} else {
		c.ParsedIdleTimeout = 5 * time.Minute
	}

	// health check durations are only required when health checks are enabled.
	if c.HealthCheck.Enabled {
		c.HealthCheck.ParsedInterval, err = parseDuration("health_check.interval", c.HealthCheck.Interval)
		if err != nil {
			return err
		}
		c.HealthCheck.ParsedTimeout, err = parseDuration("health_check.timeout", c.HealthCheck.Timeout)
		if err != nil {
			return err
		}
	}

	return nil
}
