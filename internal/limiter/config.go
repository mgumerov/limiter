package limiter

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	//This is better be decoupled from reading because we could load config from different sources. For now, we don't.
	if cfg.MaxRequest == 0 {
		return nil, fmt.Errorf("Maximum request size is not defined")
	}

	return &cfg, nil
}
