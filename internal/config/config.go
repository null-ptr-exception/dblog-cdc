package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Source struct {
	Type string `yaml:"type"`
	DSN  string `yaml:"dsn"`
}

type Target struct {
	Type string `yaml:"type"`
	DSN  string `yaml:"dsn"`
}

type CDC struct {
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	SCNType int    `yaml:"scn_type"`
	Format  string `yaml:"format"`
}

type Table struct {
	Name      string   `yaml:"name"`
	PKColumns []string `yaml:"pk_columns"`
	ChunkSize int      `yaml:"chunk_size"`
}

type Defaults struct {
	ChunkSize int      `yaml:"chunk_size"`
	PKColumns []string `yaml:"pk_columns"`
}

type Progress struct {
	Table string `yaml:"table"`
}

type Config struct {
	Source   Source   `yaml:"source"`
	Target  Target   `yaml:"target"`
	CDC     CDC      `yaml:"cdc"`
	Tables  []Table  `yaml:"tables"`
	Defaults Defaults `yaml:"defaults"`
	Progress Progress `yaml:"progress"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Defaults.ChunkSize == 0 {
		c.Defaults.ChunkSize = 10000
	}
	if len(c.Defaults.PKColumns) == 0 {
		c.Defaults.PKColumns = []string{"ID"}
	}
	if c.Progress.Table == "" {
		c.Progress.Table = "dblog_progress"
	}
	for i := range c.Tables {
		if c.Tables[i].ChunkSize == 0 {
			c.Tables[i].ChunkSize = c.Defaults.ChunkSize
		}
		if len(c.Tables[i].PKColumns) == 0 {
			c.Tables[i].PKColumns = c.Defaults.PKColumns
		}
	}
}
