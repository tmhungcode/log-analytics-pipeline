package configs

// Config holds all configuration for the application.
type Config struct {
	Server      ServerConfig      `mapstructure:"server" validate:"required"`
	Log         LogConfig         `mapstructure:"log" validate:"required"`
	FileStorage FileStorageConfig `mapstructure:"file_storage" validate:"required"`
	Aggregation AggregationConfig `mapstructure:"aggregation" validate:"required"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	Port              int `mapstructure:"port" validate:"required,min=1,max=65535"`
	ReadHeaderTimeout int `mapstructure:"read_header_timeout" validate:"required,min=1"` // seconds
	ReadTimeout       int `mapstructure:"read_timeout" validate:"required,min=1"`        // seconds (headers+body)
	WriteTimeout      int `mapstructure:"write_timeout" validate:"required,min=1"`       // seconds (response)
	IdleTimeout       int `mapstructure:"idle_timeout" validate:"required,min=1"`        // seconds (keep-alive)
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level string `mapstructure:"level" validate:"required"`
}

// FileStorageConfig holds file storage configuration.
type FileStorageConfig struct {
	RootDir string `mapstructure:"root_dir" validate:"required"`
}

// AggregationConfig holds aggregation configuration.
type AggregationConfig struct {
	WindowSize string `mapstructure:"window_size" validate:"required,oneof=minute hour"`
}
