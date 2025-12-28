package configs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_ValidConfig(t *testing.T) {
	// Create a temporary config file
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	validConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: debug
file_storage:
  root_dir: ./data
aggregation:
  window_size: minute
`

	_, err = tmpfile.WriteString(validConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 5, cfg.Server.ReadHeaderTimeout)
	assert.Equal(t, 10, cfg.Server.ReadTimeout)
	assert.Equal(t, 10, cfg.Server.WriteTimeout)
	assert.Equal(t, 60, cfg.Server.IdleTimeout)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "./data", cfg.FileStorage.RootDir)
	assert.Equal(t, "minute", cfg.Aggregation.WindowSize)
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	// Create a temporary config file with missing port
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: debug
file_storage:
  root_dir: ./data
aggregation:
  window_size: minute
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
	assert.Contains(t, err.Error(), "port")
}

func TestLoadConfig_InvalidLogLevel(t *testing.T) {
	// Create a temporary config file with invalid log level
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: invalid
file_storage:
  root_dir: ./data
aggregation:
  window_size: minute
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "invalid", cfg.Log.Level)
}

func TestLoadConfig_InvalidPortRange(t *testing.T) {
	// Create a temporary config file with invalid port
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  port: 70000
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: info
file_storage:
  root_dir: ./data
aggregation:
  window_size: minute
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
	assert.Contains(t, err.Error(), "port")
}

func TestLoadConfig_MissingFileStorageRootDir(t *testing.T) {
	// Create a temporary config file with missing root directory
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: info
file_storage: {}
aggregation:
  window_size: minute
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
	assert.Contains(t, err.Error(), " filestorage.rootdir")
}

func TestLoadConfig_MissingAggregationConfig(t *testing.T) {
	// Create a temporary config file with missing aggregation config
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: info
file_storage:
  root_dir: ./data
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
	assert.Contains(t, err.Error(), "aggregation")
}

func TestLoadConfig_MissingWindowSize(t *testing.T) {
	// Create a temporary config file with missing window_size
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: info
file_storage:
  root_dir: ./data
aggregation: {}
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
	assert.Contains(t, err.Error(), "aggregation.windowsize")
}

func TestLoadConfig_InvalidWindowSize(t *testing.T) {
	// Create a temporary config file with invalid window_size
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	invalidConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: info
file_storage:
  root_dir: ./data
aggregation:
  window_size: day
`

	_, err = tmpfile.WriteString(invalidConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
	assert.Contains(t, err.Error(), "aggregation.windowsize")
	assert.Contains(t, err.Error(), "oneof")
}

func TestLoadConfig_ValidWindowSizeHour(t *testing.T) {
	// Create a temporary config file with valid hour window_size
	tmpfile, err := os.CreateTemp("", "test_config_*.yml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	validConfig := `server:
  port: 8080
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 10
  idle_timeout: 60
log:
  level: debug
file_storage:
  root_dir: ./data
aggregation:
  window_size: hour
`

	_, err = tmpfile.WriteString(validConfig)
	require.NoError(t, err)
	tmpfile.Close()

	cfg, err := LoadConfig(tmpfile.Name())
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "hour", cfg.Aggregation.WindowSize)
}
