package configs

import (
	"fmt"
	"strings"

	"log-analytics/internal/shared/validators"

	"github.com/spf13/viper"
)

// LoadConfig reads configuration from file and validates it.
var LoadConfig = func(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// Read from file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", configPath, err)
	}

	// Unmarshal into Config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate config
	validate := validators.New()
	if err := validate.Struct(&cfg); err != nil {
		var validationErrors []string
		if ve, ok := err.(validators.ValidationErrors); ok {
			for _, e := range ve {
				validationErrors = append(validationErrors, formatValidationError(e))
			}
		}
		return nil, fmt.Errorf("config validation failed: %s", strings.Join(validationErrors, ", "))
	}

	return &cfg, nil
}

// formatValidationError formats a single validation error into a readable string.
func formatValidationError(e validators.FieldError) string {
	field := e.Field()
	tag := e.Tag()

	// Build field path (e.g., "server.port")
	if e.StructNamespace() != "" {
		// Extract nested field path (e.g., "Config.Server.Port" -> "server.port")
		parts := strings.Split(e.StructNamespace(), ".")
		if len(parts) >= 2 {
			// Skip "Config" prefix, convert to lowercase with dots
			fieldPath := strings.ToLower(strings.Join(parts[1:], "."))
			field = fieldPath
		}
	}

	var msg string
	switch tag {
	case "required":
		msg = fmt.Sprintf("%s (required)", field)
	case "min":
		msg = fmt.Sprintf("%s (min=%s)", field, e.Param())
	case "max":
		msg = fmt.Sprintf("%s (max=%s)", field, e.Param())
	case "oneof":
		msg = fmt.Sprintf("%s (oneof=%s)", field, e.Param())
	default:
		msg = fmt.Sprintf("%s (%s)", field, tag)
	}

	return msg
}
