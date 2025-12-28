package validators

import (
	"github.com/go-playground/validator/v10"
)

// Validate is a type alias for validator.Validate.
type Validate = validator.Validate

// ValidationErrors is a type alias for validator.ValidationErrors.
type ValidationErrors = validator.ValidationErrors

// FieldError is a type alias for validator.FieldError.
type FieldError = validator.FieldError

// New creates a new validator instance.
func New() *Validate {
	return validator.New()
}
