package svcerrors

import (
	"errors"
	"fmt"
)

const (
	categoryInvalidArgument  = "invalid_argument"
	categoryResourceConflict = "resource_conflict"
	categoryInternal         = "internal"
)

const (
	errorCodeInternalPanic     = "SYS_9000"
	errorCodeInternalUndefined = "SYS_9001"
)

// NewInvalidArgumentError creates a new ServiceError with category invalid_argument.
func NewInvalidArgumentError(code, message string, cause error) *ServiceError {
	return &ServiceError{
		Category:       categoryInvalidArgument,
		Code:           code,
		Message:        message,
		Cause:          cause,
		HttpStatusCode: 400,
	}
}

// NewInternalError creates a new ServiceError with category internal.
func NewInternalError(code string, cause error) *ServiceError {
	return &ServiceError{
		Category:       categoryInternal,
		Code:           code,
		Message:        "internal server error",
		Cause:          cause,
		HttpStatusCode: 500,
	}
}

// NewInternalErrorUndefined creates a new ServiceError with category internal and code SYS_9000.
func NewInternalErrorUndefined(cause error) *ServiceError {
	return NewInternalError(errorCodeInternalUndefined, cause)
}

func NewInternalErrorPanic(cause error) *ServiceError {
	return NewInternalError(errorCodeInternalPanic, cause)
}

// NewResourceConflictError creates a new ServiceError with category resource_conflict.
func NewResourceConflictError(code, message string, cause error) *ServiceError {
	return &ServiceError{
		Category:       categoryResourceConflict,
		Code:           code,
		Message:        message,
		Cause:          cause,
		HttpStatusCode: 409,
	}
}

func AsServiceError(err error) (*ServiceError, bool) {
	var svcErr *ServiceError
	if errors.As(err, &svcErr) {
		return svcErr, true
	}
	return nil, false
}

// ServiceError represents a service-level error with category, code, message, and cause.
// It implements the error interface and supports error wrapping.
type ServiceError struct {
	Category       string // invalid_argument or internal
	Code           string // service-owned stable code (e.g. LOGS_1000)
	Message        string // client-safe, human-readable
	Cause          error  // wrapped underlying error
	HttpStatusCode int    // HTTP status code
}

// Error implements the error interface.
func (e *ServiceError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying error to support errors.Is and errors.As.
func (e *ServiceError) Unwrap() error {
	return e.Cause
}

// As extracts a ServiceError from the error chain.
// It returns (*ServiceError, true) if err wraps a ServiceError, otherwise (nil, false).
func As(err error) (*ServiceError, bool) {
	var svcErr *ServiceError
	if errors.As(err, &svcErr) {
		return svcErr, true
	}
	return nil, false
}

func (e *ServiceError) IsInternalError() bool {
	return e.Category == categoryInternal
}
