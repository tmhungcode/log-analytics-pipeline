package ingestors

import (
	"fmt"
	"log-analytics/internal/shared/svcerrors"
)

// IngestionService errors
const (
	codeValidationFailed      = "ING_1000"
	codeBatchAlreadyProcessed = "ING_1001"

	codeInternalLogBatchStoreFailed           = "ING_9000"
	codeInternalPartialInsightPublisherFailed = "ING_9001"
)

// ErrValidationFailed returns an error for validation failures.
func errValidationFailed(msg string, cause error) *svcerrors.ServiceError {
	return svcerrors.NewInvalidArgumentError(codeValidationFailed, msg, cause)
}

// errLogBatchAlreadyProcessed returns an error when a log batch has already been processed.
func errLogBatchAlreadyProcessed(cause error) *svcerrors.ServiceError {
	return svcerrors.NewResourceConflictError(codeBatchAlreadyProcessed, "log batch already processed", cause)
}

// errInternalLogBatchStoreFailed returns an error when a log batch store operation fails.
func errInternalLogBatchStoreFailed(cause error) *svcerrors.ServiceError {
	return svcerrors.NewInternalError(codeInternalLogBatchStoreFailed, fmt.Errorf("logBatchStoreFailed: %w", cause))
}

// codeInternalPartialInsightPublisherFailed returns an error when a partial insight publisher operation fails.
func errInternalPartialInsightPublisherFailed(cause error) *svcerrors.ServiceError {
	return svcerrors.NewInternalError(codeInternalPartialInsightPublisherFailed, fmt.Errorf("partialInsightPublisherFailed: %w", cause))
}
