package http

import (
	"net/http"
	"strings"
)

const (
	headerRequestID      = "x-request-id"
	headerContentType    = "content-type"
	headerIdempotencyKey = "idempotency-key"
	headerCustomerID     = "x-customer-id"
)

func requestID(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(headerRequestID))
}

func setRequestID(r *http.Request, requestID string) {
	r.Header.Set(headerRequestID, requestID)
}

func contentType(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(headerContentType))
}

func idempotencyKey(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(headerIdempotencyKey))
}

func customerID(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(headerCustomerID))
}
