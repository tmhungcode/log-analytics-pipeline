package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promhttppkg "github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	FieldErrorCode = "error_code"

	ValueNoError = ""

	Namespace      = "log_analytics"
	SubIngestion   = "ingestion"
	SubAggregation = "aggregation"
	SubStream      = "stream"
	SubHTTP        = "http"
)

// CounterOpts is a type alias for prometheus.CounterOpts.
type CounterOpts = prometheus.CounterOpts

// HistogramOpts is a type alias for prometheus.HistogramOpts.
type HistogramOpts = prometheus.HistogramOpts

// DefBuckets is a re-export of prometheus.DefBuckets.
var DefBuckets = prometheus.DefBuckets

// NewCounterVec creates a new CounterVec with the given CounterOpts and label names.
// It is automatically registered with the default prometheus registry.
var NewCounterVec = promauto.NewCounterVec

// NewHistogramVec creates a new HistogramVec with the given HistogramOpts and label names.
// It is automatically registered with the default prometheus registry.
var NewHistogramVec = promauto.NewHistogramVec

// PromHTTP wraps the promhttp package to provide access via metrics.promhttp.
type promHTTP struct{}

// Handler returns an http.Handler for the Prometheus metrics endpoint.
func (promHTTP) Handler() http.Handler {
	return promhttppkg.Handler()
}

// PromHTTP is an instance that wraps the promhttp package functionality.
// Access it via metrics.PromHTTP.
var PromHTTP = promHTTP{}
