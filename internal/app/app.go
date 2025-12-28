package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"log-analytics/internal/aggregators"
	"log-analytics/internal/events"
	internalhttp "log-analytics/internal/http"
	"log-analytics/internal/ingestors"
	"log-analytics/internal/shared/configs"
	"log-analytics/internal/shared/filestorages"
	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/stores"
	"log-analytics/internal/streams"
	"log-analytics/internal/models"
)

// App holds all application dependencies and manages lifecycle.
type App struct {
	config    *configs.Config
	appLogger loggers.Logger
	server    *http.Server

	partialInsightConsumer streams.PartialInsightConsumer
	backgroundCtx          context.Context
	backgroundCancel       context.CancelFunc
}

// New creates and initializes a new App instance.
func New(config *configs.Config) (*App, error) {
	appLogger, err := loggers.New(config.Log.Level)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	appLogger = appLogger.With().
		Str(loggers.FieldApp, "log-analytics").
		Logger()

	// Initialize blob store
	fileStorage, err := filestorages.NewFileStorage(config.FileStorage.RootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Initialize stream queue
	partialInsightQueue := streams.NewPartitionedQueue[events.PartialInsightEvent]()

	// Initialize aggregation service
	windowSize, err := models.NewWindowSizeFromString(config.Aggregation.WindowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize window size: %w", err)
	}
	aggregateResultStore := stores.NewAggregateResultStore(fileStorage)
	aggregateRolluper := aggregators.NewAggregateRolluper()
	aggregationService := aggregators.NewAggregationService(aggregateRolluper, aggregateResultStore)
	consumerLogger := appLogger.With().Str(loggers.FieldComponent, "consumer").Logger()
	partialInsightConsumer := streams.NewPartialInsightConsumer(partialInsightQueue, aggregationService, consumerLogger)

	// Initialize ingestionService
	batchStore := stores.NewLogBatchStore(fileStorage)
	batchSummarizer := ingestors.NewBatchSummarizer(windowSize)
	partialInsightProducer := streams.NewPartialInsightProducer(partialInsightQueue)
	ingestionService := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	// Initialize http qrouter
	httpLogger := appLogger.With().Str(loggers.FieldComponent, "http").Logger()
	router := internalhttp.NewRouter(ingestionService, httpLogger)

	// Create HTTP server
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.Server.Port),
		Handler:           router,
		ReadHeaderTimeout: time.Duration(config.Server.ReadHeaderTimeout) * time.Second,
		ReadTimeout:       time.Duration(config.Server.ReadTimeout) * time.Second,
		WriteTimeout:      time.Duration(config.Server.WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(config.Server.IdleTimeout) * time.Second,
	}

	return &App{
		config:                 config,
		appLogger:              appLogger,
		server:                 server,
		partialInsightConsumer: partialInsightConsumer,
	}, nil
}

// Start starts the HTTP server in a blocking manner.
func (app *App) Start() error {
	app.appLogger.Info().
		Msgf("Starting log-analytics service on port %d (log_level=%s, file_storage_root_dir=%s)",
			app.config.Server.Port,
			app.config.Log.Level,
			app.config.FileStorage.RootDir)

	// start background consumers
	app.backgroundCtx, app.backgroundCancel = context.WithCancel(context.Background())
	app.partialInsightConsumer.Start(app.backgroundCtx)

	return app.server.ListenAndServe()
}

// Shutdown gracefully shuts down the application.
func (app *App) Shutdown(ctx context.Context) error {
	// 1) Shutdown server
	app.appLogger.Info().Msg("Shutting down server...")
	if err := app.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}
	app.appLogger.Info().Msg("Server stopped")
	// 2) Cancel background consumers
	if app.backgroundCancel != nil {
		app.backgroundCancel()
		app.appLogger.Info().Msg("Background consumers cancelled")
	}

	// 3) Wait for background consumers to finish
	app.partialInsightConsumer.Stop()
	app.appLogger.Info().Msg("Background consumers stopped")

	return nil
}
