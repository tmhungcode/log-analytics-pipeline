package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ### Start - fixed configs (no change)
// These values define deterministic test data generation and must match expected results.
// DO NOT MODIFY: Changing these will break the test's deterministic behavior.
const (
	totalEntries = 64000 // Total number of unique log entries to generate
)

var (
	minutes    = []string{"18:03", "18:04", "18:05", "18:06"}
	paths      = []string{"/", "/about", "/careers", "/contact"}
	userAgents = []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"curl/7.88.1",
	}
)

// ### End - fixed configs

type entry struct {
	bucket int
	round  int
}

type logEntry struct {
	ReceivedAt string `json:"receivedAt"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	UserAgent  string `json:"userAgent"`
}

type batchToSend struct {
	batchIndex int
	jsonData   []byte
	isOriginal bool
}

// main runs the e2e scenario: 001_basic_minute_rollup
//
// This scenario tests the end-to-end flow of log ingestion, batch summarization,
// and minute-level aggregation rollup. It sends 64,000 log entries across multiple
// batches to the log analytics API, with configurable duplicate batches to test
// idempotency handling.
//
// What it tests:
//   - Log batch ingestion via POST /logs endpoint
//   - Idempotency key handling for duplicate batch detection
//   - Batch summarization into minute-level aggregates
//   - Partial insight event production and consumption
//   - Window aggregate result rollup and storage
//   - Race conditions when multiple partial insights concurrently update the same aggregate result
//
// Expected results:
//   - All batches are successfully ingested (original + duplicates)
//   - Duplicate batches return 409 Conflict status (idempotency working)
//   - Four minute-level aggregate results are generated (18:03, 18:04, 18:05, 18:06 UTC)
//   - Each minute window contains 16,000 requests distributed across 4 paths and 4 user agents
//   - Aggregate results are stored in the file storage directory
//   - Race conditions when multiple partial insights update the same aggregate are handled correctly
//     (concurrent rollups to the same window aggregate result maintain data integrity)
//
// For detailed expected results, see the expected/ directory in this scenario folder.
func main() {
	// these configs can be changed to run the scenario
	baseURL := "http://localhost:8080"    // Base URL of the log analytics API server
	dateUTC := "2025-12-28"               // Date used for generating log entry timestamps (UTC)
	itemsPerBatch := 20                   // Number of log entries per batch. Original batches = totalEntries / itemsPerBatch
	parallel := 2                         // Number of concurrent batch requests to send
	totalDuplicates := 2000               // Total number of duplicate batches to send across all batches. Total batches sent = original batches + duplicate batches
	customerID := "cus-axon"              // Customer ID to use in requests
	fileStorageDir := ".tmp/file-storage" // File storage directory path relative to project root
	wantCleanFileStorage := true          // If true, clean up file storage directory before running scenario

	// Validate itemsPerBatch divides evenly
	if totalEntries%itemsPerBatch != 0 {
		fmt.Fprintf(os.Stderr, "ERROR: TOTAL_ENTRIES (%d) must be divisible by ITEMS_PER_BATCH (%d)\n", totalEntries, itemsPerBatch)
		os.Exit(1)
	}

	batchCount := totalEntries / itemsPerBatch

	// Get project root directory by looking for go.mod file
	// Start from current working directory and walk up until we find go.mod
	projectRoot, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to get current working directory: %v\n", err)
		os.Exit(1)
	}

	// Walk up the directory tree to find go.mod
	for i := 0; i < 10; i++ {
		goModPath := filepath.Join(projectRoot, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			break
		}
		parent := filepath.Dir(projectRoot)
		if parent == projectRoot {
			// Reached filesystem root without finding go.mod
			fmt.Fprintf(os.Stderr, "ERROR: Could not find go.mod file. Please run from project root or set FILE_STORAGE_DIR to absolute path\n")
			os.Exit(1)
		}
		projectRoot = parent
	}

	// Resolve file storage directory relative to project root
	storagePath := filepath.Join(projectRoot, fileStorageDir)
	storagePath, err = filepath.Abs(storagePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to resolve file storage path: %v\n", err)
		os.Exit(1)
	}

	// Clean up file storage if requested
	if wantCleanFileStorage {
		fmt.Printf("Cleaning file storage directory: %s\n", storagePath)
		if err := os.RemoveAll(storagePath); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Failed to clean file storage directory: %v\n", err)
		} else {
			fmt.Printf("File storage directory cleaned\n")
		}
		fmt.Println()
	}

	fmt.Println("Starting e2e scenario: 001_basic_minute_rollup")
	fmt.Printf("BASE_URL: %s\n", baseURL)
	fmt.Printf("DATE_UTC: %s\n", dateUTC)
	fmt.Printf("ITEMS_PER_BATCH: %d\n", itemsPerBatch)
	fmt.Printf("BATCH_COUNT: %d\n", batchCount)
	fmt.Printf("PARALLEL: %d\n", parallel)
	fmt.Printf("TOTAL_DUPLICATES: %d\n", totalDuplicates)
	fmt.Printf("FILE_STORAGE_DIR: %s\n", fileStorageDir)
	fmt.Printf("FILE_STORAGE_PATH: %s\n", storagePath)
	fmt.Printf("WANT_CLEAN_FILE_STORAGE: %v\n", wantCleanFileStorage)
	fmt.Printf("TOTAL_ENTRIES: %d\n", totalEntries)
	fmt.Println()

	// Generate all entries
	fmt.Printf("Generating all %d entries...\n", totalEntries)
	entries := generateAllEntries()
	fmt.Printf("Generated %d entries\n", len(entries))
	fmt.Println()

	// Generate all batches (original + duplicates) and sort by batchIndex
	fmt.Printf("Generating all batches (original + duplicates)...\n")
	batchesToSend := make([]batchToSend, 0, batchCount+totalDuplicates)

	// First, generate all original batches
	for batchIndex := 1; batchIndex <= batchCount; batchIndex++ {
		jsonData, err := generateBatchJSON(batchIndex, itemsPerBatch, entries, batchCount, dateUTC)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to generate JSON for batch %d: %v\n", batchIndex, err)
			os.Exit(1)
		}
		batchesToSend = append(batchesToSend, batchToSend{
			batchIndex: batchIndex,
			jsonData:   jsonData,
			isOriginal: true,
		})
	}

	// Then, add duplicate batches until we reach totalDuplicates
	// Distribute duplicates evenly across batches using round-robin
	duplicatesAdded := 0
	batchIndex := 1
	for duplicatesAdded < totalDuplicates {
		// Reuse the JSON data from the original batch
		jsonData := batchesToSend[batchIndex-1].jsonData
		batchesToSend = append(batchesToSend, batchToSend{
			batchIndex: batchIndex,
			jsonData:   jsonData,
			isOriginal: false,
		})
		duplicatesAdded++
		batchIndex++
		if batchIndex > batchCount {
			batchIndex = 1 // Round-robin back to first batch
		}
	}

	// Sort by batchIndex to ensure proper ordering
	sort.Slice(batchesToSend, func(i, j int) bool {
		return batchesToSend[i].batchIndex < batchesToSend[j].batchIndex
	})

	fmt.Printf("Generated %d batches to send (%d original + %d duplicates)\n",
		len(batchesToSend), batchCount, len(batchesToSend)-batchCount)
	fmt.Println()

	// Create worker pool for parallel batch sending
	workerChan := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	var totalBatchesSent int64   // original + duplicate batches
	var duplicateBatchSent int64 // duplicate batches only
	var conflictedRequest int64  // 409 status code
	var acceptedRequest int64    // 202 status code
	var invalidRequest int64     // 400 status code
	var internalRequest int64    // 500 status code

	// Send all batches
	for _, batch := range batchesToSend {
		wg.Add(1)
		workerChan <- struct{}{} // Acquire worker slot

		go func(b batchToSend) {
			defer wg.Done()
			defer func() { <-workerChan }() // Release worker slot

			statusCode, err := sendBatchWithJSON(baseURL, customerID, b)
			if err != nil {
				mu.Lock()
				if b.isOriginal {
					errors = append(errors, fmt.Errorf("batch %d: %w", b.batchIndex, err))
				} else {
					errors = append(errors, fmt.Errorf("batch %d (duplicate): %w", b.batchIndex, err))
				}
				mu.Unlock()
				if b.isOriginal {
					fmt.Fprintf(os.Stderr, "ERROR: Batch %d failed: %v\n", b.batchIndex, err)
				} else {
					fmt.Fprintf(os.Stderr, "ERROR: Batch %d (duplicate) failed: %v\n", b.batchIndex, err)
				}
			} else {
				// Track statistics
				atomic.AddInt64(&totalBatchesSent, 1)
				if !b.isOriginal {
					atomic.AddInt64(&duplicateBatchSent, 1)
				}

				// Track status codes
				switch statusCode {
				case http.StatusAccepted:
					atomic.AddInt64(&acceptedRequest, 1)
				case http.StatusBadRequest:
					atomic.AddInt64(&invalidRequest, 1)
				case http.StatusConflict:
					atomic.AddInt64(&conflictedRequest, 1)
				case http.StatusInternalServerError:
					atomic.AddInt64(&internalRequest, 1)
				}

				if b.isOriginal {
					fmt.Printf("Batch %d completed (status %d)\n", b.batchIndex, statusCode)
				} else {
					fmt.Printf("Batch %d (duplicate) completed (status %d)\n", b.batchIndex, statusCode)
				}
			}
		}(batch)
	}

	// Wait for all batches to complete
	wg.Wait()

	fmt.Println()
	if len(errors) > 0 {
		fmt.Fprintf(os.Stderr, "ERROR: %d batch sends failed\n", len(errors))
		os.Exit(1)
	}

	// Print statistics
	totalBatches := atomic.LoadInt64(&totalBatchesSent)
	duplicateBatches := atomic.LoadInt64(&duplicateBatchSent)
	originalBatches := totalBatches - duplicateBatches
	conflicted := atomic.LoadInt64(&conflictedRequest)
	accepted := atomic.LoadInt64(&acceptedRequest)
	invalid := atomic.LoadInt64(&invalidRequest)
	internal := atomic.LoadInt64(&internalRequest)

	fmt.Println("All batches completed successfully")
	fmt.Println("=== Statistics ===")
	fmt.Printf("Total batches sent: %d\n", totalBatches)
	fmt.Printf("Duplicate batch sent: %d\n", duplicateBatches)
	fmt.Printf("Original batch sent: %d\n", originalBatches)
	fmt.Printf("Conflicted request: %d\n", conflicted)
	fmt.Printf("Accepted request: %d\n", accepted)
	fmt.Printf("Invalid request: %d\n", invalid)
	fmt.Printf("Internal request: %d\n", internal)
	fmt.Printf("Total unique entries sent: %d\n", totalEntries)
	fmt.Println("Scenario completed successfully")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func generateAllEntries() []entry {
	entries := make([]entry, 0, totalEntries)
	bucket := 0
	round := 0

	for count := 0; count < totalEntries; count++ {
		entries = append(entries, entry{bucket: bucket, round: round})

		bucket++
		if bucket >= 64 {
			bucket = 0
			round++
		}
	}

	return entries
}

func generateEntryJSON(e entry, dateUTC string) logEntry {
	// Calculate indices
	minuteIndex := e.bucket / 16
	combo := e.bucket % 16
	pathIndex := combo / 4
	uaIndex := combo % 4

	// Get values
	minute := minutes[minuteIndex]
	path := paths[pathIndex]
	ua := userAgents[uaIndex]

	// Calculate timestamp components
	seconds := e.round % 60
	milliseconds := (e.bucket*17 + e.round) % 1000

	// Build timestamp: YYYY-MM-DDTHH:MM:SS.mmmZ
	timestamp := fmt.Sprintf("%sT%s:%02d.%03dZ", dateUTC, minute, seconds, milliseconds)

	return logEntry{
		ReceivedAt: timestamp,
		Method:     "GET",
		Path:       path,
		UserAgent:  ua,
	}
}

func generateBatchJSON(batchIndex, batchSize int, entries []entry, batchCount int, dateUTC string) ([]byte, error) {
	startIndex := (batchIndex - 1) * batchSize
	stride := batchCount + 1

	logEntries := make([]logEntry, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		// Use stride pattern to mix entries across batches
		entryIndex := (startIndex + i*stride) % totalEntries
		entry := entries[entryIndex]
		logEntries = append(logEntries, generateEntryJSON(entry, dateUTC))
	}

	return json.Marshal(logEntries)
}

func sendBatchWithJSON(baseURL, customerID string, batch batchToSend) (int, error) {
	// Generate idempotency key (zero-padded to 6 digits)
	// Same key for all duplicates of this batch
	idempotencyKey := fmt.Sprintf("batch-%06d", batch.batchIndex)

	// Create HTTP request with cached JSON data
	// Create a new reader for each request (bytes.NewReader is safe for concurrent use)
	req, err := http.NewRequest("POST", baseURL+"/logs", bytes.NewReader(batch.jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-customer-id", customerID)
	req.Header.Set("idempotency-key", idempotencyKey)

	// Send request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Return status code and error handling:
	// - 409 Conflict: return status code with nil error (expected for duplicates)
	// - Other 4xx/5xx: return status code with error
	// - 2xx/3xx: return status code with nil error (success)
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusConflict {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return resp.StatusCode, nil
}
