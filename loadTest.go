package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	numClients = 100
)

type reqResult struct {
	id       int
	duration time.Duration
	success  bool
}

func client(id int, target string, payload []byte, wg *sync.WaitGroup, results chan<- reqResult) {
	defer wg.Done()

	start := time.Now()
	conn, err := net.Dial("tcp", target)
	if err != nil {
		// fmt.Printf("Client %d: connection failed: %v\n", id, err)
		results <- reqResult{id, time.Since(start), false}
		return
	}
	defer conn.Close()

	// Send a small request.
	// message := fmt.Sprintf("Hello from client %d", id)

	_, err = conn.Write(payload)
	if err != nil {
		// fmt.Printf("Client %d: write failed: %v\n", id, err)
		results <- reqResult{id, time.Since(start), false}
		return
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}

	// Read until EOF to complete the round trip
	respBuf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(respBuf)

	if err != nil && err != io.EOF {
		results <- reqResult{id, time.Since(start), false}
		return
	}

	respStr := string(respBuf[:n])
	if strings.Contains(respStr, "429") || strings.Contains(respStr, "Too Many Requests") {
		results <- reqResult{id, time.Since(start), false}
		return
	}
	results <- reqResult{id, time.Since(start), true}
}

func main() {
	clients := flag.Int("clients", 100, "Number of concurrent clients")
	rampUp := flag.Duration("rampup", 10*time.Millisecond, "Delay between client starts")
	payloadSize := flag.Int("payload", 1024, "Payload size in bytes")
	target := flag.String("target", "localhost:8080", "Target address (host:port)")
	// csvFile := flag.String("csv", "benchmark_results.csv", "Output CSV file")
	flag.Parse()

	fmt.Printf("Starting benchmark: %d clients, %d byte payload, targeting %s\n", *clients, *payloadSize, *target)

	payload := bytes.Repeat([]byte("A"), *payloadSize)
	results := make(chan reqResult, *clients)

	var wg sync.WaitGroup

	wg.Add(*clients)
	for i := 1; i <= *clients; i++ {
		go client(i, *target, payload, &wg, results)
		time.Sleep(*rampUp)
	}

	// Wait for completion and close channel
	go func() {
		wg.Wait()
		close(results)
	}()

	var successes, failures int
	var durations []float64
	var allResults []reqResult

	// Collect metrics
	for res := range results {
		allResults = append(allResults, res)
		if res.success {
			successes++
			durations = append(durations, float64(res.duration.Milliseconds()))
		} else {
			failures++
		}
	}

	// Calculate aggregates
	sort.Float64s(durations)
	var p50, p95, p99 float64
	if len(durations) > 0 {
		p50 = durations[int(float64(len(durations))*0.50)]
		p95 = durations[int(float64(len(durations))*0.95)]
		p99 = durations[int(float64(len(durations))*0.99)]
	}

	// Print Summary
	fmt.Println("\n--- Benchmark Summary ---")
	fmt.Printf("Total Requests: %d\n", *clients)
	fmt.Printf("Successes:      %d\n", successes)
	fmt.Printf("Failures:       %d\n", failures)
	if successes > 0 {
		fmt.Printf("p50 Latency:    %.2f ms\n", p50)
		fmt.Printf("p95 Latency:    %.2f ms\n", p95)
		fmt.Printf("p99 Latency:    %.2f ms\n", p99)
	}

	// Write CSV
	// file, err := os.Create(*csvFile)
	// if err != nil {
	// 	fmt.Printf("Failed to create CSV: %v\n", err)
	// 	return
	// }
	// defer file.Close()

	// writer := csv.NewWriter(file)
	// defer writer.Flush()

	// writer.Write([]string{"client_id", "duration_ms", "status"})
	// for _, res := range allResults {
	// 	status := "success"
	// 	if !res.success {
	// 		status = "fail"
	// 	}
	// 	writer.Write([]string{
	// 		fmt.Sprintf("%d", res.id),
	// 		fmt.Sprintf("%d", res.duration.Milliseconds()),
	// 		status,
	// 	})
	// }
	// fmt.Printf("\nRaw results written to %s\n", *csvFile)
}
