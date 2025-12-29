package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"txt-to-sqlserver/internal/db"
	"txt-to-sqlserver/internal/metrics"
	"txt-to-sqlserver/internal/model"
	"txt-to-sqlserver/internal/utils"
	"txt-to-sqlserver/internal/worker"
)

const (
	workerCount = 8
	bufferSize  = 10000
	txtFilePath = "SDEAL_20251218_210045.txt"
)

func main() {
	start := time.Now()
	ctx := context.Background()

	// =====================================================
	// 1. HITUNG TOTAL BARIS (UNTUK PROGRESS & ETA)
	// =====================================================
	log.Println("Counting total lines...")
	totalLines, err := utils.CountLines(txtFilePath)
	if err != nil {
		log.Fatal(err)
	}
	atomic.StoreInt64(&metrics.TotalLines, totalLines)
	log.Printf("Total lines: %d\n", totalLines)

	// =====================================================
	// 2. KONEKSI SQL SERVER
	// =====================================================
	dbConn, err := db.NewSQLServer(
		"sqlserver://sa:Esk4link@127.0.0.1:1433?database=example-app",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer dbConn.Close()

	// =====================================================
	// 3. BUKA FILE TXT
	// =====================================================
	file, err := os.Open(txtFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// =====================================================
	// 4. CHANNEL PIPELINE
	// =====================================================
	lines := make(chan string, bufferSize)

	ch130 := make(chan model.Sdeal130Hdr, bufferSize)
	ch131 := make(chan model.Sdeal131Det, bufferSize)
	ch132 := make(chan model.Sdeal132Mix, bufferSize)
	ch120 := make(chan model.Sdeal120Hdr, bufferSize)
	ch121 := make(chan model.Sdeal121Itm, bufferSize)
	ch122 := make(chan model.Sdeal122Det, bufferSize)
	ch123 := make(chan model.Sdeal123Mix, bufferSize)
	ch124 := make(chan model.Sdeal124Reg, bufferSize)

	// =====================================================
	// 5. PARSER WORKER POOL
	// =====================================================
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker.ParseSdealWorker(
			ctx,
			&wg,
			lines,
			ch130,
			ch131,
			ch132,
			ch120,
			ch121,
			ch122,
			ch123,
			ch124,
		)
	}

	// =====================================================
	// 6. BULK WRITERS (PER BLOCK)
	// =====================================================
	done130 := make(chan struct{})
	go worker.Bulk130(ctx, dbConn, ch130, done130)

	done131 := make(chan struct{})
	go worker.Bulk131(ctx, dbConn, ch131, done131)

	done132 := make(chan struct{})
	go worker.Bulk132(ctx, dbConn, ch132, done132)

	done120 := make(chan struct{})
	go worker.Bulk120(ctx, dbConn, ch120, done120)

	done121 := make(chan struct{})
	go worker.Bulk121(ctx, dbConn, ch121, done121)

	done122 := make(chan struct{})
	go worker.Bulk122(ctx, dbConn, ch122, done122)

	done123 := make(chan struct{})
	go worker.Bulk123(ctx, dbConn, ch123, done123)

	done124 := make(chan struct{})
	go worker.Bulk124(ctx, dbConn, ch124, done124)

	// =====================================================
	// 7. PROGRESS BAR (REAL-TIME)
	// =====================================================
	progressDone := make(chan struct{})
	go metrics.StartProgressBar(totalLines, progressDone)

	// =====================================================
	// 8. STREAMING FILE READER
	// =====================================================
	scanner := bufio.NewScanner(file)
	scanner.Buffer(
		make([]byte, 64*1024),
		20*1024*1024, // max line size 20MB
	)

	for scanner.Scan() {
		lines <- scanner.Text()

		// === INI YANG ANDA TANYAKAN ===
		atomic.AddInt64(&metrics.ProcessedLines, 1)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// =====================================================
	// 9. GRACEFUL SHUTDOWN PIPELINE
	// =====================================================
	close(lines)
	wg.Wait()

	close(ch130)
	<-done130

	close(ch131)
	<-done131

	close(ch132)
	<-done132

	close(ch120)
	<-done120

	close(ch121)
	<-done121

	close(ch122)
	<-done122

	close(ch123)
	<-done123

	close(ch124)
	<-done124

	// HENTIKAN PROGRESS BAR
	close(progressDone)

	// =====================================================
	// 10. BENCHMARK SUMMARY
	// =====================================================
	elapsed := time.Since(start)
	totalInserted := atomic.LoadInt64(&metrics.InsertedRows)

	log.Println("======================================")
	log.Println("IMPORT COMPLETED")
	log.Printf("Total rows inserted : %d\n", totalInserted)
	log.Printf("Elapsed time        : %s\n", elapsed)
	log.Printf(
		"Average throughput  : %.0f rows/sec\n",
		float64(totalInserted)/elapsed.Seconds(),
	)
	log.Println("======================================")
}
