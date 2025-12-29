package metrics

import (
	"fmt"
	"sync/atomic"
	"time"
)

func StartProgressBar(total int64, done <-chan struct{}) {
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			processed := atomic.LoadInt64(&ProcessedLines)
			inserted := atomic.LoadInt64(&InsertedRows)

			elapsed := time.Since(start).Seconds()
			speed := float64(processed) / elapsed

			var percent float64
			var eta string

			if total > 0 {
				percent = float64(processed) / float64(total) * 100
				remaining := float64(total-processed) / speed
				eta = time.Duration(remaining * float64(time.Second)).String()
			} else {
				percent = 0
				eta = "N/A"
			}

			fmt.Printf(
				"\rProgress: %6.2f%% | Read: %d | Inserted: %d | %.0f rows/s | ETA: %s",
				percent,
				processed,
				inserted,
				speed,
				eta,
			)

		case <-done:
			fmt.Println()
			return
		}
	}
}
