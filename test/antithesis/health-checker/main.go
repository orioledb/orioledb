package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const setupCompleteJSON = `{"antithesis_setup":{"status":"complete","details":{"message":"OrioleDB workload is ready for testing"}}}`

func main() {
	if marker := os.Getenv("SETUP_READY_FILE"); marker != "" {
		deadline := time.Now().Add(30 * time.Minute)
		for {
			if _, err := os.Stat(marker); err == nil {
				break
			} else if !os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "health-checker: stat %s: %v\n", marker, err)
				os.Exit(1)
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "health-checker: timed out waiting for %s\n", marker)
				os.Exit(1)
			}
			time.Sleep(time.Second)
		}
	}

	outputDir := os.Getenv("ANTITHESIS_OUTPUT_DIR")
	if outputDir == "" {
		fmt.Fprintln(os.Stderr, "health-checker: ANTITHESIS_OUTPUT_DIR is not set")
		os.Exit(1)
	}

	outputPath := filepath.Join(outputDir, "sdk.jsonl")
	output, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health-checker: open %s: %v\n", outputPath, err)
		os.Exit(1)
	}

	if _, err := fmt.Fprintln(output, setupCompleteJSON); err != nil {
		_ = output.Close()
		fmt.Fprintf(os.Stderr, "health-checker: write %s: %v\n", outputPath, err)
		os.Exit(1)
	}
	if err := output.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "health-checker: close %s: %v\n", outputPath, err)
		os.Exit(1)
	}

	fmt.Println("health-checker: workload ready")
}
