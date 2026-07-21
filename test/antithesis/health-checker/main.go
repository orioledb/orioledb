package main

import (
	"fmt"
	"os"
	"path/filepath"
)

const setupCompleteJSON = `{"antithesis_setup":{"status":"complete","details":{"message":"OrioleDB workload is ready for testing"}}}`

func main() {
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
