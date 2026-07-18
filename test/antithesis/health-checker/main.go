package main

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

func main() {
	fmt.Println("health-checker: workload ready")
	lifecycle.SetupComplete(map[string]any{
		"message": "OrioleDB workload is ready for testing",
	})
}
