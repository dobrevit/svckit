package env_test

import (
	"fmt"
	"os"
	"time"

	"github.com/dobrevit/svckit/env"
)

// Each reader takes the default inline, so configuration reads as one
// expression rather than a lookup followed by a zero-value check.
func Example() {
	os.Setenv("PORT", "9000")
	os.Setenv("FEATURE_ENABLED", "true")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("FEATURE_ENABLED")
	}()

	fmt.Println(env.String("PORT", "8080"))
	fmt.Println(env.Bool("FEATURE_ENABLED", false))
	fmt.Println(env.Duration("SHUTDOWN_GRACE", 30*time.Second))
	// Output:
	// 9000
	// true
	// 30s
}
