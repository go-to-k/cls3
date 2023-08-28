package wrapper

import (
	"fmt"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	fmt.Println()
	fmt.Println("==========================================")
	fmt.Println("=========== Start Test: wrapper ==========")
	fmt.Println("==========================================")
	goleak.VerifyTestMain(m)
}
