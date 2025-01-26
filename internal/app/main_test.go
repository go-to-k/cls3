package app

import (
	"fmt"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	fmt.Println()
	fmt.Println("==========================================")
	fmt.Println("=========== Start Test: app ==========")
	fmt.Println("==========================================")
	goleak.VerifyTestMain(m)
}
