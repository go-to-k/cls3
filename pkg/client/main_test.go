package client

import (
	"fmt"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	fmt.Println()
	fmt.Println("==========================================")
	fmt.Println("=========== Start Test: client ===========")
	fmt.Println("==========================================")
	goleak.VerifyTestMain(m)
}
