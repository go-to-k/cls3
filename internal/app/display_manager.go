//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package app

import (
	"github.com/go-to-k/cls3/internal/io"
	"golang.org/x/sync/errgroup"
)

type IDisplayManager interface {
	Start(targetBuckets []string)
	Finish(targetBuckets []string) error
}

var _ IDisplayManager = (*DisplayManager)(nil)

// DisplayManager handles the lifecycle of display operations
type DisplayManager struct {
	writer    *io.Writer
	displayEg *errgroup.Group
	state     IClearingState
	quietMode bool
}

// NewDisplayManager creates a new DisplayManager instance
func NewDisplayManager(state IClearingState, quietMode bool) *DisplayManager {
	return &DisplayManager{
		state:     state,
		quietMode: quietMode,
	}
}

// Start initializes and starts the display operations
func (d *DisplayManager) Start(targetBuckets []string) {
	if d.quietMode {
		return
	}
	d.writer = io.NewWriter()

	d.displayEg = d.state.StartDisplayRoutines(targetBuckets, d.writer)
}

// Finish waits for display operations to complete and performs cleanup
func (d *DisplayManager) Finish(targetBuckets []string) error {
	if d.quietMode {
		return nil
	}

	if err := d.displayEg.Wait(); err != nil {
		return err
	}

	return d.state.OutputFinalMessages(targetBuckets)
}
