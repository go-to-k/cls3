//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE -write_package_comment=false
package app

import (
	"github.com/gosuri/uilive"
	"golang.org/x/sync/errgroup"
)

type IDisplayManager interface {
	Start(targetBuckets []string) error
	Finish(targetBuckets []string) error
}

var _ IDisplayManager = (*DisplayManager)(nil)

// DisplayManager handles the lifecycle of display operations
type DisplayManager struct {
	writer    *uilive.Writer
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
func (d *DisplayManager) Start(targetBuckets []string) error {
	if d.quietMode {
		return nil
	}
	d.writer = uilive.New()
	d.writer.Start()

	var err error
	d.displayEg, err = d.state.StartDisplayRoutines(targetBuckets, d.writer)
	if err != nil {
		d.writer.Stop()
		return err
	}

	return nil
}

// Finish waits for display operations to complete and performs cleanup
func (d *DisplayManager) Finish(targetBuckets []string) error {
	if d.quietMode {
		return nil
	}
	defer d.writer.Stop()

	if err := d.displayEg.Wait(); err != nil {
		return err
	}
	if err := d.writer.Flush(); err != nil {
		return err
	}
	return d.state.OutputFinalMessages(targetBuckets)
}
