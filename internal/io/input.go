//go:generate mockgen -source=$GOFILE -package=io -destination=mock_$GOFILE
package io

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fatih/color"
)

// IInputManager defines the interface for handling user input operations
type IInputManager interface {
	InputKeywordForFilter(label string) string
	GetCheckboxes(headers []string, opts []string) ([]string, bool, error)
	GetYesNo(label string) bool
}

type InputManager struct{}

var _ IInputManager = (*InputManager)(nil)

func NewInputManager() IInputManager {
	return &InputManager{}
}

func (im *InputManager) GetCheckboxes(headers []string, opts []string) ([]string, bool, error) {
	for {
		ui := NewUI(opts, headers)
		p := tea.NewProgram(ui)
		if _, err := p.Run(); err != nil {
			return nil, false, err
		}

		checkboxes := []string{}
		for c := range ui.Choices {
			if _, ok := ui.Selected[c]; ok {
				checkboxes = append(checkboxes, ui.Choices[c])
			}
		}

		switch {
		case ui.IsCanceled:
			Logger.Warn().Msg("Canceled!")
		case len(checkboxes) == 0:
			Logger.Warn().Msg("Not selected!")
		}
		if len(checkboxes) == 0 || ui.IsCanceled {
			ok := im.GetYesNo("Do you want to finish?")
			if ok {
				Logger.Info().Msg("Finished...")
				return checkboxes, false, nil
			}
			continue
		}

		fmt.Fprintf(os.Stderr, " %s\n", color.CyanString(strings.Join(checkboxes, ", ")))

		ok := im.GetYesNo("OK?")
		if ok {
			return checkboxes, true, nil
		}
	}
}

func (im *InputManager) InputKeywordForFilter(label string) string {
	reader := bufio.NewReader(os.Stdin)

	fmt.Fprintf(os.Stderr, "%s", label)
	s, _ := reader.ReadString('\n')
	fmt.Fprintln(os.Stderr)

	s = strings.TrimSpace(s)

	return s
}

func (im *InputManager) GetYesNo(label string) bool {
	choices := "Y/n"
	r := bufio.NewReader(os.Stdin)
	var s string

	for {
		fmt.Fprintf(os.Stderr, "%s (%s) ", label, choices)
		s, _ = r.ReadString('\n')
		fmt.Fprintln(os.Stderr)

		s = strings.TrimSpace(s)
		if s == "" {
			return true
		}
		s = strings.ToLower(s)
		if s == "y" || s == "yes" {
			return true
		}
		if s == "n" || s == "no" {
			return false
		}
	}
}
