package io

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fatih/color"
)

const SelectionPageSize = 20

type UI struct {
	Choices    []string
	Headers    []string
	Cursor     int
	Selected   map[int]struct{}
	Filtered   *Filtered
	Keyword    string
	IsEntered  bool
	IsCanceled bool
}

type Filtered struct {
	Choices map[int]struct{}
	Prev    *Filtered
	Cursor  int
}

var _ tea.Model = (*UI)(nil)

func NewUI(choices []string, headers []string) *UI {
	return &UI{
		Choices:  choices,
		Headers:  headers,
		Selected: make(map[int]struct{}),
	}
}

func (u *UI) Init() tea.Cmd {
	filtered := make(map[int]struct{})
	for i := range u.Choices {
		filtered[i] = struct{}{}
	}
	u.Filtered = &Filtered{Choices: filtered}

	return nil
}

func (u *UI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	//nolint:gocritic
	switch msg := msg.(type) {

	case tea.KeyMsg:

		switch msg.Type {

		// Quit the selection
		case tea.KeyEnter:
			u.IsEntered = true
			return u, tea.Quit

		// Quit the selection
		case tea.KeyCtrlC:
			u.IsCanceled = true
			return u, tea.Quit

		case tea.KeyUp, tea.KeyShiftTab:
			if len(u.Filtered.Choices) < 2 {
				return u, nil
			}
			for range u.Choices {
				if u.Cursor == 0 {
					u.Cursor = len(u.Choices) - 1
				} else if u.Cursor > 0 {
					u.Cursor--
				}

				if _, ok := u.Filtered.Choices[u.Cursor]; !ok {
					continue
				}
				if u.Filtered.Cursor == 0 {
					u.Filtered.Cursor = len(u.Filtered.Choices) - 1
				} else if u.Filtered.Cursor > 0 {
					u.Filtered.Cursor--
				}

				f := u.Filtered
				for f.Prev != nil {
					f.Prev.Cursor = u.Filtered.Cursor
					f = f.Prev
				}
				break
			}

		case tea.KeyDown, tea.KeyTab:
			if len(u.Filtered.Choices) < 2 {
				return u, nil
			}
			for range u.Choices {
				if u.Cursor < len(u.Choices)-1 {
					u.Cursor++
				} else if u.Cursor == len(u.Choices)-1 {
					u.Cursor = 0
				}

				if _, ok := u.Filtered.Choices[u.Cursor]; !ok {
					continue
				}
				if u.Filtered.Cursor < len(u.Filtered.Choices)-1 {
					u.Filtered.Cursor++
				} else if u.Filtered.Cursor == len(u.Filtered.Choices)-1 {
					u.Filtered.Cursor = 0
				}

				f := u.Filtered
				for f.Prev != nil {
					f.Prev.Cursor = u.Filtered.Cursor
					f = f.Prev
				}
				break
			}

		// select or deselect an item
		case tea.KeySpace:
			if _, ok := u.Filtered.Choices[u.Cursor]; !ok {
				return u, nil
			}
			_, ok := u.Selected[u.Cursor]
			if ok {
				delete(u.Selected, u.Cursor)
			} else {
				u.Selected[u.Cursor] = struct{}{}
			}

		// select all items in filtered list
		case tea.KeyRight:
			for i := range u.Choices {
				if _, ok := u.Filtered.Choices[i]; !ok {
					continue
				}
				_, ok := u.Selected[i]
				if !ok {
					u.Selected[i] = struct{}{}
				}
			}

		// clear all selected items in filtered list
		case tea.KeyLeft:
			for i := range u.Choices {
				if _, ok := u.Filtered.Choices[i]; !ok {
					continue
				}
				_, ok := u.Selected[i]
				if ok {
					delete(u.Selected, i)
				}
			}

		// clear one character from the keyword
		case tea.KeyBackspace:
			u.backspace()

		// clear the keyword
		case tea.KeyCtrlW:
			for u.Keyword != "" {
				u.backspace()
			}

		// add a character to the keyword
		case tea.KeyRunes:
			str := msg.String()
			if !msg.Paste {
				for _, r := range str {
					u.addCharacter(string(r))
				}
			} else {
				if strings.Contains(str, string('\n')) || strings.Contains(str, string('\r')) {
					u.IsEntered = true
					return u, tea.Quit
				}

				runes := []rune(str)
				for i, r := range runes {
					// characters by paste key are enclosed by '[' and ']'
					if i == 0 || i == len(runes)-1 {
						continue
					}
					if r != ' ' && r != '\t' {
						u.addCharacter(string(r))
					}
				}
			}

		}
	}

	return u, nil
}

func (u *UI) backspace() {
	if len(u.Keyword) == 0 {
		return
	}

	keywordRunes := []rune(u.Keyword)
	keywordRunes = keywordRunes[:len(keywordRunes)-1]
	u.Keyword = string(keywordRunes)
	u.Filtered = u.Filtered.Prev
	cnt := 0
	for i := range u.Choices {
		if _, ok := u.Filtered.Choices[i]; !ok {
			continue
		}
		if cnt == u.Filtered.Cursor {
			u.Cursor = i
			break
		}
		cnt++
	}
}

func (u *UI) addCharacter(c string) {
	u.Keyword += c
	u.Filtered = &Filtered{
		Choices: make(map[int]struct{}),
		Prev:    u.Filtered,
	}

	tmpCursor := u.Cursor
	for i, choice := range u.Choices {
		lk := strings.ToLower(u.Keyword)
		lc := strings.ToLower(choice)
		contains := strings.Contains(lc, lk)

		fLen := len(u.Filtered.Choices)
		if contains && fLen != 0 && fLen <= u.Filtered.Prev.Cursor {
			u.Filtered.Cursor++
			u.Cursor = i
		}

		switch {
		case contains:
			u.Filtered.Choices[i] = struct{}{}
			tmpCursor = i
		case u.Cursor == i && u.Cursor < len(u.Choices)-1:
			u.Cursor++
		case u.Cursor == i:
			u.Cursor = tmpCursor
		}
	}

	if len(u.Filtered.Choices) == 0 {
		return
	}
	f := u.Filtered
	for f.Prev != nil {
		f.Prev.Cursor = u.Filtered.Cursor
		f = f.Prev
	}
}

func (u *UI) View() string {
	bold := color.New(color.Bold)

	s := color.CyanString("? ")

	for _, header := range u.Headers {
		s += bold.Sprintln(header)
	}

	if u.IsEntered && len(u.Selected) != 0 {
		return s
	}

	s += bold.Sprintln(u.Keyword)

	s += color.CyanString(" [Use arrows to move, space to select, <right> to all, <left> to none, type to filter]")
	s += "\n"

	var contents []string
	for i, choice := range u.Choices {
		if _, ok := u.Filtered.Choices[i]; !ok {
			continue
		}

		cursor := " " // no cursor
		if u.Cursor == i {
			cursor = color.CyanString(bold.Sprint(">")) // cursor!
		}

		checked := bold.Sprint("[ ]") // not selected
		if _, ok := u.Selected[i]; ok {
			checked = color.GreenString("[x]") // selected!
		}

		contents = append(contents, fmt.Sprintf("%s %s %s\n", cursor, checked, choice))
	}

	if len(contents) > SelectionPageSize {
		switch {
		case u.Filtered.Cursor < SelectionPageSize/2:
			contents = contents[:SelectionPageSize]
		case u.Filtered.Cursor > len(contents)-SelectionPageSize/2:
			contents = contents[len(contents)-SelectionPageSize:]
		default:
			contents = contents[u.Filtered.Cursor-SelectionPageSize/2 : u.Filtered.Cursor+SelectionPageSize/2]
		}
	}

	s += strings.Join(contents, "")
	return s
}
