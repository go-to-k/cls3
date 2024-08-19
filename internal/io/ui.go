package io

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fatih/color"
)

type UI struct {
	Choices   []string
	Headers   []string
	Cursor    int
	Selected  map[int]struct{}
	Filtered  *Filtered
	Keyword   string
	isEntered bool
}

type Filtered struct {
	Choices map[int]struct{}
	Prev    *Filtered
	Cursor  int
}

var _ tea.Model = (*UI)(nil)

func NewUI(choices []string, headers []string) *UI {
	filtered := make(map[int]struct{})
	for i := range choices {
		filtered[i] = struct{}{}
	}

	return &UI{
		Choices:  choices,
		Headers:  headers,
		Selected: make(map[int]struct{}),
		Filtered: &Filtered{Choices: filtered},
	}
}

func (u *UI) Init() tea.Cmd {
	return nil
}

func (u *UI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	//nolint:gocritic
	switch msg := msg.(type) {

	case tea.KeyMsg:

		switch msg.String() {

		// Quit the selection
		case "enter":
			u.isEntered = true
			return u, tea.Quit

		// Quit the selection and clear ALL selected items
		case "ctrl+c":
			u.Selected = make(map[int]struct{})
			return u, tea.Quit

		case "up":
			if len(u.Filtered.Choices) < 2 {
				return u, nil
			}
			for range u.Choices {
				if u.Cursor == 0 {
					u.Cursor = len(u.Choices) - 1
				} else if u.Cursor > 0 {
					u.Cursor--
				}

				if _, ok := u.Filtered.Choices[u.Cursor]; ok {
					if u.Filtered.Cursor == 0 {
						u.Filtered.Cursor = len(u.Filtered.Choices) - 1
					} else if u.Filtered.Cursor > 0 {
						u.Filtered.Cursor--
					}

					f := u.Filtered
					for {
						if f.Prev == nil {
							break
						}
						f.Prev.Cursor = u.Filtered.Cursor
						f = f.Prev
					}
					break
				}
			}

		case "down":
			if len(u.Filtered.Choices) < 2 {
				return u, nil
			}
			for range u.Choices {
				if u.Cursor < len(u.Choices)-1 {
					u.Cursor++
				} else if u.Cursor == len(u.Choices)-1 {
					u.Cursor = 0
				}

				if _, ok := u.Filtered.Choices[u.Cursor]; ok {
					if u.Filtered.Cursor < len(u.Filtered.Choices)-1 {
						u.Filtered.Cursor++
					} else if u.Filtered.Cursor == len(u.Filtered.Choices)-1 {
						u.Filtered.Cursor = 0
					}

					f := u.Filtered
					for {
						if f.Prev == nil {
							break
						}
						f.Prev.Cursor = u.Filtered.Cursor
						f = f.Prev
					}
					break
				}
			}

		// select or deselect an item
		case " ":
			_, ok := u.Selected[u.Cursor]
			if ok {
				delete(u.Selected, u.Cursor)
			} else {
				u.Selected[u.Cursor] = struct{}{}
			}

		// select all items in filtered list
		case "right":
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
		case "left":
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
		case "backspace":
			if len(u.Keyword) > 0 {
				u.Keyword = u.Keyword[:len(u.Keyword)-1]
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

		// add a character to the keyword
		default:
			u.Keyword += msg.String()
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

				if contains {
					u.Filtered.Choices[i] = struct{}{}
					tmpCursor = i
				} else if u.Cursor == i && u.Cursor < len(u.Choices)-1 {
					u.Cursor++
				} else if u.Cursor == i {
					u.Cursor = tmpCursor
				}
			}

			if len(u.Filtered.Choices) != 0 {
				f := u.Filtered
				for {
					if f.Prev == nil {
						break
					}
					f.Prev.Cursor = u.Filtered.Cursor
					f = f.Prev
				}
			}

		}
	}

	return u, nil
}

func (u *UI) View() string {
	bold := color.New(color.Bold)

	s := color.CyanString("? ")

	for _, header := range u.Headers {
		s += bold.Sprintln(header)
	}

	if u.isEntered {
		return s
	}

	s += u.Keyword

	s += color.CyanString(" [Use arrows to move, space to select, <right> to all, <left> to none, type to filter]")
	s += "\n"

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

		s += fmt.Sprintf("%s %s %s\n", cursor, checked, choice)
	}

	return s
}
