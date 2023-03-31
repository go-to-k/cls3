package io

import (
	"os"

	"github.com/rs/zerolog"
)

var Logger *zerolog.Logger

func NewLogger(isDebug bool) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if isDebug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	consoleWriter := zerolog.ConsoleWriter{
		Out: os.Stderr,
		PartsExclude: []string{
			zerolog.TimestampFieldName,
		},
	}

	l := zerolog.New(&consoleWriter)

	Logger = &l
}
