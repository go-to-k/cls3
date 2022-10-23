package main

import (
	"context"
	"os"
	"runtime/debug"

	"github.com/go-to-k/cls3"
)

func main() {
	cls3.NewLogger(isDebug())
	ctx := context.TODO()
	app := cls3.NewApp(getVersion())

	if err := app.Run(ctx); err != nil {
		cls3.Logger.Error().Msg(err.Error())
		os.Exit(1)
	}
}

func isDebug() bool {
	if cls3.Version == "" || cls3.Revision != "" {
		return true
	}
	return false
}

func getVersion() string {
	if cls3.Version != "" && cls3.Revision != "" {
		return cls3.Version + "-" + cls3.Revision
	}
	if cls3.Version != "" {
		return cls3.Version
	}

	i, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return i.Main.Version
}
