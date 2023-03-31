package main

import (
	"context"
	"os"

	"github.com/go-to-k/cls3/internal/app"
	"github.com/go-to-k/cls3/internal/io"
	"github.com/go-to-k/cls3/internal/version"
)

func main() {
	io.NewLogger(version.IsDebug())
	ctx := context.Background()
	app := app.NewApp(version.GetVersion())

	if err := app.Run(ctx); err != nil {
		io.Logger.Error().Msg(err.Error())
		os.Exit(1)
	}
}
