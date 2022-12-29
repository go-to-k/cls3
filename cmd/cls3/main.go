package main

import (
	"context"
	"os"

	"github.com/go-to-k/cls3"
)

func main() {
	cls3.NewLogger(cls3.IsDebug())
	ctx := context.Background()
	app := cls3.NewApp(cls3.GetVersion())

	if err := app.Run(ctx); err != nil {
		cls3.Logger.Error().Msg(err.Error())
		os.Exit(1)
	}
}
