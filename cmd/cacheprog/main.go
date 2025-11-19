package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/platacard/cacheprog/internal/app"
)

var Version = "development"

func main() {
	sigCtx, sigCancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exitCode := app.RunApp(sigCtx, Version, os.Args[1:]...)
	sigCancel()
	os.Exit(exitCode)
}
