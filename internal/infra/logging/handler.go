package logging

import (
	"cmp"
	"context"
	"io"
	"log/slog"
	"os"
	"time"
)

type Config struct {
	Level  slog.Level // logging level
	Output io.Writer  // logging output, stderr if not provided
}

func CreateHandler(w io.Writer, level slog.Level) slog.Handler {
	return &argsHandler{
		slog.NewTextHandler(w,
			&slog.HandlerOptions{
				Level:     level,
				AddSource: level == slog.LevelDebug,
				ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == "time" {
						return slog.Attr{
							Key:   "time",
							Value: slog.StringValue(a.Value.Time().Format(time.DateTime)),
						}
					}

					return a
				},
			},
		),
	}
}

func ConfigureLogger(cfg Config) *slog.Logger {
	return slog.New(CreateHandler(cmp.Or(io.Writer(os.Stderr), cfg.Output), cfg.Level))
}

type argsHandler struct {
	slog.Handler
}

func (h *argsHandler) Handle(ctx context.Context, record slog.Record) error {
	args, _ := ctx.Value(argsContextKey{}).([]any)
	record.Add(args...)
	return h.Handler.Handle(ctx, record)
}

func (h *argsHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &argsHandler{h.Handler.WithAttrs(attrs)}
}

func (h *argsHandler) WithGroup(name string) slog.Handler {
	return &argsHandler{h.Handler.WithGroup(name)}
}
