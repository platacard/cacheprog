package logging

import (
	"cmp"
	"context"
	"log/slog"

	"github.com/aws/smithy-go/logging"
)

type SmithyLogger struct {
	*slog.Logger

	ctx context.Context
}

func (s *SmithyLogger) Logf(classification logging.Classification, format string, v ...any) {
	level := slog.LevelInfo
	switch classification {
	case logging.Debug:
		level = slog.LevelDebug
	case logging.Warn:
		level = slog.LevelWarn
	}

	s.Log(cmp.Or(s.ctx, context.Background()), level, format, v...)
}

func (s *SmithyLogger) WithContext(ctx context.Context) logging.Logger {
	return &SmithyLogger{
		Logger: s.Logger,
		ctx:    ctx,
	}
}
