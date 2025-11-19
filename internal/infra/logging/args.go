package logging

import (
	"context"
	"encoding/base64"
	"log/slog"
)

func Error(err error) slog.Attr {
	if err == nil {
		return slog.Any("error", nil)
	}
	return slog.String("error", err.Error())
}

type argsContextKey struct{}

func AttachArgs(ctx context.Context, args ...any) context.Context {
	existingArgs, _ := ctx.Value(argsContextKey{}).(*[]any)
	if existingArgs == nil {
		args := append([]any{}, args...)
		return context.WithValue(ctx, argsContextKey{}, &args)
	}

	*existingArgs = append(*existingArgs, args...)
	return ctx
}

type base64Bytes []byte

func (b base64Bytes) LogValue() slog.Value {
	return slog.StringValue(base64.StdEncoding.EncodeToString(b))
}

func Bytes(b []byte) slog.Value {
	return slog.AnyValue(base64Bytes(b))
}
