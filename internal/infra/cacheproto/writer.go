package cacheproto

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
)

type Writer struct {
	enc *json.Encoder
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{enc: json.NewEncoder(w)}
}

func (w *Writer) WriteResponse(resp *Response) error {
	if err := w.enc.Encode(resp); err != nil {
		return fmt.Errorf("encode response: %w", err)
	}
	return nil
}

type noopWriter struct {
	ctx context.Context
}

func (w *noopWriter) WriteResponse(response *Response) error {
	if response.Err != "" {
		slog.ErrorContext(cmp.Or(w.ctx, context.Background()), "Response error", "error", response.Err)
	}
	return nil
}
