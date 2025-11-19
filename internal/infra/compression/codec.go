package compression

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/valyala/bytebufferpool"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
)

// zstd compression chosen during tests because it shows nice compression ratio and best decompression speed

const (
	compressionZstd = "zstd"
	compressionNone = ""
)

type Codec struct {
	writerPool *sync.Pool
	readerPool *sync.Pool
	memPool    *bytebufferpool.Pool
}

func NewCodec() *Codec {
	return &Codec{
		writerPool: &sync.Pool{
			New: func() any {
				// concurrency handled by upper layers
				writer, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
				if err != nil {
					panic(err)
				}
				return writer
			},
		},
		readerPool: &sync.Pool{
			New: func() any {
				reader, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
				if err != nil {
					panic(err)
				}
				return reader
			},
		},
		memPool: new(bytebufferpool.Pool),
	}
}

func (c *Codec) Compress(request *cacheprog.CompressRequest) (*cacheprog.CompressResponse, error) {
	if request.Size == 0 {
		// do not even try to compress empty files
		return &cacheprog.CompressResponse{
			Size:      request.Size,
			Body:      newHookableCloseSeeker(request.Body, nil),
			Algorithm: compressionNone,
		}, nil
	}

	// cachable objects are reasonably small to be compressed in memory

	buf := c.memPool.Get()
	buf.Reset()

	writer := c.writerPool.Get().(*zstd.Encoder)
	writer.Reset(buf)
	defer c.writerPool.Put(writer)

	_, err := writer.ReadFrom(request.Body)
	if err != nil {
		return nil, fmt.Errorf("copy body: %w", err)
	}

	if err = writer.Close(); err != nil {
		return nil, fmt.Errorf("finalize writer: %w", err)
	}

	size := int64(buf.Len())

	if size >= request.Size {
		_, err = request.Body.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seek request body to start: %w", err)
		}

		// return original file
		return &cacheprog.CompressResponse{
			Size:      request.Size,
			Body:      newHookableCloseSeeker(request.Body, nil),
			Algorithm: compressionNone,
		}, nil
	}

	return &cacheprog.CompressResponse{
		Size: size,
		// get seekable reader using bytes.NewReader
		Body: newHookableCloseSeeker(bytes.NewReader(buf.B), func() error {
			c.memPool.Put(buf)
			return nil
		}),
		Algorithm: compressionZstd,
	}, nil
}

func (c *Codec) Decompress(request *cacheprog.DecompressRequest) (*cacheprog.DecompressResponse, error) {
	switch request.Algorithm {
	case compressionZstd:
		// pass
	case compressionNone:
		return &cacheprog.DecompressResponse{
			Body: request.Body,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported algorithm %q: %w", request.Algorithm, cacheprog.ErrNotFound)
	}

	// concurrency handled by upper layers
	reader := c.readerPool.Get().(*zstd.Decoder)
	if err := reader.Reset(request.Body); err != nil {
		return nil, fmt.Errorf("reset reader: %w", err)
	}

	return &cacheprog.DecompressResponse{
		Body: &hookableCloseDecoder{d: reader, closeHook: func() error {
			err := request.Body.Close()
			c.readerPool.Put(reader)
			return err
		}},
	}, nil
}

type hookableCloseSeeker struct {
	io.ReadSeeker
	closeHook func() error
}

func (h *hookableCloseSeeker) Close() error {
	if h.closeHook != nil {
		return h.closeHook()
	}
	return nil
}

type hookableCloseSeekerWriterTo struct {
	io.ReadSeeker
	closeHook func() error
}

func (h *hookableCloseSeekerWriterTo) Close() error {
	if h.closeHook != nil {
		return h.closeHook()
	}
	return nil
}

func (h *hookableCloseSeekerWriterTo) WriteTo(w io.Writer) (int64, error) {
	return h.ReadSeeker.(io.WriterTo).WriteTo(w)
}

func newHookableCloseSeeker(r io.ReadSeeker, closeHook func() error) io.ReadSeekCloser {
	if _, ok := r.(io.WriterTo); ok {
		return &hookableCloseSeekerWriterTo{r, closeHook}
	}
	return &hookableCloseSeeker{r, closeHook}
}

type hookableCloseDecoder struct {
	d         *zstd.Decoder
	closeHook func() error
}

func (r *hookableCloseDecoder) Read(p []byte) (int, error) {
	return r.d.Read(p)
}

func (r *hookableCloseDecoder) WriteTo(w io.Writer) (int64, error) {
	return r.d.WriteTo(w)
}

func (r *hookableCloseDecoder) Close() error {
	if r.closeHook != nil {
		return r.closeHook()
	}
	return nil
}
