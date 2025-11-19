package cacheproto

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		br: bufio.NewReader(r),
	}
}

func (r *Reader) ReadRequest() (*Request, error) {
	// first line - json request object

	line, err := r.br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read line: %w", err)
	}

	var request Request
	if err = json.Unmarshal(line, &request); err != nil {
		return nil, fmt.Errorf("unmarshal request: %w", err)
	}

	// read empty line
	if _, err = r.br.ReadSlice('\n'); err != nil {
		return nil, fmt.Errorf("read empty line: %w", err)
	}

	if request.Command != CmdPut {
		return &request, nil
	}

	// for "put" command - read body, base64 encoded json string
	// pick next character - must be '"' if body is present
	if request.BodySize == 0 {
		request.Body = bytes.NewReader(nil)
		return &request, nil
	}

	bodyBegin, err := r.br.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read body begin: %w", err)
	}

	if bodyBegin != '"' {
		return nil, fmt.Errorf("unexpected body begin: %q", bodyBegin)
	}

	request.Body = newBodyReader(r.br, request.BodySize)

	return &request, nil
}

type bodyReader struct {
	br      *bufio.Reader
	decoder io.Reader

	atEOF bool
}

func newBodyReader(br *bufio.Reader, size int64) *bodyReader {
	encoding := base64.StdEncoding
	return &bodyReader{
		br: br,
		decoder: base64.NewDecoder(
			encoding,
			&io.LimitedReader{
				R: br,
				N: int64(encoding.EncodedLen(int(size))), // prevent buffering in decoder since we know the size
			},
		),
	}
}

var bodyEnd = [...]byte{'"', '\n'}

func (b *bodyReader) Read(p []byte) (n int, err error) {
	if b.atEOF {
		return 0, io.EOF
	}

	n, err = b.decoder.Read(p)
	if !errors.Is(err, io.EOF) {
		return n, err
	}

	b.atEOF = true

	// validate that the body ends with '"' and '\n'
	// read the rest of the body

	var endData [len(bodyEnd)]byte
	if _, err = io.ReadFull(b.br, endData[:]); err != nil {
		return n, fmt.Errorf("read body end: %w", err)
	}

	if endData != bodyEnd {
		return n, fmt.Errorf("unexpected body end: %q", endData)
	}

	return n, nil
}
