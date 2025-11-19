package cacheproto

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader_ReadRequest(t *testing.T) {
	t.Run("get request", func(t *testing.T) {
		input := `{"ID":1,"Command":"get","ActionID":"YWJjMTIz"}` + "\n\n"
		reader := NewReader(bytes.NewBufferString(input))

		req, err := reader.ReadRequest()
		require.NoError(t, err)

		assert.Equal(t, int64(1), req.ID)
		assert.Equal(t, CmdGet, req.Command)
		assert.Equal(t, []byte("abc123"), req.ActionID)
		assert.Nil(t, req.Body)
		assert.Zero(t, req.BodySize)
	})

	t.Run("put request with body", func(t *testing.T) {
		body := []byte("hello world")
		bodyBase64 := base64.StdEncoding.EncodeToString(body)

		req := Request{
			ID:       2,
			Command:  CmdPut,
			ActionID: []byte("def456"),
			BodySize: int64(len(body)),
		}
		reqJSON, err := json.Marshal(req)
		require.NoError(t, err)

		input := string(reqJSON) + "\n\n\"" + bodyBase64 + "\"\n"
		reader := NewReader(bytes.NewBufferString(input))

		gotReq, err := reader.ReadRequest()
		require.NoError(t, err)

		assert.Equal(t, int64(2), gotReq.ID)
		assert.Equal(t, CmdPut, gotReq.Command)
		assert.Equal(t, []byte("def456"), gotReq.ActionID)
		assert.Equal(t, int64(len(body)), gotReq.BodySize)

		// Read and verify body
		gotBody, err := io.ReadAll(gotReq.Body)
		require.NoError(t, err)
		assert.Equal(t, body, gotBody)
	})

	t.Run("put request with empty body", func(t *testing.T) {
		input := `{"ID":3,"Command":"put","ActionID":"Z2hpNzg5","BodySize":0}` + "\n\n"
		reader := NewReader(bytes.NewBufferString(input))

		req, err := reader.ReadRequest()
		require.NoError(t, err)

		assert.Equal(t, int64(3), req.ID)
		assert.Equal(t, CmdPut, req.Command)
		assert.Equal(t, []byte("ghi789"), req.ActionID)
		assert.Equal(t, int64(0), req.BodySize)

		// Verify empty body
		gotBody, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Empty(t, gotBody)
	})

	t.Run("error cases", func(t *testing.T) {
		tests := []struct {
			name  string
			input string
			want  string
		}{
			{
				name:  "invalid json",
				input: "invalid json\n\n",
				want:  "unmarshal request:",
			},
			{
				name:  "missing newline after json",
				input: `{"ID":1,"Command":"get"}`,
				want:  "read line:",
			},
			{
				name:  "missing empty line",
				input: `{"ID":1,"Command":"get"}` + "\n",
				want:  "read empty line:",
			},
			{
				name:  "invalid body start for put",
				input: `{"ID":1,"Command":"put","BodySize":1}` + "\n\nX",
				want:  "unexpected body begin:",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reader := NewReader(bytes.NewBufferString(tt.input))
				_, err := reader.ReadRequest()
				assert.ErrorContains(t, err, tt.want)
			})
		}
	})
}

func FuzzReader_ReadRequest(f *testing.F) {
	// Add initial corpus
	f.Add([]byte(`{"ID":1,"Command":"get","ActionID":"abc123"}`))
	f.Add([]byte(`{"ID":2,"Command":"put","ActionID":"def456","BodySize":11}`))
	f.Add([]byte(`{"ID":3,"Command":"close"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip empty input
		if len(data) == 0 {
			return
		}

		// Create input with proper line endings
		input := string(data) + "\n\n"

		// If it looks like a put request with body, try to add a valid body
		if bytes.Contains(data, []byte(`"Command":"put"`)) && bytes.Contains(data, []byte(`"BodySize"`)) {
			// Add a base64-encoded body
			body := []byte("test body")
			bodyBase64 := base64.StdEncoding.EncodeToString(body)
			input = input + "\"" + bodyBase64 + "\"\n"
		}

		reader := NewReader(bytes.NewBufferString(input))
		req, err := reader.ReadRequest()

		// If parsing succeeds, verify basic invariants
		if err == nil {
			// Verify ID is not negative
			if req.ID < 0 {
				t.Errorf("negative ID: %d", req.ID)
			}

			// Verify Command is one of the valid commands
			validCmd := req.Command == CmdGet || req.Command == CmdPut || req.Command == CmdClose
			if !validCmd {
				t.Errorf("invalid command: %s", req.Command)
			}

			// For put requests with body, verify we can read the body
			if req.Command == CmdPut && req.BodySize > 0 {
				if req.Body == nil {
					t.Error("body is nil for put request with BodySize > 0")
				} else {
					body, err := io.ReadAll(req.Body)
					if err != nil {
						t.Errorf("failed to read body: %v", err)
					}
					if int64(len(body)) != req.BodySize {
						t.Errorf("body size mismatch: got %d, want %d", len(body), req.BodySize)
					}
				}
			}
		}
	})
}

// Add a more targeted fuzzer for base64-encoded bodies
func FuzzReader_ReadRequestBody(f *testing.F) {
	// Add initial corpus
	f.Add([]byte("hello world"), int64(11))
	f.Add([]byte{}, int64(0))
	f.Add([]byte{0xFF, 0x00, 0xFF}, int64(3))

	f.Fuzz(func(t *testing.T, body []byte, size int64) {
		// Skip invalid size
		if size < 0 || size != int64(len(body)) {
			return
		}

		// Create a valid put request with the fuzzed body
		req := Request{
			ID:       1,
			Command:  CmdPut,
			ActionID: []byte("test"),
			BodySize: size,
		}
		reqJSON, err := json.Marshal(req)
		if err != nil {
			t.Skip()
		}

		// Encode body in base64
		bodyBase64 := base64.StdEncoding.EncodeToString(body)
		input := string(reqJSON) + "\n\n\"" + bodyBase64 + "\"\n"

		reader := NewReader(bytes.NewBufferString(input))
		gotReq, err := reader.ReadRequest()
		if err != nil {
			return // It's okay if some inputs are rejected
		}

		// Verify the body was correctly read
		gotBody, err := io.ReadAll(gotReq.Body)
		if err != nil {
			t.Errorf("failed to read body: %v", err)
		}
		if !bytes.Equal(gotBody, body) {
			t.Errorf("body mismatch: got %v, want %v", gotBody, body)
		}
		if int64(len(gotBody)) != size {
			t.Errorf("body size mismatch: got %d, want %d", len(gotBody), size)
		}
	})
}
