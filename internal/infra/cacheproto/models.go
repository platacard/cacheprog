package cacheproto

import (
	"io"
	"time"
)

// Cmd is a command that can be issued to a child process.
//
// If the interface needs to grow, we can add new commands or new versioned
// commands like "get2".
type Cmd string

const (
	CmdGet   = Cmd("get")
	CmdPut   = Cmd("put")
	CmdClose = Cmd("close") // handled by server, not passed to handler
)

// Request is the JSON-encoded message that's sent from cmd/go to
// the GOCACHEPROG child process over stdin. Each JSON object is on its
// own line. A Request of Type "put" with BodySize > 0 will be followed
// by a line containing a base64-encoded JSON string literal of the body.
type Request struct {
	// ID is a unique number per process across all requests.
	// It must be echoed in the ProgResponse from the child.
	ID int64
	// Command is the type of request.
	// The cmd/go tool will only send commands that were declared
	// as supported by the child.
	Command Cmd
	// ActionID is non-nil for get and puts.
	ActionID []byte `json:",omitempty"` // or nil if not used
	// OutputID is set for Type "put".
	OutputID []byte `json:",omitempty"` // or nil if not used
	// ObjectID is legacy field for "put" command, some older implementations used it instead of OutputID (like old versions of golangci-lint).
	ObjectID []byte `json:",omitempty"` // or nil if not used
	// Body is the body for "put" requests. It's sent after the JSON object
	// as a base64-encoded JSON string when BodySize is non-zero.
	// It's sent as a separate JSON value instead of being a struct field
	// send in this JSON object so large values can be streamed in both directions.
	// The base64 string body of a ProgRequest will always be written
	// immediately after the JSON object and a newline.
	Body io.Reader `json:"-"`
	// BodySize is the number of bytes of Body. If zero, the body isn't written.
	BodySize int64 `json:",omitempty"`
}

// Response is the JSON response from the child process to cmd/go.
//
// Except the first protocol message that the child writes to its
// stdout with ID==0 and KnownCommands populated, these are only sent in
// response to a Request from cmd/go.
//
// Responses can be sent in any order. The ID must match the request they're
// replying to.
type Response struct {
	ID  int64  // that corresponds to ProgRequest; they can be answered out of order
	Err string `json:",omitempty"` // if non-empty, the error
	// KnownCommands is included in the first message that cache helper program
	// writes to stdout on startup (with ID==0). It includes the
	// ProgRequest.Command types that are supported by the program.
	//
	// This lets us extend the protocol gracefully over time (adding "get2",
	// etc), or fail gracefully when needed. It also lets us verify the program
	// wants to be a cache helper.
	KnownCommands []Cmd `json:",omitempty"`
	// For Get requests.
	Miss     bool       `json:",omitempty"` // cache miss
	OutputID []byte     `json:",omitempty"`
	Size     int64      `json:",omitempty"` // in bytes
	Time     *time.Time `json:",omitempty"` // an Entry.Time; when the object was added to the docs
	// DiskPath is the absolute path on disk of the OutputID corresponding
	// a "get" request's ActionID (on cache hit) or a "put" request's
	// provided OutputID.
	DiskPath string `json:",omitempty"`
}
