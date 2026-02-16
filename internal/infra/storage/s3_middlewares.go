package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"net/http"

	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func addPropagateSHA256Middleware(stack *middleware.Stack, sha256Sum []byte) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc(
		"PropagateSHA256Middleware",
		func(ctx context.Context, input middleware.InitializeInput, next middleware.InitializeHandler) (
			out middleware.InitializeOutput, metadata middleware.Metadata, err error,
		) {
			return next.HandleInitialize(signer.SetPayloadHash(ctx, hex.EncodeToString(sha256Sum)), input)
		},
	), middleware.Before)
}

func addIgnoreHeadersSigningMiddleware(stack *middleware.Stack, headers []string) error {
	if len(headers) == 0 {
		return nil
	}

	// avoid header string transformation in middleware on each request
	canonicalHeaders := make([]string, 0, len(headers))
	for _, header := range headers {
		canonicalHeaders = append(canonicalHeaders, http.CanonicalHeaderKey(header))
	}

	err := stack.Finalize.Insert(hideHeadersBeforeSigning(canonicalHeaders), "Signing", middleware.Before)
	if err != nil {
		return fmt.Errorf("insert hideHeadersBeforeSigning: %w", err)
	}

	err = stack.Finalize.Insert(restoreHeadersAfterSigning(), "Signing", middleware.After)
	if err != nil {
		return fmt.Errorf("insert restoreHeadersAfterSigning: %w", err)
	}

	return nil
}

type ignoredHeadersKey struct{}

func hideHeadersBeforeSigning(canonicalHeaders []string) middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"HideHeadersBeforeSigning",
		func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (
			out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
		) {
			req, ok := input.Request.(*smithyhttp.Request)
			if !ok {
				return out, metadata, &signer.SigningError{
					Err: fmt.Errorf("(hideHeaders) invalid request type: %T", input.Request),
				}
			}

			ignoredHeaders := make(http.Header, len(canonicalHeaders))
			for _, header := range canonicalHeaders {
				ignoredHeaders[header] = req.Header[header]
				delete(req.Header, header)
			}

			ctx = middleware.WithStackValue(ctx, ignoredHeadersKey{}, ignoredHeaders)

			return next.HandleFinalize(ctx, input)
		},
	)
}

func restoreHeadersAfterSigning() middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"RestoreHeadersAfterSigning",
		func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (
			out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
		) {
			req, ok := input.Request.(*smithyhttp.Request)
			if !ok {
				return out, metadata, &signer.SigningError{
					Err: fmt.Errorf("(restoreHeaders) invalid request type: %T", input.Request),
				}
			}

			ignoredHeaders, ok := middleware.GetStackValue(ctx, ignoredHeadersKey{}).(http.Header)
			if ok {
				maps.Copy(req.Header, ignoredHeaders)
			}

			return next.HandleFinalize(ctx, input)
		},
	)
}
