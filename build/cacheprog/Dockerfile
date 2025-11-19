ARG GO_IMAGE=golang:1.25-alpine

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS builder

ARG CACHEPROG_VERSION=unknown

WORKDIR /app

ENV \
    GOCACHE=/root/.cache/go-build \
    GOMODCACHE=/root/.cache/go-mod \
    CGO_ENABLED=0 \
    GOOS=$TARGETOS \
    GOARCH=$TARGETARCH

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/.cache/go-mod \
    --mount=type=bind,source=.,target=/app \
    go build -ldflags "-s -w -X 'main.Version=${CACHEPROG_VERSION}'" -trimpath -o /cacheprog ./cmd/cacheprog

FROM scratch AS runner

COPY --from=builder /cacheprog /cacheprog

ENTRYPOINT ["/cacheprog"]
