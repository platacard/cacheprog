E2E tests for cacheprog
========

To run tests you need to have docker or any compatible container runtime on your machine.

To run tests:

```bash
go test -count=1 ./...
```

`-count=1` flag is used to disable caching of test results.

`-short` flag also supported. If specified, tests will be run without starting docker compose stack.

To run tests with race detector and coverage:

```bash
GOCOVERDIR="<coverage_directory>" go test -race -count=1 -covermode=atomic ./...
```
And then run following command to get text coverage report:

```bash
go tool covdata textfmt -i=<coverage_directory> -o=<coverage_report_file>
```

## Running in Docker

This may help if you're running tests on platform not fully supported by `github.com/rogpeppe/go-internal/gotooltest` i.e. `darwin/arm64`.

Simple one-shot run of full test suite may be launched from project root like this:
```bash
docker run --rm \
    -v $(pwd):$(pwd):ro -w $(pwd)/functests \
    -v /var/run/docker.sock:/var/run/docker.sock \
    --net=host \
    -e TESTCONTAINERS_RYUK_DISABLED=true \
    golang:1.26-alpine \
    sh -c "apk add --no-cache build-base && go test -count=1 ./..."
```

However it doesn't preserve caches or installed packages. So if you want to run test a lot of times during debugging session use following approach:
1. Start a container with prepared environment
```bash
docker run --rm -it \
    -v $(pwd):$(pwd):ro -w $(pwd)/functests \
    -v /var/run/docker.sock:/var/run/docker.sock \
    --net=host \
    -e TESTCONTAINERS_RYUK_DISABLED=true \
    golang:1.26-alpine \
    sh
```
2. Install deps to ensure that CGO-involving tests will be launched
```bash
apk add --no-cache build-base
```
3. Run tests as usual
```bash
go test -count=1 ./...
```
Unless you're exited from session tests may be run multiple times.
