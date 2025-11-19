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
