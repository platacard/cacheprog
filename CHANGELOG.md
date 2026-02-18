# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The guideline for versioning is as follows:

- If the change is breaking, increase major version (e.g. `v1.3.0` -> `v2.0.0`).
- If the change is a new non-breaking feature, increase minor version (e.g. `v1.2.1` -> `v1.3.0`).
- If the change is not related to new features i.e. bugfixes, dependency upgrades, etc., increase patch version (e.g. `v1.2.0` -> `v1.2.1`).

## [Unreleased]

## [1.1.0] - 2026-02-18

- Added ability to exclude specified http headers from request signing in S3 client. Mainly to support Google Cloud Storage, see [README.md](./README.md#s3-compatible-storage-configuration) for more details.
- Added consecutive error threshold based circuit-breaker for remote storage
- Introduce CHANGELOG.md

## [1.0.3] - 2026-02-12

- Upgrade to Go 1.26 in tests and release process
- Upgrade dependencies

## [1.0.2] - 2026-01-08

- Refactor to use `waitgroup.Go()` across codebase
- Upgrade dependencies

## [1.0.1] - 2025-11-28

- Fixed ignoring of provided `http.Client` in `internal/infra/storage.NewHTTP()`
- Fixed file opening flags for metadata files of disk storage to avoid its' truncation.

## [1.0.0] - 2025-11-24

Initial release.
