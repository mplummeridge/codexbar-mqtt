SHELL := /bin/bash
VERSION ?= 0.2.0-dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
MODULE := github.com/mplummeridge/codexbar-mqtt
LDFLAGS := -s -w -X $(MODULE)/internal/version.Version=$(VERSION) -X $(MODULE)/internal/version.Commit=$(COMMIT) -X $(MODULE)/internal/version.Date=$(DATE)

.PHONY: fmt-check test vet verify build release clean

fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "gofmt required:"; gofmt -l .; exit 1; }

test:
	go test ./...

vet:
	go vet ./...

verify: fmt-check test vet

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/codexbar-mqtt ./cmd/codexbar-mqtt

release: verify
	rm -rf dist && mkdir -p dist
	for arch in arm64 amd64; do \
		package="codexbar-mqtt-$(VERSION)-darwin-$$arch"; \
		root="dist/$$package"; \
		mkdir -p "$$root"; \
		CGO_ENABLED=0 GOOS=darwin GOARCH=$$arch go build -trimpath -ldflags '$(LDFLAGS)' -o "$$root/codexbar-mqtt" ./cmd/codexbar-mqtt; \
		cp README.md CHANGELOG.md LICENSE SECURITY.md config.example.json "$$root/"; \
		cp -R docs launchd scripts "$$root/"; \
		tar -C dist -czf "dist/$$package.tar.gz" "$$package"; \
		rm -rf "$$root"; \
	done
	cd dist && if command -v sha256sum >/dev/null; then sha256sum *.tar.gz > SHA256SUMS; else shasum -a 256 *.tar.gz > SHA256SUMS; fi

clean:
	rm -rf bin dist
