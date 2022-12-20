.PHONY: install-tools
## install-tools: installs dependencies for tools
install-tools:
	@echo Installing tools from tools.go
	@cat tools.go | grep _ | awk -F'"' '{print $$2}' | xargs -tI % go install %

.PHONY: format
format: install-tools
	goimports -local github.com/proost -w $(shell find . -type f -name '*.go')
	gofmt -s -w .
	go mod tidy

.PHONY: unit-test
unit-test: install-tools
	gotest -p 60 -race -coverpkg ./... -coverprofile=coverage.out -v ./...

.PHONY: test
## test: runs tests
test: install-tools unit-test

.PHONY: lint
lint: install-tools
	golangci-lint run ./...
	go mod verify
