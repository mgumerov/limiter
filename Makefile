# .PHONY is "a fake target"
.PHONY: test lint check format-check

test:
	go test ./...

lint:
	golangci-lint run

format-check:
	test -z "$$(gofumpt -l .)"

check: test lint format-check