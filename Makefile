# .PHONY is "a fake target" & don't check any file versions and just run given action
# Also keep in mind that "make" without params runs the 1st action
.PHONY: test lint check format-check etcd f1

test:
	go test ./...

lint:
	golangci-lint run

format-check:
	test -z "$$(gofumpt -l .)"

check: test lint format-check

# All sorts of commands that might be useful

etcd:
	etcd

f1:
	go run ./cmd/limiter run constant tests --rate 6000000/s  --concurrency 256

prometheus:
	prometheus_brew_services
