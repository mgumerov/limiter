GOPATH := $(shell go env GOPATH)

# .PHONY is "a fake target" & don't check any file versions and just run given action
# Also keep in mind that "make" without params runs the 1st action
.PHONY: test lint check format-check etcd f1 install_linters

test:
	go test ./...

lint:
# Note: unlike golangci, when used as standalone it by default does not check many opinionated checks like SA9003 or ST1023
# I fully support it actually. But if I decide to turn those checks on, at least with staticcheck I have a way 
# to selectively turn them off case by case via comments, which golangci cannot do. Sadly, for SA9003 it looks really ugly.
	$(GOPATH)/bin/staticcheck ./...

	$(GOPATH)/bin/errcheck ./...

	$(GOPATH)/bin/ineffassign ./...

format-check:
	test -z "$$($(GOPATH)/bin/gofumpt -l .)"

check: test lint #I decided against format-check for now

install_linters:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/kisielk/errcheck@latest
	go install github.com/gordonklaus/ineffassign@latest
	go install mvdan.cc/gofumpt@latest

# All sorts of commands that might be useful

etcd:
	etcd

f1:
	go run ./cmd/limiter run constant tests --rate 6000000/s  --concurrency 256

prometheus:
	prometheus_brew_services
