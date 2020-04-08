
.PHONY: test

test:
	go test --race ./pkg/...
