.PHONY: test testv protobuf drop-replace add-replace

install:
	go install ./cmd/blobcache

build:
	mkdir -p ./out
	go build -o ./out/blobcache ./cmd/blobcache 

test:
	go test --race ./pkg/...

testv:
	go test --race -count=1 -v ./pkg/...

protobuf:
	cd ./pkg/tries && ./build.sh
	cd ./pkg/bcgrpc && ./build.sh

drop-replace:
	go mod edit -dropreplace github.com/inet256/inet256

add-replace:
	go mod edit -replace github.com/inet256/inet256=../../inet256/inet256

