install:
	go install ./cmd/blobcache

# Build the blobcache binary for the current platform.
build:
	mkdir -p ./build/out
	go build -o ./build/out/blobcache ./cmd/blobcache 

# Build the blobcache binary for amd64 linux.
build-amd64-linux:
	mkdir -p ./build/out
	GOOS=linux GOARCH=amd64 go build -o ./build/out/blobcache_amd64-linux ./cmd/blobcache 

test:
	go test ./...

testv:
	go test -count=1 -v ./pkg/...

protobuf:
	cd ./src/tries && ./build.sh
	cd ./pkg/bcgrpc && ./build.sh

docker-build: build-amd64-linux
	podman build --tag blobcache .
