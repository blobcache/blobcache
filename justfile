
# Build the blobcache binary for the current platform.
build: capnp
	mkdir -p ./build/out
	CGO_ENABLED=0 go build -o ./build/out/blobcache ./cmd/blobcache

# Build the blobcache binary for amd64 linux.
build-amd64-linux:
	mkdir -p ./build/out
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ./build/out/blobcache_amd64-linux ./cmd/blobcache

test:
	go test ./...

testv:
	go test -count=1 -v ./pkg/...

capnp:
	cd ./src/internal/tries/triescnp && ./build.sh

build-images: build-amd64-linux
	./etc/build_images.sh

# Installs just the blobcache binary to /usr/bin/blobcache
install-unix: build
	sudo cp ./build/out/blobcache /usr/bin/blobcache

# Install blobcache with systemd service
install-systemd: build
	./etc/install-systemd.sh
