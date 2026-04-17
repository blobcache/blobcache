
# Build the blobcache binary for the current platform.
build: capnp
	mkdir -p ./build/out
	./build/go_exec.sh ./build/out/blobcache ./cmd/blobcache
	./build/go_exec.sh ./build/out/git-remote-bc ./cmd/git-remote-bc

# Build the blobcache binary for amd64 linux.
build-amd64-linux: capnp
	mkdir -p ./build/out
	GOARCH=amd64 GOOS=linux ./build/go_exec.sh ./build/out/blobcache_amd64-linux ./cmd/blobcache
	GOARCH=amd64 GOOS=linux ./build/go_exec.sh ./build/out/git-remote-bc_amd64-linux ./cmd/git-remote-bc

build-arm64-linux: capnp
    mkdir -p ./build/out
    GOARCH=arm64 GOOS=linux ./build/go_exec.sh ./build/out/blobcache_arm64-linux ./cmd/blobcache
    GOARCH=arm64 GOOS=linux ./build/go_exec.sh ./build/out/git-remote-bc_arm64-linux ./cmd/git-remote-bc

build-arm64-darwin: capnp
	mkdir -p ./build/out
	GOARCH=arm64 GOOS=darwin ./build/go_exec.sh ./build/out/blobcache_arm64-darwin ./cmd/blobcache
	GOARCH=arm64 GOOS=darwin ./build/go_exec.sh ./build/out/git-remote-bc_arm64-darwin ./cmd/git-remote-bc

build-exec: build-amd64-linux build-arm64-linux build-arm64-darwin

test: capnp
	go test ./...

test-rs: build
	cd ./client/rs && cargo test

testv:
	go test -count=1 -v ./pkg/...

install-capnpc-go:
	GOBIN="${GOBIN:-$(go env GOPATH)/bin}" go install capnproto.org/go/capnp/v3/capnpc-go@latest

capnp: install-capnpc-go
	PATH="$(go env GOPATH)/bin:${PATH}"; cd ./src/internal/tries/triescnp && ./build.sh

clean:
	rm -f ./build/out/*
	./build/rm_images.sh

build-images: build-amd64-linux
	./build/build_images.sh

release-build:
    just build-exec
    just build-images

release-publish:
	./build/push_images.sh

# Installs just the blobcache binary to /usr/bin/blobcache
install-unix: build
	sudo cp ./build/out/blobcache /usr/bin/blobcache
	sudo cp ./build/out/git-remote-bc /usr/bin/git-remote-bc

# Install blobcache with systemd service
install-systemd: build
	./etc/install-systemd.sh
