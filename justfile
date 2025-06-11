
install:
	go install ./cmd/blobcache

build:
	mkdir -p ./build/out
	go build -o ./build/out/blobcache ./cmd/blobcache 

test:
	go test ./...

testv:
	go test -count=1 -v ./pkg/...

protobuf:
	cd ./src/tries && ./build.sh
	cd ./pkg/bcgrpc && ./build.sh
