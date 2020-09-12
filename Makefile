.PHONY: test

test:
	go test --race ./pkg/...

protobuf:
	cd ./pkg/blobnet/bcproto && ./build.sh
	cd ./pkg/tries && ./build.sh

