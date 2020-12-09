.PHONY: test testv protobuf drop-replace add-replace

test:
	go test --race ./pkg/...

testv:
	go test --race -count=1 -v ./pkg/...

protobuf:
	cd ./pkg/blobnet/bcproto && ./build.sh
	cd ./pkg/tries && ./build.sh

drop-replace:
	go mod edit -dropreplace github.com/inet256/inet256

add-replace:
	go mod edit -replace github.com/inet256/inet256=../../inet256/inet256

