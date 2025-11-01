FROM alpine:latest

WORKDIR /app

COPY build/out/blobcache_amd64-linux ./blobcache

RUN mkdir /state

EXPOSE 6025/udp

CMD ["/app/blobcache", "daemon", "--state", "/state", "--net", "0.0.0.0:6025"]
