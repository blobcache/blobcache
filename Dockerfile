FROM alpine:latest

WORKDIR /app

COPY build/out/blobcache_amd64-linux ./blobcache

RUN mkdir /state
RUN mkdir /run/blobcache

EXPOSE 6025/udp

CMD ["/app/blobcache", "daemon", "--state", "/state", "--net", "0.0.0.0:6025", "--serve-ipc", "/run/blobcache/blobcache.sock"]
