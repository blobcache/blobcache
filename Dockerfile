FROM alpine:latest

WORKDIR /app

COPY build/out/blobcache_amd64-linux /usr/bin/blobcache

RUN mkdir /state
RUN mkdir /run/blobcache

EXPOSE 6025/udp

CMD ["/usr/bin/blobcache", "daemon", "run", "--state", "/state", "--net", "0.0.0.0:6025", "--serve-ipc", "/run/blobcache/blobcache.sock"]
