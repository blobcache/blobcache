FROM alpine:latest

WORKDIR /app

COPY build/out/blobcache_amd64-linux ./blobcache

EXPOSE 6025

CMD ["/app/blobcache", "daemon", "--state", "/state", "--listen", "0.0.0.0:6025"]