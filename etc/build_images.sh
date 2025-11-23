#!/bin/sh
set -xve

# Fetch tags from get_tags.sh and store the output
TAGS=$(./etc/get_tags.sh)

# Remove old blobcache images
podman images --format "{{.Repository}}:{{.Tag}}" | grep "blobcache" | xargs -r podman rmi --force

# Build and tag the image with each fetched tag
for tag in $TAGS; do
  podman build --tag ghcr.io/blobcache/blobcache:$tag .
done
