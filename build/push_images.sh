#!/bin/sh
set -xve

# Fetch tags from get_tags.sh and store the output
TAGS=$(./build/get_tags.sh)

# Build and tag the image with each fetched tag
for tag in $TAGS; do
  podman push ghcr.io/blobcache/blobcache:$tag
done
