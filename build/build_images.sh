#!/bin/sh
set -xve

# Fetch tags from get_tags.sh and store the output
TAGS=$(./build/get_tags.sh)

# Remove old blobcache images
./build/rm_images.sh

# Build and tag the image with each fetched tag
for tag in $TAGS; do
  podman build --tag ghcr.io/blobcache/blobcache:$tag .
done
