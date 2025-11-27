#!/bin/sh

# Remove old blobcache images
podman images --format "{{.Repository}}:{{.Tag}}" | grep "blobcache" | xargs -r podman rmi --force

